"""Time based, exclusive occupancy leases for compressors."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from coldstore.audit import AUDIT_KEY, Entry
from coldstore.store.store import Store

LEASE_KEY_PREFIX = "comp:lease:"
QUEUE_KEY_PREFIX = "comp:queue:"
DEFAULT_TTL_SECONDS = 30.0
MIN_TTL_SECONDS = 1.0
MAX_TTL_SECONDS = 300.0
DEFAULT_MAX_RENEWALS = 5
DEFAULT_QUEUE_LIMIT = 100

GRANTED = "granted"
QUEUED = "queued"
RELEASED = "released"
EXPIRED = "expired"
RENEWED = "renewed"
REJECTED = "rejected"
CANCELED = "canceled"
INVALIDATED = "invalidated"

EVENT_PREFIX = "comp.lease."


class LeaseError(ValueError):
    """A malformed lease request or an unknown lease."""


class LeaseConflict(LeaseError):
    """The requested lease operation conflicts with the current holder."""

    def __init__(self, result: str, message: str, lease: "Lease | None" = None) -> None:
        super().__init__(message)
        self.result = result
        self.lease = lease


@dataclass(frozen=True)
class Lease:
    """The exclusive occupancy held by one room."""

    id: str
    comp_id: str
    room_id: str
    version: int
    request_id: str
    acquired_at: float
    expires_at: float
    ttl_seconds: float
    renewals: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "comp_id": self.comp_id,
            "room_id": self.room_id,
            "version": self.version,
            "request_id": self.request_id,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
            "ttl_seconds": self.ttl_seconds,
            "renewals": self.renewals,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Lease":
        try:
            return cls(
                id=str(payload["id"]),
                comp_id=str(payload["comp_id"]),
                room_id=str(payload["room_id"]),
                version=int(payload["version"]),
                request_id=str(payload["request_id"]),
                acquired_at=float(payload["acquired_at"]),
                expires_at=float(payload["expires_at"]),
                ttl_seconds=float(payload["ttl_seconds"]),
                renewals=int(payload.get("renewals", 0)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LeaseError("corrupt compressor lease") from exc


@dataclass(frozen=True)
class LeaseRequest:
    """A request waiting in the per-compressor FIFO queue."""

    id: str
    comp_id: str
    room_id: str
    ttl_seconds: float
    queued_at: float

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "comp_id": self.comp_id,
            "room_id": self.room_id,
            "ttl_seconds": self.ttl_seconds,
            "queued_at": self.queued_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LeaseRequest":
        try:
            return cls(
                id=str(payload["id"]),
                comp_id=str(payload["comp_id"]),
                room_id=str(payload["room_id"]),
                ttl_seconds=float(payload["ttl_seconds"]),
                queued_at=float(payload["queued_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LeaseError("corrupt compressor lease request") from exc


@dataclass(frozen=True)
class AcquireResult:
    """The result of a serialised acquisition attempt."""

    status: str
    lease: Lease | None
    request: LeaseRequest | None
    queue_position: int
    holder: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "lease": self.lease.as_dict() if self.lease else None,
            "request": self.request.as_dict() if self.request else None,
            "queue_position": self.queue_position,
            "holder": self.holder,
        }


class LeaseManager:
    """Persisted exclusive leases with a FIFO queue and expiry reaper."""

    def __init__(
        self,
        store: Store,
        auditor: Any | None = None,
        *,
        default_ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_renewals: int = DEFAULT_MAX_RENEWALS,
        queue_limit: int = DEFAULT_QUEUE_LIMIT,
        reaper_interval_seconds: float = 1.0,
        start_reaper: bool = False,
        time_func: Callable[[], float] = time.time,
        on_change: Callable[[str, str], None] | None = None,
    ) -> None:
        self._store = store
        self._auditor = auditor
        self._default_ttl = self._validate_ttl(default_ttl_seconds)
        self._max_renewals = max_renewals
        self._queue_limit = queue_limit
        self._reaper_interval = reaper_interval_seconds
        self._time = time_func
        self._on_change = on_change
        self._batch_depth = 0
        self._batch_puts: dict[str, str] = {}
        self._batch_deletes: list[str] = []
        self._batch_audits: list[str] = []
        self._batch_committed_puts: dict[str, str] = {}
        self._batch_committed_deletes: list[str] = []
        self._batch_committed_audits: list[str] = []
        self._batch_notifications: list[tuple[str, str]] = []
        self._condition = threading.Condition()
        self._stopped = threading.Event()
        self._reaper: threading.Thread | None = None
        self._recover()
        if start_reaper:
            self.start_reaper()

    def start_reaper(self) -> None:
        """Start the daemon that automatically reclaims expired leases."""

        with self._condition:
            if self._reaper is not None and self._reaper.is_alive():
                return
            self._stopped.clear()
            self._reaper = threading.Thread(
                target=self._reap_forever, name="comp-lease-reaper", daemon=True
            )
            self._reaper.start()

    def close(self) -> None:
        """Stop the background reaper."""

        with self._condition:
            self._stopped.set()
            self._condition.notify_all()
        reaper = self._reaper
        if reaper is not None and reaper is not threading.current_thread():
            reaper.join(timeout=self._reaper_interval + 0.1)

    def acquire(
        self,
        comp_id: str,
        room_id: str,
        *,
        request_id: str = "",
        ttl_seconds: float | None = None,
        wait_seconds: float = 0.0,
        queue: bool = True,
    ) -> AcquireResult:
        """Acquire exclusively, or enqueue this machine's request in FIFO order."""

        if not comp_id:
            raise LeaseError("compressor id is required")
        if not room_id:
            raise LeaseError("room id is required")
        ttl = self._default_ttl if ttl_seconds is None else self._validate_ttl(ttl_seconds)
        request_id = request_id or uuid.uuid4().hex
        wait_seconds = max(0.0, float(wait_seconds))

        with self._condition, self._transaction():
            self._expire_locked(comp_id)
            duplicate = self._duplicate_locked(comp_id, room_id, request_id)
            if duplicate is not None:
                return duplicate

            lease = self._lease_locked(comp_id)
            if lease is not None and lease.room_id == room_id:
                return AcquireResult(
                    GRANTED, lease, None, 0, lease.room_id
                )

            requests = self._queue_locked(comp_id)
            if lease is not None or requests:
                if not queue:
                    holder = lease.room_id if lease else requests[0].room_id
                    self._audit(
                        "denied",
                        {
                            "event": REJECTED,
                            "comp_id": comp_id,
                            "room_id": room_id,
                            "request_id": request_id,
                            "holder": holder,
                            "result": "busy",
                        },
                    )
                    self._commit_locked()
                    raise LeaseConflict("busy", f"compressor {comp_id} is busy", lease)
                if len(requests) >= self._queue_limit:
                    self._audit(
                        "denied",
                        {
                            "event": REJECTED,
                            "comp_id": comp_id,
                            "room_id": room_id,
                            "request_id": request_id,
                            "result": "queue_full",
                        },
                    )
                    self._commit_locked()
                    raise LeaseConflict(
                        "queue_full", f"lease queue for {comp_id} is full", lease
                    )

                request = LeaseRequest(
                    id=request_id,
                    comp_id=comp_id,
                    room_id=room_id,
                    ttl_seconds=ttl,
                    queued_at=self._now(),
                )
                requests.append(request)
                self._save_queue_locked(comp_id, requests)
                self._audit(
                    "queued",
                    {
                        "event": QUEUED,
                        "comp_id": comp_id,
                        "room_id": room_id,
                        "request_id": request_id,
                        "queue_position": len(requests),
                        "ttl_seconds": ttl,
                    },
                )
                self._promote_locked(comp_id)
                self._condition.notify_all()
                if wait_seconds > 0:
                    self._commit_locked()
                    self._wait_locked(request.id, wait_seconds)
                    status = GRANTED if self._is_granted_locked(comp_id, request_id) else QUEUED
                else:
                    status = QUEUED
                return self._request_result_locked(request, status)

            lease = self._grant_locked(
                LeaseRequest(request_id, comp_id, room_id, ttl, self._now())
            )
            self._condition.notify_all()
            return AcquireResult(GRANTED, lease, None, 0, room_id)

    def renew(
        self,
        comp_id: str,
        room_id: str,
        *,
        lease_id: str = "",
        expected_version: int | None = None,
        ttl_seconds: float | None = None,
    ) -> Lease:
        """Explicitly extend a lease using its fixed duration and next version."""

        if not room_id:
            raise LeaseError("room id is required")
        with self._condition, self._transaction():
            self._expire_locked(comp_id)
            lease = self._require_lease_locked(comp_id, room_id, lease_id, expected_version)
            if lease.renewals >= self._max_renewals:
                self._audit(
                    "denied",
                    {
                        "event": REJECTED,
                        "comp_id": comp_id,
                        "room_id": room_id,
                        "lease_id": lease.id,
                        "version": lease.version,
                        "request_id": lease.request_id,
                        "result": "renewal_limit",
                    },
                )
                self._commit_locked()
                raise LeaseConflict("renewal_limit", "lease reached its renewal limit", lease)

            extension = lease.ttl_seconds if ttl_seconds is None else self._validate_ttl(ttl_seconds)
            renewed = Lease(
                id=lease.id,
                comp_id=lease.comp_id,
                room_id=lease.room_id,
                version=lease.version + 1,
                request_id=lease.request_id,
                acquired_at=lease.acquired_at,
                expires_at=lease.expires_at + extension,
                ttl_seconds=lease.ttl_seconds,
                renewals=lease.renewals + 1,
            )
            self._save_lease_locked(renewed)
            self._audit(
                "renewed",
                {
                    "event": RENEWED,
                    "comp_id": comp_id,
                    "room_id": room_id,
                    "lease_id": renewed.id,
                    "version": renewed.version,
                    "request_id": renewed.request_id,
                    "expires_at": renewed.expires_at,
                    "renewals": renewed.renewals,
                },
            )
            self._condition.notify_all()
            return renewed

    def release(
        self,
        comp_id: str,
        room_id: str,
        *,
        lease_id: str = "",
        expected_version: int | None = None,
        allow_absent: bool = False,
    ) -> Lease | None:
        """Release a held lease and grant the machine to the next queued room."""

        with self._condition, self._transaction():
            expired = self._expire_locked(comp_id)
            lease = self._lease_locked(comp_id)
            if lease is None:
                if allow_absent:
                    return expired
                raise LeaseError(f"compressor {comp_id} is not leased")
            self._require_lease_locked(comp_id, room_id, lease_id, expected_version, lease)
            self._delete_lease_locked(comp_id)
            self._audit(
                "released",
                {
                    "event": RELEASED,
                    "comp_id": comp_id,
                    "room_id": room_id,
                    "lease_id": lease.id,
                    "version": lease.version,
                    "request_id": lease.request_id,
                },
            )
            self._notify_change(comp_id, "")
            self._promote_locked(comp_id)
            self._condition.notify_all()
            return lease

    def cancel(self, request_id: str, room_id: str = "") -> LeaseRequest | None:
        """Cancel a request that has not yet been granted."""

        if not request_id:
            raise LeaseError("request id is required")
        with self._condition, self._transaction():
            for comp_id in self._compressor_ids_locked():
                requests = self._queue_locked(comp_id)
                for index, request in enumerate(requests):
                    if request.id != request_id:
                        continue
                    if room_id and request.room_id != room_id:
                        raise LeaseConflict("wrong_room", "request belongs to another room")
                    removed = requests.pop(index)
                    self._save_queue_locked(comp_id, requests)
                    self._audit(
                        "canceled",
                        {
                            "event": CANCELED,
                            "comp_id": comp_id,
                            "room_id": removed.room_id,
                            "request_id": request_id,
                        },
                    )
                    self._condition.notify_all()
                    return removed
        return None

    def invalidate(self, comp_id: str, *, reason: str = "compressor_removed") -> None:
        """Revoke one active lease and cancel all waiting requests."""

        with self._condition, self._transaction():
            lease = self._lease_locked(comp_id)
            if lease is not None and self._now() >= lease.expires_at:
                self._expire_locked(comp_id)
                lease = None
            if lease is not None:
                self._delete_lease_locked(comp_id)
                self._audit(
                    "invalidated",
                    {
                        "event": INVALIDATED,
                        "comp_id": comp_id,
                        "room_id": lease.room_id,
                        "lease_id": lease.id,
                        "version": lease.version,
                        "request_id": lease.request_id,
                        "reason": reason,
                    },
                )
                self._notify_change(comp_id, "")
            requests = self._queue_locked(comp_id)
            for request in requests:
                self._audit(
                    "canceled",
                    {
                        "event": CANCELED,
                        "comp_id": comp_id,
                        "room_id": request.room_id,
                        "request_id": request.id,
                        "reason": reason,
                    },
                )
            if requests:
                self._save_queue_locked(comp_id, [])
            self._condition.notify_all()

    def lease(self, comp_id: str) -> Lease | None:
        with self._condition, self._transaction():
            self._expire_locked(comp_id)
            return self._lease_locked(comp_id)

    def holder(self, comp_id: str) -> str:
        lease = self.lease(comp_id)
        return lease.room_id if lease else ""

    def pending(self, comp_id: str) -> list[LeaseRequest]:
        with self._condition, self._transaction():
            self._expire_locked(comp_id)
            return list(self._queue_locked(comp_id))

    def leases(self) -> dict[str, Lease]:
        with self._condition, self._transaction():
            leases: dict[str, Lease] = {}
            for comp_id in self._compressor_ids_locked():
                self._expire_locked(comp_id)
                lease = self._lease_locked(comp_id)
                if lease is not None:
                    leases[comp_id] = lease
            return leases

    def state(self) -> dict[str, object]:
        """Return active leases and pending occupancy requests."""

        with self._condition, self._transaction():
            leases: dict[str, Lease] = {}
            pending: dict[str, list[LeaseRequest]] = {}
            for comp_id in self._compressor_ids_locked():
                self._expire_locked(comp_id)
                lease = self._lease_locked(comp_id)
                if lease is not None:
                    leases[comp_id] = lease
                requests = self._queue_locked(comp_id)
                if requests:
                    pending[comp_id] = list(requests)
            return {
                "leases": {comp_id: lease.as_dict() for comp_id, lease in leases.items()},
                "pending": {
                    comp_id: [request.as_dict() for request in requests]
                    for comp_id, requests in pending.items()
                },
            }

    def reservations(self) -> dict[str, str]:
        return {comp_id: lease.room_id for comp_id, lease in self.leases().items()}

    def history(
        self, comp_id: str = "", room_id: str = "", limit: int = 100
    ) -> list[dict[str, object]]:
        """Query occupancy audit history, newest last and optionally filtered."""

        if self._auditor is None:
            return []
        entries = self._lease_audit_locked()
        events: list[dict[str, object]] = []
        for entry in entries:
            if not entry.kind.startswith(EVENT_PREFIX):
                continue
            try:
                detail = json.loads(entry.detail)
            except ValueError:
                continue
            if not isinstance(detail, dict):
                continue
            if comp_id and detail.get("comp_id") != comp_id:
                continue
            if room_id and detail.get("room_id") != room_id:
                continue
            event = {"audit_id": entry.id, "time": entry.time, "kind": entry.kind}
            event.update(detail)
            events.append(event)
        if limit > 0:
            events = events[-limit:]
        return events

    def reap_expired(self) -> list[Lease]:
        """Recover every currently expired active lease."""

        with self._condition, self._transaction():
            recovered: list[Lease] = []
            for comp_id in self._compressor_ids_locked():
                lease = self._expire_locked(comp_id)
                if lease is not None:
                    recovered.append(lease)
            if recovered:
                self._condition.notify_all()
            return recovered

    def _recover(self) -> None:
        with self._condition, self._transaction():
            for comp_id in self._compressor_ids_locked():
                self._expire_locked(comp_id)

    def _reap_forever(self) -> None:
        while not self._stopped.wait(self._reaper_interval):
            try:
                self.reap_expired()
            except Exception:  # noqa: BLE001 - reaper must stay alive
                continue

    def _wait_locked(self, request_id: str, wait_seconds: float) -> None:
        deadline = self._now() + wait_seconds
        while True:
            remaining = deadline - self._now()
            if remaining <= 0:
                return
            self._condition.wait(remaining)

    def _is_granted_locked(self, comp_id: str, request_id: str) -> bool:
        lease = self._lease_locked(comp_id)
        return lease is not None and lease.request_id == request_id

    def _request_result_locked(self, request: LeaseRequest, status: str) -> AcquireResult:
        lease = self._lease_locked(request.comp_id)
        if lease is not None and lease.request_id == request.id:
            return AcquireResult(GRANTED, lease, None, 0, lease.room_id)
        requests = self._queue_locked(request.comp_id)
        current = next((item for item in requests if item.id == request.id), None)
        position = requests.index(current) + 1 if current is not None else 0
        holder = lease.room_id if lease else ""
        return AcquireResult(status, None, current, position, holder)

    def _duplicate_locked(
        self, comp_id: str, room_id: str, request_id: str
    ) -> AcquireResult | None:
        lease = self._lease_locked(comp_id)
        if lease is not None and lease.request_id == request_id:
            if lease.room_id != room_id:
                raise LeaseConflict("request_reused", "request id was issued to another room", lease)
            return AcquireResult(GRANTED, lease, None, 0, lease.room_id)
        for index, request in enumerate(self._queue_locked(comp_id)):
            if request.id == request_id:
                if request.room_id != room_id:
                    raise LeaseConflict("request_reused", "request id was issued to another room")
                return AcquireResult(QUEUED, None, request, index + 1, lease.room_id if lease else "")
        for event in reversed(self._lease_audit_locked()):
            if not event.kind.startswith(EVENT_PREFIX):
                continue
            try:
                detail = json.loads(event.detail)
            except ValueError:
                continue
            if not isinstance(detail, dict) or not str(detail.get("request_id", "")):
                continue
            if detail.get("request_id") != request_id:
                continue
            if detail.get("comp_id") != comp_id or detail.get("room_id") != room_id:
                raise LeaseConflict("request_reused", "request id was already used")
            return AcquireResult(
                str(detail.get("event", REJECTED)),
                lease if lease and lease.request_id == request_id else None,
                None,
                0,
                lease.room_id if lease else "",
            )
        return None

    def _promote_locked(self, comp_id: str) -> Lease | None:
        lease = self._lease_locked(comp_id)
        if lease is not None:
            return lease
        requests = self._queue_locked(comp_id)
        if requests:
            request = requests.pop(0)
            self._save_queue_locked(comp_id, requests)
            return self._grant_locked(request)
        return None

    def _grant_locked(self, request: LeaseRequest) -> Lease:
        now = self._now()
        lease = Lease(
            id=uuid.uuid4().hex,
            comp_id=request.comp_id,
            room_id=request.room_id,
            version=1,
            request_id=request.id,
            acquired_at=now,
            expires_at=now + request.ttl_seconds,
            ttl_seconds=request.ttl_seconds,
        )
        self._save_lease_locked(lease)
        self._audit(
            "granted",
            {
                "event": GRANTED,
                "comp_id": lease.comp_id,
                "room_id": lease.room_id,
                "lease_id": lease.id,
                "version": lease.version,
                "request_id": lease.request_id,
                "acquired_at": lease.acquired_at,
                "expires_at": lease.expires_at,
                "ttl_seconds": lease.ttl_seconds,
                "result": "granted",
            },
        )
        self._notify_change(lease.comp_id, lease.room_id)
        return lease

    def _expire_locked(self, comp_id: str) -> Lease | None:
        lease = self._lease_locked(comp_id)
        if lease is None or self._now() < lease.expires_at:
            return None
        self._delete_lease_locked(comp_id)
        self._audit(
            "expired",
            {
                "event": EXPIRED,
                "comp_id": comp_id,
                "room_id": lease.room_id,
                "lease_id": lease.id,
                "version": lease.version,
                "request_id": lease.request_id,
                "expires_at": lease.expires_at,
                "result": EXPIRED,
            },
        )
        self._notify_change(comp_id, "")
        self._promote_locked(comp_id)
        return lease

    def _require_lease_locked(
        self,
        comp_id: str,
        room_id: str,
        lease_id: str,
        expected_version: int | None,
        lease: Lease | None = None,
    ) -> Lease:
        lease = lease or self._lease_locked(comp_id)
        if lease is None:
            raise LeaseError(f"compressor {comp_id} is not leased")
        if lease.room_id != room_id:
            message = f"compressor {comp_id} is held by {lease.room_id}"
            self._conflict_locked(comp_id, room_id, "held_by_other", message, lease)
        if lease_id and lease.id != lease_id:
            self._conflict_locked(
                comp_id, room_id, "wrong_lease",
                "lease id does not match the current holder", lease,
            )
        if expected_version is not None and lease.version != expected_version:
            self._conflict_locked(
                comp_id, room_id, "version_conflict",
                "lease version does not match", lease,
            )
        return lease

    def _conflict_locked(
        self, comp_id: str, room_id: str, result: str, message: str, lease: Lease
    ) -> None:
        self._audit(
            "denied",
            {
                "event": REJECTED,
                "comp_id": comp_id,
                "room_id": room_id,
                "lease_id": lease.id,
                "version": lease.version,
                "request_id": lease.request_id,
                "holder": lease.room_id,
                "result": result,
            },
        )
        self._commit_locked()
        raise LeaseConflict(result, message, lease)

    def _lease_locked(self, comp_id: str) -> Lease | None:
        raw, present = self._get_locked(self._lease_key(comp_id))
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise LeaseError(f"corrupt lease for compressor {comp_id}") from exc
        if not isinstance(payload, dict):
            raise LeaseError(f"corrupt lease for compressor {comp_id}")
        return Lease.from_dict(payload)

    def _queue_locked(self, comp_id: str) -> list[LeaseRequest]:
        raw, present = self._get_locked(self._queue_key(comp_id))
        if not present:
            return []
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise LeaseError(f"corrupt lease queue for compressor {comp_id}") from exc
        if not isinstance(payload, list):
            raise LeaseError(f"corrupt lease queue for compressor {comp_id}")
        return [LeaseRequest.from_dict(item) for item in payload if isinstance(item, dict)]

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        if self._batch_depth:
            yield
            return
        self._batch_depth = 1
        try:
            yield
            self._commit_locked()
        finally:
            self._batch_depth = 0
            self._reset_batch_locked()

    def _commit_locked(self) -> None:
        if not (self._batch_puts or self._batch_deletes or self._batch_audits):
            return
        notifications = list(self._batch_notifications)
        self._batch_committed_puts.update(self._batch_puts)
        deleted_in_tx = set(self._batch_deletes)
        for key in self._batch_deletes:
            self._batch_committed_puts.pop(key, None)
        self._batch_committed_deletes = list(
            (set(self._batch_committed_deletes) | deleted_in_tx) - set(self._batch_committed_puts)
        )
        self._batch_committed_audits.extend(self._batch_audits)
        self._store.apply(
            puts=self._batch_puts,
            deletes=self._batch_deletes,
            append_lists={AUDIT_KEY: self._batch_audits} if self._batch_audits else {},
        )
        self._reset_active_batch_locked()
        for comp_id, room_id in notifications:
            if self._on_change is not None:
                self._on_change(comp_id, room_id)

    def _reset_batch_locked(self) -> None:
        self._reset_active_batch_locked()
        self._batch_committed_puts = {}
        self._batch_committed_deletes = []
        self._batch_committed_audits = []

    def _reset_active_batch_locked(self) -> None:
        self._batch_puts = {}
        self._batch_deletes = []
        self._batch_audits = []
        self._batch_notifications = []

    def _get_locked(self, key: str) -> tuple[str, bool]:
        if key in self._batch_puts:
            return self._batch_puts[key], True
        if key in self._batch_deletes or key in self._batch_committed_deletes:
            return "", False
        if key in self._batch_committed_puts:
            return self._batch_committed_puts[key], True
        return self._store.get(key)

    def _keys_locked(self) -> list[str]:
        keys = set(self._store.keys())
        keys.update(self._batch_committed_puts)
        keys.update(self._batch_puts)
        keys.difference_update(self._batch_committed_deletes)
        keys.difference_update(self._batch_deletes)
        return sorted(keys)

    def _save_lease_locked(self, lease: Lease) -> None:
        key = self._lease_key(lease.comp_id)
        value = json.dumps(lease.as_dict(), ensure_ascii=False)
        self._batch_puts[key] = value
        if key in self._batch_deletes:
            self._batch_deletes.remove(key)

    def _delete_lease_locked(self, comp_id: str) -> None:
        key = self._lease_key(comp_id)
        self._batch_puts.pop(key, None)
        if key not in self._batch_deletes:
            self._batch_deletes.append(key)

    def _save_queue_locked(self, comp_id: str, requests: list[LeaseRequest]) -> None:
        key = self._queue_key(comp_id)
        if requests:
            payload = [request.as_dict() for request in requests]
            self._batch_puts[key] = json.dumps(payload, ensure_ascii=False)
            if key in self._batch_deletes:
                self._batch_deletes.remove(key)
        else:
            self._batch_puts.pop(key, None)
            if key not in self._batch_deletes:
                self._batch_deletes.append(key)

    def _lease_audit_locked(self) -> list[Any]:
        if self._auditor is None:
            events = []
        else:
            events = list(self._auditor.entries())
        for raw in self._batch_committed_audits:
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue
            events.append(
                Entry(
                    id=str(payload.get("id", "")),
                    time=str(payload.get("time", "")),
                    kind=str(payload.get("kind", "")),
                    detail=str(payload.get("detail", "")),
                )
            )
        return events

    def _audit(self, suffix: str, detail: dict[str, object]) -> None:
        if self._auditor is None:
            return
        detail = dict(detail)
        detail.setdefault("at", self._now())
        entry = {
            "id": uuid.uuid4().hex,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "kind": f"{EVENT_PREFIX}{suffix}",
            "detail": json.dumps(detail, ensure_ascii=False, sort_keys=True),
        }
        self._batch_audits.append(json.dumps(entry, ensure_ascii=False))

    def _notify_change(self, comp_id: str, room_id: str) -> None:
        if self._on_change is None:
            return
        if self._batch_depth:
            self._batch_notifications.append((comp_id, room_id))
        else:
            self._on_change(comp_id, room_id)

    def _compressor_ids_locked(self) -> list[str]:
        keys = self._keys_locked()
        prefix_len = len(LEASE_KEY_PREFIX)
        ids = [key[prefix_len:] for key in keys if key.startswith(LEASE_KEY_PREFIX)]
        queue_prefix_len = len(QUEUE_KEY_PREFIX)
        ids.extend(key[queue_prefix_len:] for key in keys if key.startswith(QUEUE_KEY_PREFIX))
        return sorted(set(ids))

    def _lease_key(self, comp_id: str) -> str:
        return f"{LEASE_KEY_PREFIX}{comp_id}"

    def _queue_key(self, comp_id: str) -> str:
        return f"{QUEUE_KEY_PREFIX}{comp_id}"

    def _now(self) -> float:
        return float(self._time())

    @staticmethod
    def _validate_ttl(ttl_seconds: float) -> float:
        try:
            ttl = float(ttl_seconds)
        except (TypeError, ValueError) as exc:
            raise LeaseError("lease ttl must be a number") from exc
        if ttl < MIN_TTL_SECONDS or ttl > MAX_TTL_SECONDS:
            raise LeaseError(f"lease ttl must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS} seconds")
        return ttl
