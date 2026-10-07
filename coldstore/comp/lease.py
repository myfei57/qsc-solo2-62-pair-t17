"""Compressor occupancy leases.

A compressor is occupied through a lease, not a flag: the lease names the
cold room that holds it, carries a fencing version and expires unless the
holder renews it. Two design decisions are settled here:

* Leases are renewed, not fixed length. The holder extends its lease with a
  heartbeat; a holder that stops renewing (crashed, cut off, taken out of
  rotation) loses the machine when the lease lapses and the sweeper reclaims
  it. A fixed slot would cut long jobs short and could never be released
  early by a dead holder.
* Conflicts reject the latecomer; nobody is ever preempted. A room actively
  running a compressor keeps it until it releases or its lease expires. A
  latecomer is either rejected outright (with the holder and version in the
  response) or parked in a bounded FIFO queue and granted the machine in
  order once it frees up. The queue bound is the rate limit: once full,
  further requests for that compressor are turned away.

Every mutation is serialised behind one lock and written through to the
store before it is applied in memory, so a restarted console recovers the
exact lease table. Fencing versions come from a persisted counter that only
ever increases, so a version is never issued twice and a stale holder can
neither renew nor release someone else's lease.
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING, Callable

from coldstore.store.commands import append_command, load_commands
from coldstore.store.store import Store

from .report import LeaseState

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor

ACTIVE_KEY_PREFIX = "lease:active:"
QUEUE_KEY_PREFIX = "lease:queue:"
VERSION_KEY_PREFIX = "lease:version:"
HISTORY_KEY = "lease:history"

DEFAULT_TTL_SECONDS = 30.0
MAX_QUEUE = 8

GRANTED = "granted"
QUEUED = "queued"
REJECTED = "rejected"
QUEUE_FULL = "queue_full"


class Lease:
    """One occupancy grant: holder, fencing version and expiry."""

    def __init__(
        self,
        comp_id: str,
        room_id: str,
        version: int,
        ttl: float,
        issued_at: float,
        expires_at: float,
    ) -> None:
        self.comp_id = comp_id
        self.room_id = room_id
        self.version = version
        self.ttl = ttl
        self.issued_at = issued_at
        self.expires_at = expires_at

    def expired(self, now: float) -> bool:
        return now >= self.expires_at

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at - now)

    def as_dict(self) -> dict[str, object]:
        return {
            "comp": self.comp_id,
            "room": self.room_id,
            "version": self.version,
            "ttl": self.ttl,
            "issued_at": round(self.issued_at, 4),
            "expires_at": round(self.expires_at, 4),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Lease":
        return cls(
            comp_id=str(payload.get("comp", "")),
            room_id=str(payload.get("room", "")),
            version=int(payload.get("version", 0)),
            ttl=float(payload.get("ttl", DEFAULT_TTL_SECONDS)),
            issued_at=float(payload.get("issued_at", 0.0)),
            expires_at=float(payload.get("expires_at", 0.0)),
        )


class AcquireResult:
    """The outcome of one lease request, conflict detail included."""

    def __init__(
        self,
        status: str,
        comp_id: str,
        room_id: str,
        lease: Lease | None = None,
        holder: str = "",
        version: int = 0,
        position: int = 0,
    ) -> None:
        self.status = status
        self.comp_id = comp_id
        self.room_id = room_id
        self.lease = lease
        self.holder = holder
        self.version = version
        self.position = position

    @property
    def granted(self) -> bool:
        return self.status == GRANTED

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "granted": self.granted,
            "comp": self.comp_id,
            "room": self.room_id,
            "lease": self.lease.as_dict() if self.lease else None,
            "holder": self.holder,
            "version": self.version,
            "position": self.position,
        }


class LeaseManager:
    """Issues, renews, queues and reclaims compressor occupancy leases."""

    def __init__(
        self,
        store: Store | None = None,
        auditor: "Auditor | None" = None,
        ttl: float = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._store = store
        self._auditor = auditor
        self._ttl = ttl
        self._clock = clock or time.time
        self._lock = threading.RLock()
        self._leases: dict[str, Lease] = {}
        self._queues: dict[str, list[str]] = {}
        self._versions: dict[str, int] = {}
        self._history: list[dict[str, object]] = []
        self.recover()

    def recover(self) -> None:
        """Reload the persisted lease table after a restart.

        Leases that lapsed while the console was down are reclaimed at once;
        everything still valid keeps its holder and fencing version, so the
        machine is never handed out twice.
        """

        with self._lock:
            self._leases = {}
            self._queues = {}
            self._versions = {}
            self._history = []
            if self._store is None:
                return
            for key in self._store.keys():
                if key.startswith(ACTIVE_KEY_PREFIX):
                    lease = self._load_lease(key)
                    if lease is not None and lease.comp_id:
                        self._leases[lease.comp_id] = lease
                elif key.startswith(QUEUE_KEY_PREFIX):
                    comp_id = key[len(QUEUE_KEY_PREFIX) :]
                    queue = self._load_queue(key)
                    if queue:
                        self._queues[comp_id] = queue
                elif key.startswith(VERSION_KEY_PREFIX):
                    comp_id = key[len(VERSION_KEY_PREFIX) :]
                    raw, present = self._store.get(key)
                    if present:
                        try:
                            self._versions[comp_id] = int(raw)
                        except ValueError:
                            continue
            for item in load_commands(self._store, HISTORY_KEY):
                try:
                    entry = json.loads(item)
                except ValueError:
                    continue
                if isinstance(entry, dict):
                    self._history.append(entry)
            self._sweep_locked()

    def acquire(
        self,
        comp_id: str,
        room_id: str,
        ttl: float | None = None,
        wait: bool = False,
    ) -> AcquireResult:
        """Ask for a compressor; the latecomer is queued or rejected."""

        with self._lock:
            self._sweep_locked()
            lease = self._leases.get(comp_id)
            if lease is None:
                granted = self._issue_locked(comp_id, room_id, ttl, via="direct")
                return AcquireResult(
                    GRANTED, comp_id, room_id, lease=granted, holder=room_id,
                    version=granted.version,
                )
            if lease.room_id == room_id:
                # A retry from the holder gets its own lease back, never a copy.
                return AcquireResult(
                    GRANTED, comp_id, room_id, lease=lease, holder=room_id,
                    version=lease.version,
                )
            if wait:
                return self._enqueue_locked(comp_id, room_id, lease)
            self._record_locked(
                "conflict", comp_id, room_id, lease.version,
                holder=lease.room_id, result=REJECTED,
            )
            return AcquireResult(
                REJECTED, comp_id, room_id, holder=lease.room_id, version=lease.version
            )

    def renew(self, comp_id: str, room_id: str, version: int) -> Lease:
        """Extend the holder's lease; a stale fencing version is refused."""

        with self._lock:
            self._sweep_locked()
            lease = self._require_lease(comp_id, room_id)
            if version != lease.version:
                self._record_locked(
                    "conflict", comp_id, room_id, version,
                    holder=lease.room_id, result="stale_version",
                )
                raise ValueError(
                    f"stale lease version {version} for compressor {comp_id}, "
                    f"current is {lease.version}"
                )
            now = self._clock()
            renewed = Lease(comp_id, room_id, version, lease.ttl, lease.issued_at, now + lease.ttl)
            self._persist_lease_locked(renewed)
            self._leases[comp_id] = renewed
            self._record_locked("renewed", comp_id, room_id, version, expires_at=renewed.expires_at)
            return renewed

    def release(self, comp_id: str, room_id: str, version: int | None = None) -> bool:
        """Hand a lease back early; the queue head is granted the machine."""

        with self._lock:
            self._sweep_locked()
            lease = self._leases.get(comp_id)
            if lease is None:
                return False
            if lease.room_id != room_id:
                raise ValueError(f"compressor {comp_id} leased by {lease.room_id}")
            if version is not None and version != lease.version:
                self._record_locked(
                    "conflict", comp_id, room_id, version,
                    holder=lease.room_id, result="stale_version",
                )
                raise ValueError(
                    f"stale lease version {version} for compressor {comp_id}, "
                    f"current is {lease.version}"
                )
            self._drop_lease_locked(comp_id, "released")
            return True

    def sweep(self) -> list[str]:
        """Reclaim every lease whose holder stopped renewing."""

        with self._lock:
            return self._sweep_locked()

    def retire(self, comp_id: str) -> None:
        """Drop the lease and wait queue of a removed compressor.

        The fencing counter is deliberately kept: a version once issued for
        this compressor name is never issued again.
        """

        with self._lock:
            if comp_id in self._leases:
                self._drop_lease_locked(comp_id, "retired", drain=False)
            self._queues.pop(comp_id, None)
            if self._store is not None:
                self._store.delete(f"{QUEUE_KEY_PREFIX}{comp_id}")

    def lease(self, comp_id: str) -> Lease | None:
        with self._lock:
            lease = self._leases.get(comp_id)
            if lease is None or lease.expired(self._clock()):
                return None
            return lease

    def holder(self, comp_id: str) -> str:
        lease = self.lease(comp_id)
        return lease.room_id if lease is not None else ""

    def active(self) -> dict[str, Lease]:
        with self._lock:
            now = self._clock()
            return {
                comp_id: lease
                for comp_id, lease in self._leases.items()
                if not lease.expired(now)
            }

    def queue(self, comp_id: str) -> list[str]:
        with self._lock:
            return list(self._queues.get(comp_id, []))

    def queues(self) -> dict[str, list[str]]:
        with self._lock:
            return {
                comp_id: list(queue)
                for comp_id, queue in sorted(self._queues.items())
                if queue
            }

    def history(self, comp_id: str = "") -> list[dict[str, object]]:
        """Occupancy transitions, oldest first, optionally per compressor."""

        with self._lock:
            return [
                dict(entry)
                for entry in self._history
                if not comp_id or entry.get("comp") == comp_id
            ]

    def state(self) -> LeaseState:
        with self._lock:
            active = self.active()
            return LeaseState(
                leases=[
                    active[comp_id].as_dict() for comp_id in sorted(active)
                ],
                queues=self.queues(),
                history=len(self._history),
            )

    def describe(self) -> str:
        with self._lock:
            queued = sum(len(queue) for queue in self._queues.values())
            return f"leases active={len(self.active())} queued={queued} ttl={self._ttl:.0f}s"

    def _enqueue_locked(self, comp_id: str, room_id: str, lease: Lease) -> AcquireResult:
        queue = self._queues.setdefault(comp_id, [])
        if room_id in queue:
            return AcquireResult(
                QUEUED, comp_id, room_id, holder=lease.room_id,
                version=lease.version, position=queue.index(room_id) + 1,
            )
        if len(queue) >= MAX_QUEUE:
            self._record_locked(
                "conflict", comp_id, room_id, lease.version,
                holder=lease.room_id, result=QUEUE_FULL,
            )
            return AcquireResult(
                QUEUE_FULL, comp_id, room_id, holder=lease.room_id, version=lease.version
            )
        queue.append(room_id)
        self._persist_queue_locked(comp_id)
        self._record_locked(
            "queued", comp_id, room_id, lease.version,
            holder=lease.room_id, position=len(queue),
        )
        return AcquireResult(
            QUEUED, comp_id, room_id, holder=lease.room_id,
            version=lease.version, position=len(queue),
        )

    def _issue_locked(
        self, comp_id: str, room_id: str, ttl: float | None, via: str
    ) -> Lease:
        version = self._next_version_locked(comp_id)
        now = self._clock()
        lease_ttl = ttl if ttl and ttl > 0 else self._ttl
        lease = Lease(comp_id, room_id, version, lease_ttl, now, now + lease_ttl)
        self._persist_lease_locked(lease)
        self._leases[comp_id] = lease
        self._record_locked(
            "issued", comp_id, room_id, version, expires_at=lease.expires_at, via=via
        )
        return lease

    def _drop_lease_locked(self, comp_id: str, event: str, drain: bool = True) -> None:
        lease = self._leases.get(comp_id)
        if lease is None:
            return
        if self._store is not None:
            self._store.delete(f"{ACTIVE_KEY_PREFIX}{comp_id}")
        del self._leases[comp_id]
        self._record_locked(event, comp_id, lease.room_id, lease.version)
        if drain:
            self._drain_queue_locked(comp_id)

    def _drain_queue_locked(self, comp_id: str) -> None:
        if comp_id in self._leases:
            return
        queue = self._queues.get(comp_id)
        if not queue:
            return
        room_id = queue.pop(0)
        if not queue:
            self._queues.pop(comp_id, None)
        self._persist_queue_locked(comp_id)
        self._issue_locked(comp_id, room_id, None, via="queue")

    def _sweep_locked(self) -> list[str]:
        now = self._clock()
        expired = sorted(
            comp_id for comp_id, lease in self._leases.items() if lease.expired(now)
        )
        for comp_id in expired:
            self._drop_lease_locked(comp_id, "expired")
        return expired

    def _require_lease(self, comp_id: str, room_id: str) -> Lease:
        lease = self._leases.get(comp_id)
        if lease is None:
            raise ValueError(f"compressor {comp_id} has no active lease")
        if lease.room_id != room_id:
            raise ValueError(f"compressor {comp_id} leased by {lease.room_id}")
        return lease

    def _next_version_locked(self, comp_id: str) -> int:
        version = self._versions.get(comp_id, 0) + 1
        if self._store is not None:
            self._store.put(f"{VERSION_KEY_PREFIX}{comp_id}", str(version))
        self._versions[comp_id] = version
        return version

    def _persist_lease_locked(self, lease: Lease) -> None:
        if self._store is not None:
            self._store.put(
                f"{ACTIVE_KEY_PREFIX}{lease.comp_id}",
                json.dumps(lease.as_dict(), ensure_ascii=False),
            )

    def _persist_queue_locked(self, comp_id: str) -> None:
        if self._store is None:
            return
        queue = self._queues.get(comp_id, [])
        key = f"{QUEUE_KEY_PREFIX}{comp_id}"
        if queue:
            self._store.put(key, json.dumps(queue, ensure_ascii=False))
        else:
            self._store.delete(key)

    def _record_locked(
        self, event: str, comp_id: str, room_id: str, version: int, **extra: object
    ) -> None:
        entry: dict[str, object] = {
            "at": round(self._clock(), 4),
            "event": event,
            "comp": comp_id,
            "room": room_id,
            "version": version,
        }
        entry.update(extra)
        if self._store is not None:
            append_command(self._store, HISTORY_KEY, json.dumps(entry, ensure_ascii=False))
        self._history.append(entry)
        if self._auditor is not None:
            self._auditor.record(f"lease_{event}", json.dumps(entry, ensure_ascii=False))

    def _load_lease(self, key: str) -> Lease | None:
        raw, present = self._store.get(key)
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        return Lease.from_dict(payload)

    def _load_queue(self, key: str) -> list[str]:
        raw, present = self._store.get(key)
        if not present:
            return []
        try:
            payload = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(payload, list):
            return []
        return [str(room) for room in payload]
