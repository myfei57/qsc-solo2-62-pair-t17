"""Compressor bank with staged loading and lease based arbitration.

Compressors are loaded one stage at a time. Occupancy is governed by the
lease manager in :mod:`coldstore.comp.lease`: a room holds a compressor
through a renewable, expiring lease rather than a bare flag, so two rooms
can never both believe they own the machine and a room that walks away
without releasing loses it when the lease lapses.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable

from coldstore.store.store import Store

from .lease import DEFAULT_TTL_SECONDS, AcquireResult, Lease, LeaseManager
from .report import BankState, CompressorView, LeaseState

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor


class Compressor:
    """Mutable state of one compressor."""

    def __init__(self, comp_id: str, stages: int) -> None:
        self.id = comp_id
        self.stages = stages
        self.stage = 0
        self.running = False

    def snapshot(self) -> dict[str, object]:
        return {
            "id": self.id,
            "stages": self.stages,
            "stage": self.stage,
            "running": self.running,
        }


class Bank:
    """Thread safe registry of compressors."""

    def __init__(
        self,
        store: Store | None = None,
        auditor: "Auditor | None" = None,
        lease_ttl: float = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._compressors: dict[str, Compressor] = {}
        self._lock = threading.RLock()
        self.leases = LeaseManager(store, auditor, ttl=lease_ttl, clock=clock)

    def add_compressor(self, comp_id: str, stages: int) -> Compressor:
        with self._lock:
            compressor = Compressor(comp_id=comp_id, stages=stages)
            self._compressors[comp_id] = compressor
            return compressor

    def compressor(self, comp_id: str) -> Compressor | None:
        with self._lock:
            return self._compressors.get(comp_id)

    def compressors(self) -> list[Compressor]:
        with self._lock:
            return [self._compressors[key] for key in sorted(self._compressors)]

    def compressor_ids(self) -> list[str]:
        return [compressor.id for compressor in self.compressors()]

    def count(self) -> int:
        with self._lock:
            return len(self._compressors)

    def remove(self, comp_id: str) -> None:
        with self._lock:
            if comp_id not in self._compressors:
                raise ValueError(f"compressor {comp_id} not found")
            del self._compressors[comp_id]
            self.leases.retire(comp_id)

    def stage_up(self, comp_id: str) -> int:
        with self._lock:
            compressor = self._require(comp_id)
            if compressor.stage < compressor.stages:
                compressor.stage += 1
            compressor.running = compressor.stage > 0
            return compressor.stage

    def stage_down(self, comp_id: str) -> int:
        with self._lock:
            compressor = self._require(comp_id)
            if compressor.stage > 0:
                compressor.stage -= 1
            compressor.running = compressor.stage > 0
            return compressor.stage

    def set_stage(self, comp_id: str, stage: int) -> int:
        with self._lock:
            compressor = self._require(comp_id)
            if stage < 0 or stage > compressor.stages:
                raise ValueError(f"stage must be between 0 and {compressor.stages}")
            compressor.stage = stage
            compressor.running = stage > 0
            return stage

    def start(self, comp_id: str) -> None:
        with self._lock:
            compressor = self._require(comp_id)
            if compressor.stage == 0:
                compressor.stage = 1
            compressor.running = True

    def stop(self, comp_id: str) -> None:
        with self._lock:
            compressor = self._require(comp_id)
            compressor.stage = 0
            compressor.running = False

    def stage(self, comp_id: str) -> tuple[int, bool]:
        with self._lock:
            compressor = self._compressors.get(comp_id)
            if compressor is None:
                return 0, False
            return compressor.stage, True

    def running_ids(self) -> list[str]:
        return [compressor.id for compressor in self.compressors() if compressor.running]

    def running_count(self) -> int:
        return len(self.running_ids())

    def total_stage(self) -> int:
        return sum(compressor.stage for compressor in self.compressors())

    def max_stage(self) -> int:
        return sum(compressor.stages for compressor in self.compressors())

    def load_ratio(self) -> float:
        maximum = self.max_stage()
        if maximum <= 0:
            return 0.0
        return self.total_stage() / maximum

    def order(self) -> list[str]:
        """Compressors from the highest stage down, then by identifier."""

        ordered = sorted(self.compressors(), key=lambda compressor: (-compressor.stage, compressor.id))
        return [compressor.id for compressor in ordered]

    def reserve(self, comp_id: str, room_id: str) -> str:
        """Give one room exclusive use of a compressor.

        Backed by a lease: the hold expires unless the room renews it, so a
        room that leaves without releasing cannot squat on the machine.
        """

        with self._lock:
            self._require(comp_id)
            result = self.leases.acquire(comp_id, room_id)
            if not result.granted:
                raise ValueError(f"compressor {comp_id} already reserved by {result.holder}")
            return room_id

    def release(self, comp_id: str, room_id: str) -> None:
        with self._lock:
            self.leases.release(comp_id, room_id)

    def reserved_by(self, comp_id: str) -> str:
        with self._lock:
            return self.leases.holder(comp_id)

    def reservations(self) -> dict[str, str]:
        with self._lock:
            return {
                comp_id: lease.room_id
                for comp_id, lease in sorted(self.leases.active().items())
            }

    def acquire_lease(
        self, comp_id: str, room_id: str, ttl: float = 0.0, wait: bool = False
    ) -> AcquireResult:
        """Lease a compressor, optionally joining the bounded wait queue."""

        with self._lock:
            self._require(comp_id)
            return self.leases.acquire(comp_id, room_id, ttl=ttl or None, wait=wait)

    def renew_lease(self, comp_id: str, room_id: str, version: int) -> Lease:
        """Extend the holder's lease; the fencing version must match."""

        with self._lock:
            self._require(comp_id)
            return self.leases.renew(comp_id, room_id, version)

    def release_lease(self, comp_id: str, room_id: str, version: int = 0) -> bool:
        """Release a lease, checking the fencing version when one is given."""

        with self._lock:
            self._require(comp_id)
            return self.leases.release(comp_id, room_id, version or None)

    def sweep_leases(self) -> list[str]:
        """Reclaim every lease whose holder stopped renewing."""

        with self._lock:
            return self.leases.sweep()

    def lease_state(self) -> LeaseState:
        with self._lock:
            return self.leases.state()

    def lease_history(self, comp_id: str = "") -> list[dict[str, object]]:
        with self._lock:
            return self.leases.history(comp_id)

    def state(self) -> BankState:
        compressors = self.compressors()
        return BankState(
            compressors=[
                CompressorView(
                    id=compressor.id,
                    stages=compressor.stages,
                    stage=compressor.stage,
                    running=compressor.running,
                    reserved_by=self.reserved_by(compressor.id),
                )
                for compressor in compressors
            ],
            running=self.running_count(),
            total_stage=self.total_stage(),
            max_stage=self.max_stage(),
            load_ratio=self.load_ratio(),
        )

    def describe(self) -> str:
        state = self.state()
        return (
            f"comp compressors={len(state.compressors)} running={state.running} "
            f"stage={state.total_stage}/{state.max_stage}"
        )

    def _require(self, comp_id: str) -> Compressor:
        compressor = self._compressors.get(comp_id)
        if compressor is None:
            raise ValueError(f"compressor {comp_id} not found")
        return compressor
