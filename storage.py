import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

@dataclass(frozen=True)
class Version:
    ts_ms: int
    node_id: str

    def as_tuple(self) -> Tuple[int, str]:
        return (self.ts_ms, self.node_id)

@dataclass
class Record:
    value: Optional[Any]          # None means tombstone for deletes
    version: Version

class InMemoryStorage:
    """
    Thread-safe-ish for asyncio (uses a lock).
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._kv: Dict[str, Record] = {}

    async def get(self, key: str) -> Optional[Record]:
        async with self._lock:
            return self._kv.get(key)

    async def put(self, key: str, value: Any, version: Version) -> Record:
        async with self._lock:
            cur = self._kv.get(key)
            if cur is None or version.as_tuple() >= cur.version.as_tuple():
                rec = Record(value=value, version=version)
                self._kv[key] = rec
                return rec
            return cur

    async def delete(self, key: str, version: Version) -> Record:
        async with self._lock:
            cur = self._kv.get(key)
            if cur is None or version.as_tuple() >= cur.version.as_tuple():
                rec = Record(value=None, version=version)  # tombstone
                self._kv[key] = rec
                return rec
            return cur

    async def dump(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            out = {}
            for k, r in self._kv.items():
                out[k] = {
                    "value": r.value,
                    "version": {"ts_ms": r.version.ts_ms, "node_id": r.version.node_id},
                }
            return out

class HintStore:
    """
    Holds hinted handoff writes: target_node_id -> list of ops.
    Each op is (op_type, key, value, version_dict).
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._hints: Dict[str, List[Dict[str, Any]]] = {}

    async def add_hint(self, target_node_id: str, op: Dict[str, Any]) -> None:
        async with self._lock:
            self._hints.setdefault(target_node_id, []).append(op)

    async def pop_batch(self, target_node_id: str, max_batch: int = 50) -> List[Dict[str, Any]]:
        async with self._lock:
            ops = self._hints.get(target_node_id, [])
            if not ops:
                return []
            batch = ops[:max_batch]
            self._hints[target_node_id] = ops[max_batch:]
            if not self._hints[target_node_id]:
                self._hints.pop(target_node_id, None)
            return batch

    async def targets(self) -> List[str]:
        async with self._lock:
            return list(self._hints.keys())

def now_ms() -> int:
    return int(time.time() * 1000)
