import asyncio
from typing import Any, Dict, Optional, List, Tuple

import aiohttp

from ring import ConsistentHashRing
from storage import InMemoryStorage, HintStore, Version, Record, now_ms


class DynamoNode:
    def __init__(
        self,
        node_id: str,
        ring: ConsistentHashRing,
        storage: InMemoryStorage,
        hint_store: HintStore,
        rf: int,
        r_quorum: int,
        w_quorum: int,
        request_timeout_s: float = 0.25,
    ):
        self.node_id = node_id
        self.ring = ring
        self.storage = storage
        self.hint_store = hint_store
        self.rf = int(rf)
        self.r_quorum = int(r_quorum)
        self.w_quorum = int(w_quorum)
        self.request_timeout_s = float(request_timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._handoff_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.request_timeout_s)
            self._session = aiohttp.ClientSession(timeout=timeout)
        if self._handoff_task is None:
            self._handoff_task = asyncio.create_task(self._handoff_loop())

    async def close(self):
        if self._handoff_task:
            self._handoff_task.cancel()
        if self._session:
            await self._session.close()

    # -------------------------
    # Public (client-facing) ops
    # -------------------------

    async def client_put(self, key: str, value: Any) -> Dict[str, Any]:
        ver = Version(ts_ms=now_ms(), node_id=self.node_id)
        pref = self.ring.preference_list(key, self.rf)
        acks = await self._replicate_write(op="put", key=key, value=value, version=ver, pref=pref)
        ok = acks >= self.w_quorum
        return {"ok": ok, "acks": acks, "n": self.rf, "w": self.w_quorum, "version": _ver_to_dict(ver)}

    async def client_delete(self, key: str) -> Dict[str, Any]:
        ver = Version(ts_ms=now_ms(), node_id=self.node_id)
        pref = self.ring.preference_list(key, self.rf)
        acks = await self._replicate_write(op="del", key=key, value=None, version=ver, pref=pref)
        ok = acks >= self.w_quorum
        return {"ok": ok, "acks": acks, "n": self.rf, "w": self.w_quorum, "version": _ver_to_dict(ver)}

    async def client_get(self, key: str) -> Dict[str, Any]:
        pref = self.ring.preference_list(key, self.rf)
        replies = await self._replica_read_quorum(key, pref)

        if len(replies) < self.r_quorum:
            return {
                "ok": False,
                "error": "read_quorum_not_met",
                "responses": len(replies),
                "r": self.r_quorum,
                "n": self.rf,
            }

        latest = _pick_latest(replies)
        asyncio.create_task(self._read_repair(key, latest, replies, pref))

        if latest is None:
            return {"ok": True, "found": False, "value": None, "version": None}

        if latest.value is None:
            return {"ok": True, "found": False, "value": None, "version": _ver_to_dict(latest.version)}

        return {"ok": True, "found": True, "value": latest.value, "version": _ver_to_dict(latest.version)}

    # -------------------------
    # Internal replica endpoints
    # -------------------------

    async def replica_put(self, key: str, value: Any, version: Version) -> Dict[str, Any]:
        rec = await self.storage.put(key, value, version)
        return {"ok": True, "stored_version": _ver_to_dict(rec.version)}

    async def replica_delete(self, key: str, version: Version) -> Dict[str, Any]:
        rec = await self.storage.delete(key, version)
        return {"ok": True, "stored_version": _ver_to_dict(rec.version)}

    async def replica_get(self, key: str) -> Dict[str, Any]:
        rec = await self.storage.get(key)
        if rec is None:
            return {"ok": True, "record": None}
        return {"ok": True, "record": {"value": rec.value, "version": _ver_to_dict(rec.version)}}

    # -------------------------
    # Quorum / replication helpers
    # -------------------------

    async def _replicate_write(self, op: str, key: str, value: Any, version: Version, pref: List[str]) -> int:
        """
        Fire write to all N replicas; count acks (including local if applicable).
        If a replica is unreachable, store a hint for it (hinted handoff).
        """
        assert op in ("put", "del")
        tasks: List[asyncio.Task] = []
        acks = 0

        # local write if this node is in preference list
        if self.node_id in pref:
            if op == "put":
                await self.storage.put(key, value, version)
            else:
                await self.storage.delete(key, version)
            acks += 1

        # remote writes
        for nid in pref:
            if nid == self.node_id:
                continue
            tasks.append(asyncio.create_task(self._send_replica_write(nid, op, key, value, version)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for item in results:
                if isinstance(item, Exception):
                    # Some unexpected failure (timeout/cancel/etc.). 
                    # Quorum will be computed from successful acks.
                    continue

                nid, success = item
                if success:
                    acks += 1
                else:
                    # store hint for later delivery
                    await self.hint_store.add_hint(
                        nid,
                        {
                            "op": op,
                            "key": key,
                            "value": value,
                            "version": _ver_to_dict(version),
                        },
                    )

        return acks

    async def _send_replica_write(
        self, target_node_id: str, op: str, key: str, value: Any, version: Version
    ) -> Tuple[str, bool]:
        """
        Returns (target_node_id, success_bool). Never raises (best effort).
        """
        if self._session is None:
            return (target_node_id, False)

        peer = self.ring.peer(target_node_id)
        url = f"{peer.base_url}/internal/replica/{key}"
        payload = {"op": op, "value": value, "version": _ver_to_dict(version)}

        try:
            async with self._session.put(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return (target_node_id, bool(data.get("ok", False)))
                return (target_node_id, False)
        except Exception:
            return (target_node_id, False)

    async def _replica_read_quorum(self, key: str, pref: List[str]) -> List[Optional[Record]]:
        """
        Query all N replicas. Collect up to N responses; return list of Records (or None if missing).
        Only successful HTTP 200 with ok=True count as a response.
        """
        if self._session is None:
            raise RuntimeError("Node not started; call await node.start()")

        async def read_one(nid: str) -> Tuple[str, Optional[Record], bool]:
            if nid == self.node_id:
                rec = await self.storage.get(key)
                return (nid, rec, True)

            peer = self.ring.peer(nid)
            url = f"{peer.base_url}/internal/replica/{key}"
            try:
                async with self._session.get(url) as resp:
                    if resp.status != 200:
                        return (nid, None, False)
                    data = await resp.json()
                    if not data.get("ok"):
                        return (nid, None, False)
                    rec_payload = data.get("record")
                    if rec_payload is None:
                        return (nid, None, True)
                    v = _dict_to_ver(rec_payload["version"])
                    return (nid, Record(value=rec_payload.get("value"), version=v), True)
            except Exception:
                return (nid, None, False)

        tasks = [asyncio.create_task(read_one(nid)) for nid in pref]
        replies: List[Optional[Record]] = []
        ok_count = 0

        for fut in asyncio.as_completed(tasks, timeout=self.request_timeout_s * 2):
            try:
                nid, rec, ok = await fut
                if ok:
                    replies.append(rec)
                    ok_count += 1
                    if ok_count >= self.r_quorum:
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        break
            except Exception:
                continue

        return replies

    async def _read_repair(
        self, key: str, latest: Optional[Record], replies: List[Optional[Record]], pref: List[str]
    ) -> None:
        """
        If latest exists, push it to replicas that are stale/missing.
        If latest is tombstone, also repair deletes.
        """
        if latest is None:
            return

        op = "del" if latest.value is None else "put"
        tasks = []
        for nid in pref:
            if nid == self.node_id:
                if op == "put":
                    await self.storage.put(key, latest.value, latest.version)
                else:
                    await self.storage.delete(key, latest.version)
                continue
            tasks.append(asyncio.create_task(self._send_replica_write(nid, op, key, latest.value, latest.version)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------
    # Hinted handoff background loop
    # -------------------------

    async def _handoff_loop(self) -> None:
        """
        Periodically attempts to deliver hints to their target nodes.
        """
        while True:
            try:
                await asyncio.sleep(0.5)
                targets = await self.hint_store.targets()
                if not targets:
                    continue
                for target in targets:
                    batch = await self.hint_store.pop_batch(target, max_batch=50)
                    if not batch:
                        continue
                    for op in batch:
                        ok = await self._deliver_hint(target, op)
                        if not ok:
                            await self.hint_store.add_hint(target, op)
                            break
            except asyncio.CancelledError:
                return
            except Exception:
                continue

    async def _deliver_hint(self, target_node_id: str, op: Dict[str, Any]) -> bool:
        """
        Deliver a hinted write to target. Uses same /internal/replica endpoint.
        """
        if self._session is None:
            return False

        peer = self.ring.peer(target_node_id)
        key = op["key"]
        url = f"{peer.base_url}/internal/replica/{key}"

        try:
            async with self._session.put(url, json=op) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return bool(data.get("ok", False))
        except Exception:
            return False

        return False


# -------------------------
# Helpers
# -------------------------

def _ver_to_dict(v: Version) -> Dict[str, Any]:
    return {"ts_ms": v.ts_ms, "node_id": v.node_id}


def _dict_to_ver(d: Dict[str, Any]) -> Version:
    return Version(ts_ms=int(d["ts_ms"]), node_id=str(d["node_id"]))


def _pick_latest(recs: List[Optional[Record]]) -> Optional[Record]:
    best: Optional[Record] = None
    for r in recs:
        if r is None:
            continue
        if best is None or r.version.as_tuple() >= best.version.as_tuple():
            best = r
    return best
