import os
import asyncio
from typing import List, Dict, Any, Optional

from aiohttp import web

from ring import Peer, ConsistentHashRing
from storage import InMemoryStorage, HintStore, Version
from node import DynamoNode
import time

def parse_peers(peers_str: str) -> List[Peer]:
    """
    peers_str format:
      nodeA=http://127.0.0.1:8001,nodeB=http://127.0.0.1:8002,nodeC=http://127.0.0.1:8003
    """
    peers: List[Peer] = []
    for chunk in peers_str.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        node_id, url = chunk.split("=", 1)
        peers.append(Peer(node_id=node_id.strip(), base_url=url.strip().rstrip("/")))
    return peers

def make_app(node: DynamoNode) -> web.Application:
    app = web.Application()

    # ---- Client API ----
    async def health(request: web.Request):
        return web.json_response({"ok": True, "node_id": node.node_id})

    async def ring_info(request: web.Request):
        return web.json_response({
            "node_id": node.node_id,
            "peers": [{"node_id": p.node_id, "base_url": p.base_url} for p in node.ring.all_peers()],
            "rf": node.rf,
            "r": node.r_quorum,
            "w": node.w_quorum,
        })

    async def kv_get(request: web.Request):
        key = request.match_info["key"]
        start = time.perf_counter()
        out = await node.client_get(key)
        latency_ms = (time.perf_counter() - start) * 1000
        print(f"[GET] latency_ms={latency_ms:.2f}")

        key = request.match_info["key"]
        out = await node.client_get(key)
        status = 200 if out.get("ok", True) else 503
        return web.json_response(out, status=status)

    async def kv_put(request: web.Request):
        key = request.match_info["key"]
        start = time.perf_counter()
        out = await node.client_get(key)
        latency_ms = (time.perf_counter() - start) * 1000
        print(f"[GET] latency_ms={latency_ms:.2f}")
        
        key = request.match_info["key"]
        body = await request.json()
        if "value" not in body:
            return web.json_response({"ok": False, "error": "missing_value"}, status=400)
        out = await node.client_put(key, body["value"])
        status = 200 if out.get("ok") else 503
        return web.json_response(out, status=status)

    async def kv_delete(request: web.Request):
        key = request.match_info["key"]
        out = await node.client_delete(key)
        status = 200 if out.get("ok") else 503
        return web.json_response(out, status=status)

    # ---- Internal replica API ----
    async def replica_get(request: web.Request):
        key = request.match_info["key"]
        out = await node.replica_get(key)
        return web.json_response(out)

    async def replica_put(request: web.Request):
        key = request.match_info["key"]
        body = await request.json()
        op = body.get("op")
        version_dict = body.get("version")
        if op not in ("put", "del") or not isinstance(version_dict, dict):
            return web.json_response({"ok": False, "error": "bad_request"}, status=400)

        ver = Version(ts_ms=int(version_dict["ts_ms"]), node_id=str(version_dict["node_id"]))
        if op == "put":
            out = await node.replica_put(key, body.get("value"), ver)
        else:
            out = await node.replica_delete(key, ver)
        return web.json_response(out)

    # routes
    app.router.add_get("/health", health)
    app.router.add_get("/ring", ring_info)

    app.router.add_get("/kv/{key}", kv_get)
    app.router.add_put("/kv/{key}", kv_put)
    app.router.add_delete("/kv/{key}", kv_delete)

    app.router.add_get("/internal/replica/{key}", replica_get)
    app.router.add_put("/internal/replica/{key}", replica_put)

    async def on_startup(app: web.Application):
        await node.start()

    async def on_cleanup(app: web.Application):
        await node.close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

def main():
    node_id = os.environ.get("NODE_ID", "nodeA")
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8001"))

    peers_str = os.environ.get(
        "PEERS",
        "nodeA=http://127.0.0.1:8001,nodeB=http://127.0.0.1:8002,nodeC=http://127.0.0.1:8003",
    )
    peers = parse_peers(peers_str)

    vnodes = int(os.environ.get("VNODE_COUNT", "64"))
    rf = int(os.environ.get("N", "3"))
    r_quorum = int(os.environ.get("R", "2"))
    w_quorum = int(os.environ.get("W", "2"))
    timeout_s = float(os.environ.get("TIMEOUT_S", "0.25"))

    ring = ConsistentHashRing(peers=peers, vnodes=vnodes)
    storage = InMemoryStorage()
    hint_store = HintStore()
    node = DynamoNode(
        node_id=node_id,
        ring=ring,
        storage=storage,
        hint_store=hint_store,
        rf=rf,
        r_quorum=r_quorum,
        w_quorum=w_quorum,
        request_timeout_s=timeout_s,
    )

    app = make_app(node)
    web.run_app(app, host=host, port=port)

if __name__ == "__main__":
    main()
