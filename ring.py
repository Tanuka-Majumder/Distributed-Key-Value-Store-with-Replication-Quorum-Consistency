import hashlib
import bisect
from dataclasses import dataclass
from typing import Dict, List, Tuple

def _h(s: str) -> int:
    # 64-bit hash (stable across processes)
    digest = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)

@dataclass(frozen=True)
class Peer:
    node_id: str
    base_url: str  # e.g. http://127.0.0.1:8001

class ConsistentHashRing:
    """
    Consistent hashing ring with virtual nodes.
    Ring key space: 0..2^64-1 using SHA1 truncated.
    """
    def __init__(self, peers: List[Peer], vnodes: int = 64):
        if not peers:
            raise ValueError("peers must be non-empty")
        self.vnodes = int(vnodes)
        self.peers = list(peers)

        # ring_points: sorted list of (point, node_id)
        points: List[Tuple[int, str]] = []
        for p in self.peers:
            for i in range(self.vnodes):
                points.append((_h(f"{p.node_id}#{i}"), p.node_id))
        points.sort(key=lambda x: x[0])

        self._points = points
        self._points_only = [pt for pt, _ in points]
        self._node_to_peer: Dict[str, Peer] = {p.node_id: p for p in self.peers}

    def owner(self, key: str) -> str:
        """
        Return primary owner node_id for key.
        """
        k = _h(key)
        idx = bisect.bisect_left(self._points_only, k)
        if idx == len(self._points_only):
            idx = 0
        return self._points[idx][1]

    def preference_list(self, key: str, n: int) -> List[str]:
        """
        Return up to n distinct node_ids in ring order starting at key's position.
        """
        if n <= 0:
            return []
        k = _h(key)
        idx = bisect.bisect_left(self._points_only, k)
        if idx == len(self._points_only):
            idx = 0

        picked: List[str] = []
        seen = set()
        i = idx
        while len(picked) < min(n, len(self._node_to_peer)):
            node_id = self._points[i][1]
            if node_id not in seen:
                picked.append(node_id)
                seen.add(node_id)
            i = (i + 1) % len(self._points)
        return picked

    def peer(self, node_id: str) -> Peer:
        return self._node_to_peer[node_id]

    def all_peers(self) -> List[Peer]:
        return list(self.peers)
