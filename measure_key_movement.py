from ring import Peer, ConsistentHashRing
import random
import string

def rand_key():
    return ''.join(random.choices(string.ascii_letters, k=16))

# baseline cluster
peers_before = [
    Peer("A", "http://a"),
    Peer("B", "http://b"),
    Peer("C", "http://c"),
]

# after adding a node
peers_after = peers_before + [Peer("D", "http://d")]

ring_before = ConsistentHashRing(peers_before, vnodes=64)
ring_after = ConsistentHashRing(peers_after, vnodes=64)

keys = [rand_key() for _ in range(50000)]

moved = sum(
    1 for k in keys
    if ring_before.owner(k) != ring_after.owner(k)
)

print("Key movement %:", moved / len(keys) * 100)
