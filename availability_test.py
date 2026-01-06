import requests

success = 0
total = 1000

for _ in range(total):
    r = requests.get("http://127.0.0.1:8001/kv/testkey")
    if r.status_code == 200:
        success += 1

print("Availability:", success / total)
