import json
import random
import time

actions = ["login", "logout", "purchase", "click", "error", "upload"]
users = [f"user_{i:05d}" for i in range(1, 10000)]

print("Generating 10,000,000 log records...")
batch_size = 50000
buffer = []

with open("data/massive_logs.txt", "w") as f:
    for i in range(1, 10000001):
        record = {
            "id": i,
            "timestamp": int(time.time()) - random.randint(0, 100000),
            "user": random.choice(users),
            "action": random.choice(actions),
            "payload_size": random.randint(64, 2048)
        }
        buffer.append(json.dumps(record) + "\n")
        
        if len(buffer) >= batch_size:
            f.writelines(buffer)
            buffer = []

    if buffer:
        f.writelines(buffer)

print("Done! File saved as massive_logs.txt")