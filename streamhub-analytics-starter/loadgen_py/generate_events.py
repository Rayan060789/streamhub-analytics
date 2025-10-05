
import argparse
import random
import time
import uuid
import requests
from datetime import datetime, timezone

def generate_event(user_pool=1000, video_pool=5000):
    return {
        "user_id": f"user-{random.randint(1, user_pool)}",
        "video_id": f"vid-{random.randint(1, video_pool)}",
        "watch_seconds": round(random.random() * 60, 3),
        "ts": datetime.now(timezone.utc).isoformat()
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=100, help="events per second")
    ap.add_argument("--url", type=str, default="http://localhost:8080/events")
    ap.add_argument("--batch", type=int, default=100)
    args = ap.parse_args()

    interval = 1.0
    print(f"posting ~{args.rate} evts/s to {args.url} (batch={args.batch}) ... Ctrl+C to stop")
    try:
        while True:
            batch = [generate_event() for _ in range(args.batch)]
            requests.post(args.url, json=batch, timeout=5)
            time.sleep(max(0, interval - (args.batch / max(1, args.rate))))
    except KeyboardInterrupt:
        print("stopped")

if __name__ == "__main__":
    main()
