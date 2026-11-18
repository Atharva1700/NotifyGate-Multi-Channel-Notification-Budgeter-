"""Load test against a live uvicorn server: open-loop style concurrency with httpx, mixed users,
channels and campaigns. Reports throughput, latency percentiles and decision mix.
"""
import asyncio
import json
import random
import subprocess
import sys
import time
from pathlib import Path

import httpx
import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from notifygate import service  # noqa: E402

PORT = 8088
REDIS_URL = "redis://127.0.0.1:6380/2"


async def blast(n, users, concurrency, seed):
    rng = random.Random(seed)
    lat, decisions = [], {}
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{PORT}", timeout=10) as c:
        async def one(i):
            body = {"user": f"lu{rng.randrange(users)}", "channel": rng.choice(service.CHANNELS),
                    "campaign": f"lc{rng.randrange(40)}", "now": 1_700_006_400 + 3600 * rng.randrange(24)}
            async with sem:
                t = time.perf_counter()
                res = await c.post("/notify", json=body)
                lat.append((time.perf_counter() - t) * 1000)
            d = res.json()["decision"]
            decisions[d] = decisions.get(d, 0) + 1
        t0 = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(n)))
        wall = time.perf_counter() - t0
    lat.sort()
    pct = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 2)
    return {"requests": n, "concurrency": concurrency, "wall_s": round(wall, 2),
            "rps": round(n / wall, 1), "p50_ms": pct(0.5), "p99_ms": pct(0.99), "decisions": decisions}


def main():
    r = redis.Redis.from_url(REDIS_URL)
    r.flushdb()
    users = 2000
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "notifygate.api:app", "--port", str(PORT),
                             "--log-level", "warning", "--workers", "1"], cwd=ROOT,
                            env={**__import__("os").environ, "REDIS_URL": REDIS_URL})
    try:
        for _ in range(100):
            try:
                if httpx.get(f"http://127.0.0.1:{PORT}/healthz").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        runs = []
        for conc in (8, 32, 64):
            r.flushdb()   # each run starts from empty caps and dedupe state
            with httpx.Client(base_url=f"http://127.0.0.1:{PORT}", timeout=10) as c:
                for u in range(users):
                    c.post(f"/profiles/lu{u}", json={"tz_offset": u % 12 - 6, "quiet_start": 22,
                                                      "quiet_end": 7, "caps": {"email": 3, "sms": 2, "push": 4}})
            runs.append(asyncio.run(blast(5000, users, conc, seed=conc)))
    finally:
        proc.terminate()
        proc.wait()
    out = ROOT / "results" / "load.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(runs, indent=2))
    print(json.dumps(runs, indent=2))


if __name__ == "__main__":
    main()
