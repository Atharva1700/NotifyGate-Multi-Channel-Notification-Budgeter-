"""Crash-injection suite for the outbox worker.

Each trial: flush Redis test DB and the provider table, enqueue N approvals through the real Lua
decision path, run a worker that SIGKILLs itself at KILL_AT on entry KILL_AFTER, restart a clean
worker, run the provider, then check: every approval delivered exactly once in the provider table,
nothing left pending in the outbox, and the raw deliveries log's duplicate count (expected > 0
when a kill lands after publish).
"""
import json
import os
import random
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from notifygate import provider, service  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6380/3")
PG_DSN = "host=127.0.0.1 port=5434 dbname=notifygate user=postgres"
PY = sys.executable


def _fresh():
    r = service.connect(REDIS_URL)
    r.flushdb()
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute(provider.SCHEMA)
        c.execute("TRUNCATE provider_sends")
    return r


def enqueue(r, n, seed):
    script = service.register(r)
    rng = random.Random(seed)
    approved = set()
    for i in range(n):
        user = f"u{rng.randrange(200)}"
        ch = rng.choice(service.CHANNELS)
        service.set_profile(r, user, caps={c: 10**6 for c in service.CHANNELS})
        d, idem = service.decide(r, script, user, ch, f"camp{i}", {"i": i}, now=1_700_000_000)
        if d == "APPROVED":
            approved.add(idem)
    return approved


def trial(kill_at, seed, n=300, kill_after=None):
    r = _fresh()
    approved = enqueue(r, n, seed)
    rng = random.Random(seed * 7 + 1)
    k = kill_after if kill_after is not None else rng.randint(1, n)
    base = [PY, "-m", "notifygate.worker", "--redis", REDIS_URL]
    p = subprocess.run(base + ["--consumer", "crashy", "--kill-at", kill_at, "--kill-after", str(k)],
                       cwd=ROOT, capture_output=True, timeout=120)
    killed = p.returncode == -9
    subprocess.run(base + ["--consumer", "clean"], cwd=ROOT, capture_output=True, timeout=120, check=True)
    provider.run(r, PG_DSN)
    with psycopg.connect(PG_DSN) as c:
        sent = [row[0] for row in c.execute("SELECT idem FROM provider_sends").fetchall()]
    raw = [f["idem"] for _, f in r.xrange(service.DELIVERIES)]
    pending = r.xpending(service.OUTBOX, "publishers")["pending"]
    return {"kill_at": kill_at, "kill_after": k, "killed_by_sigkill": killed,
            "approved": len(approved), "delivered_distinct": len(set(sent)),
            "lost": len(approved - set(sent)), "unexpected": len(set(sent) - approved),
            "effective_duplicates": len(sent) - len(set(sent)),
            "raw_log_duplicates": len(raw) - len(set(raw)), "outbox_pending": pending}


def main(trials=10, n=300):
    out = {}
    for point in ("read", "publish", "ack"):
        rows = [trial(point, seed=100 * (i + 1) + len(point), n=n) for i in range(trials)]
        out[point] = {
            "trials": len(rows),
            "all_sigkilled": all(x["killed_by_sigkill"] for x in rows),
            "max_lost": max(x["lost"] for x in rows),
            "max_unexpected": max(x["unexpected"] for x in rows),
            "total_effective_duplicates": sum(x["effective_duplicates"] for x in rows),
            "total_raw_log_duplicates": sum(x["raw_log_duplicates"] for x in rows),
            "trials_with_raw_duplicates": sum(1 for x in rows if x["raw_log_duplicates"] > 0),
            "max_outbox_pending": max(x["outbox_pending"] for x in rows),
        }
    return out


if __name__ == "__main__":
    res = main(int(sys.argv[1]) if len(sys.argv) > 1 else 10)
    out = ROOT / "results" / "crash_suite.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))
