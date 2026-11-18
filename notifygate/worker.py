"""Outbox publisher: reads approvals from the outbox stream, publishes each to the deliveries log,
then acks. Stands in for a Kafka producer loop.

At-least-once by construction: a crash after publish and before ack leaves the entry pending, and the
next worker reclaims it (XAUTOCLAIM) and publishes again. The provider deduplicates on the
idempotency key, so the effect is exactly once even though the raw log can hold duplicates.

KILL_AT=read|publish|ack and KILL_AFTER=n make the worker SIGKILL itself at that point on the n-th
entry (crash injection). The kill is a real OS signal, not an exception.
"""
import argparse
import os
import signal
import time

from notifygate import service

GROUP = "publishers"


def _maybe_kill(point, kill_at, kill_after, counter):
    if kill_at == point and counter == kill_after:
        os.kill(os.getpid(), signal.SIGKILL)


def run(r, consumer, kill_at=None, kill_after=0, idle_exit_s=1.0, min_idle_ms=200):
    try:
        r.xgroup_create(service.OUTBOX, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    n = 0
    idle_since = None
    while True:
        _, claimed, _ = r.xautoclaim(service.OUTBOX, GROUP, consumer, min_idle_time=min_idle_ms,
                                     start_id="0-0", count=200)
        entries = [e for e in claimed if e]
        if not entries:
            res = r.xreadgroup(GROUP, consumer, {service.OUTBOX: ">"}, count=200, block=200)
            entries = res[0][1] if res else []
        if not entries:
            idle_since = idle_since or time.time()
            if time.time() - idle_since >= idle_exit_s:
                return n
            continue
        idle_since = None
        for eid, fields in entries:
            n += 1
            _maybe_kill("read", kill_at, kill_after, n)
            r.xadd(service.DELIVERIES, {"idem": fields["idem"], "user": fields["user"],
                                        "channel": fields["channel"], "campaign": fields["campaign"],
                                        "payload": fields["payload"]})
            _maybe_kill("publish", kill_at, kill_after, n)
            r.xack(service.OUTBOX, GROUP, eid)
            _maybe_kill("ack", kill_at, kill_after, n)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", required=True)
    ap.add_argument("--consumer", default=f"w-{os.getpid()}")
    ap.add_argument("--kill-at", default=None)
    ap.add_argument("--kill-after", type=int, default=0)
    a = ap.parse_args()
    print(run(service.connect(a.redis, db=None), a.consumer, a.kill_at, a.kill_after), flush=True)
