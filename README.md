# NotifyGate: Multi-Channel Notification Budgeter

A FastAPI service that decides whether a notification may go out. Each decision applies per-user
channel caps, quiet hours, and campaign deduplication in one atomic Redis Lua script. An approval is
written to an outbox in the same script. A worker publishes approvals to a delivery log, and a
provider records each send once, keyed by idempotency key. Crash-injection tests kill the worker at
three points and check that nothing is lost and nothing is double-sent.

## What is real and what is substituted

| Brief item | Here | Status |
|---|---|---|
| FastAPI | `notifygate/api.py`, uvicorn, one worker | Real |
| Redis, atomic Lua | Redis 7.0.15 on 127.0.0.1:6380. One `EVAL` per decision. | Real |
| Kafka | **Redis Streams** (`outbox`, `deliveries`) with consumer groups, pending entries and `XAUTOCLAIM`. | **Substituted.** Same at-least-once semantics for this workload; no brokers, partitions or replication. |
| Idempotency keys | `campaign:user:channel`, enforced by a PostgreSQL unique key in the provider | Real |
| Worker crash-injection | Real `SIGKILL` of a worker subprocess at three points (after read, after publish, after ack) | Real |
| Redis durability | Redis runs without AOF or RDB. Redis itself is never killed. | Not tested |

## Design

- **Decision (one `EVAL`):** quiet hours are evaluated in the user's local time (a UTC offset in the profile, using Lua's floored modulo, which is correct for negative times). Campaign dedupe comes next: `SET ... NX`-style existence check, one message per campaign per user. The daily channel cap is `INCR` on a per-user, per-channel, per-local-day key, with `DECR` rollback on overshoot. On approval, the script writes the dedupe key, adds the idempotency key to an approved set, and `XADD`s the outbox entry. Redis runs scripts without interleaving, so two requests can't both take the last cap unit.
- **Decisions returned:** `APPROVED`, `QUIET_HOURS`, `DUP_CAMPAIGN`, `CAP_EXCEEDED`. Rejections write nothing (tested).
- **Critical messages** bypass quiet hours and still respect dedupe.
- **Worker:** reclaims stale pending entries with `XAUTOCLAIM`, publishes to `deliveries`, then `XACK`s. Publish happens before ack, so a crash in between causes a re-publish.
- **Provider:** `INSERT ... ON CONFLICT (idem) DO NOTHING` in PostgreSQL. This is what makes the at-least-once log behave as exactly once for the send.

## Results

### Correctness under concurrency (tests, `python3 -m pytest -q tests/`, 9 pass)

- 200 concurrent requests against a cap of 5: exactly 5 approved, 195 `CAP_EXCEEDED`.
- 100 concurrent requests for the same campaign and user: exactly 1 approved.
- Cap resets on the next local day. Daily window boundaries are tested.
- Quiet hours wrap midnight. The end hour is exclusive. A UTC offset moves the window (checked both directions).
- Critical bypasses quiet hours, but a duplicate critical is still rejected.
- Rejected decisions write no outbox entry and no dedupe key.
- Approval and outbox entry are written together.
- API rejects bad user IDs and unknown channels (422) and decides correctly end to end.
- One crash trial per kill point passes.

### Crash-injection suite (`results/crash_suite.json`, 10 trials per kill point, 300 approvals each)

| Kill point | Trials, all SIGKILLed | Lost | Unexpected sends | Effective duplicates at provider | Raw-log duplicates | Outbox pending at end |
|---|---|---|---|---|---|---|
| after read, before publish | 10/10 | 0 | 0 | 0 | 0 | 0 |
| after publish, before ack | 10/10 | 0 | 0 | 0 | **10 trials** | 0 |
| after ack | 10/10 | 0 | 0 | 0 | 0 | 0 |

The publish-kill row is the important one. The raw deliveries log does contain duplicates, because
publish happened and the ack never did. The provider absorbs every one of them, so the effective
send count is exact. Without the provider's idempotency key, these duplicates would reach users.

### Load (`results/load.json`, uvicorn single worker, 2 cores, Redis on localhost)

| Concurrency | Requests | Throughput | p50 | p99 |
|---|---|---|---|---|
| 8 | 5,000 | 836 rps | 7.4 ms | 31 ms |
| 32 | 5,000 | 609 rps | 36.5 ms | 224 ms |
| 64 | 5,000 | 871 rps | 65.9 ms | 122 ms |

Caps and dedupe are reset before each level, so each run starts clean. The 32-concurrency p99 is
noisy: a single run on a shared 2-core host. Throughput is limited by the single Python process and
FastAPI, not by Redis. These are local numbers, not production capacity.

## Limits

- Redis Streams stand in for Kafka. Kafka's partitioning, retention and replication are not exercised.
- Redis is not persisted here (no AOF). A Redis crash would lose outbox state. Durability needs AOF with fsync, or a replica.
- Cap and dedupe keys are built inside the Lua script. That works on one Redis node. A Redis cluster would need hash tags so the keys share a slot.
- The provider is a single consumer. Its own crash is not tested, only the worker's.
- Quiet hours use a fixed UTC offset per user, not a timezone database (no DST).
- Load numbers come from one host and one uvicorn worker.

## Run

```
pip install -r requirements.txt
redis-server --port 6380 --save "" --appendonly no
# PostgreSQL 16 on 127.0.0.1:5434, database notifygate (UTF-8)
python3 -m pytest -q tests/
python3 -m bench.crash_suite 10       # writes results/crash_suite.json
python3 -m bench.load_test            # writes results/load.json
```
