"""Idempotent provider: reads the deliveries log and records each send once.

The sink is PostgreSQL. A unique primary key on the idempotency key plus ON CONFLICT DO NOTHING turns
the at-least-once log into one effective send per key.
"""
import psycopg

from notifygate import service

GROUP = "provider"
SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_sends (
    idem text PRIMARY KEY, user_id text NOT NULL, channel text NOT NULL,
    campaign text NOT NULL, payload text NOT NULL, sent_at timestamptz DEFAULT now());
"""


def run(r, pg_dsn, consumer="p-1", idle_exit_s=1.5):
    conn = psycopg.connect(pg_dsn, autocommit=True)
    conn.execute(SCHEMA)
    try:
        r.xgroup_create(service.DELIVERIES, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    seen_new, idle_since = 0, None
    while True:
        res = r.xreadgroup(GROUP, consumer, {service.DELIVERIES: ">"}, count=500, block=200)
        entries = res[0][1] if res else []
        if not entries:
            idle_since = idle_since or __import__("time").time()
            if __import__("time").time() - idle_since >= idle_exit_s:
                return seen_new
            continue
        idle_since = None
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO provider_sends (idem, user_id, channel, campaign, payload) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (idem) DO NOTHING",
                    [(f["idem"], f["user"], f["channel"], f["campaign"], f["payload"]) for _, f in entries])
        r.xack(service.DELIVERIES, GROUP, *[eid for eid, _ in entries])
        seen_new += len(entries)
