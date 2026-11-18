"""Decision correctness under concurrency, quiet hours, day boundaries, API validation, crash suite.
Run: python3 -m pytest -q tests/   (needs Redis on 127.0.0.1:6380 and PostgreSQL on 5434)
"""
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6380/2")
from notifygate import service  # noqa: E402

TEST_URL = "redis://127.0.0.1:6380/4"
DAY = 86400
T0 = 1_700_006_400  # 00:00 UTC on a day boundary


@pytest.fixture()
def r():
    client = service.connect(TEST_URL)
    client.flushdb()
    return client


def _u():
    return "u" + uuid.uuid4().hex[:8]


def test_cap_is_exact_under_concurrency(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, caps={"sms": 5})
    def go(i):
        return service.decide(r, script, user, "sms", f"c{i}", now=T0 + 3600)[0]
    with ThreadPoolExecutor(32) as ex:
        results = list(ex.map(go, range(200)))
    assert results.count("APPROVED") == 5
    assert results.count("CAP_EXCEEDED") == 195


def test_dedupe_approves_exactly_once_under_concurrency(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, caps={"email": 10**6})
    def go(_):
        return service.decide(r, script, user, "email", "same-campaign", now=T0 + 3600)[0]
    with ThreadPoolExecutor(32) as ex:
        results = list(ex.map(go, range(100)))
    assert results.count("APPROVED") == 1
    assert results.count("DUP_CAMPAIGN") == 99


def test_cap_resets_on_the_next_local_day(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, caps={"push": 1})
    assert service.decide(r, script, user, "push", "a", now=T0 + 100)[0] == "APPROVED"
    assert service.decide(r, script, user, "push", "b", now=T0 + 200)[0] == "CAP_EXCEEDED"
    assert service.decide(r, script, user, "push", "c", now=T0 + DAY + 100)[0] == "APPROVED"


def test_quiet_hours_wrap_midnight_and_respect_timezone(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, tz_offset=0, quiet=(22, 7))
    at = lambda h: T0 + h * 3600  # UTC hour h
    assert service.decide(r, script, user, "email", "q1", now=at(23))[0] == "QUIET_HOURS"
    assert service.decide(r, script, user, "email", "q2", now=at(3))[0] == "QUIET_HOURS"
    assert service.decide(r, script, user, "email", "q3", now=at(12))[0] == "APPROVED"
    assert service.decide(r, script, user, "email", "q4", now=at(7))[0] == "APPROVED"  # end is exclusive
    service.set_profile(r, user, tz_offset=-5, quiet=(22, 7))   # local 22:00 = UTC 03:00
    assert service.decide(r, script, user, "email", "q5", now=at(3))[0] == "QUIET_HOURS"
    assert service.decide(r, script, user, "email", "q6", now=at(23))[0] == "APPROVED"


def test_critical_bypasses_quiet_hours_but_not_dedupe(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, quiet=(0, 23))
    assert service.decide(r, script, user, "sms", "otp", now=T0 + 3600, critical=True)[0] == "APPROVED"
    assert service.decide(r, script, user, "sms", "otp", now=T0 + 3600, critical=True)[0] == "DUP_CAMPAIGN"


def test_rejected_decisions_write_nothing(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, caps={"sms": 0})
    before = r.xlen(service.OUTBOX)
    assert service.decide(r, script, user, "sms", "x", now=T0)[0] == "CAP_EXCEEDED"
    assert r.xlen(service.OUTBOX) == before and not r.exists(f"dedupe:x:{user}")


def test_approval_and_outbox_are_written_together(r):
    script = service.register(r)
    user = _u()
    service.set_profile(r, user, caps={"email": 3})
    d, idem = service.decide(r, script, user, "email", "y", {"k": 1}, now=T0)
    assert d == "APPROVED"
    assert r.sismember(service.APPROVED, idem)
    assert r.xlen(service.OUTBOX) == 1


def test_api_validates_and_decides():
    from fastapi.testclient import TestClient
    from notifygate.api import app
    c = TestClient(app)
    assert c.post("/notify", json={"user": "a b", "channel": "email", "campaign": "c"}).status_code == 422
    assert c.post("/notify", json={"user": "ok", "channel": "fax", "campaign": "c"}).status_code == 422
    assert c.post("/profiles/apiuser", json={"tz_offset": 0, "caps": {"email": 2}}).status_code == 200
    body = {"user": "apiuser", "channel": "email", "campaign": f"api{uuid.uuid4().hex[:6]}", "now": T0}
    res = c.post("/notify", json=body).json()
    assert res["decision"] == "APPROVED" and res["idempotency_key"]


def test_crash_suite_one_trial_per_kill_point():
    from bench.crash_suite import trial
    for point in ("read", "publish", "ack"):
        t = trial(point, seed=7, n=120)
        assert t["killed_by_sigkill"]
        assert t["lost"] == 0 and t["unexpected"] == 0
        assert t["effective_duplicates"] == 0 and t["outbox_pending"] == 0
