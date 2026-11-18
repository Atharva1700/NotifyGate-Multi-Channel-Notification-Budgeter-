"""Budget decisions as one atomic Redis Lua script.

One EVAL does, in order: quiet-hours check (user's local hour from a UTC offset), campaign dedupe
(one message per campaign per user), daily per-channel cap (INCR with rollback on overshoot), and,
if approved, the approval record plus the outbox entry. Redis runs a script without interleaving,
so no two requests can both take the last unit of a cap, and approval and outbox never diverge.

Decisions: APPROVED, QUIET_HOURS, DUP_CAMPAIGN, CAP_EXCEEDED.
Keys used by the script are built in Lua (cap keys are per day), which is fine on one Redis node
but would need hash tags on a cluster; see README.
"""
import json
import time

import redis

CHANNELS = ("email", "sms", "push")
OUTBOX = "outbox"
DELIVERIES = "deliveries"
APPROVED = "approved"

LUA = """
-- KEYS[1] dedupe key, KEYS[2] cap base, KEYS[3] outbox stream, KEYS[4] profile hash, KEYS[5] approved set
-- ARGV[1] now, ARGV[2] critical 1/0, ARGV[3] user, ARGV[4] channel, ARGV[5] campaign,
-- ARGV[6] default cap, ARGV[7] dedupe ttl, ARGV[8] payload json
local now = tonumber(ARGV[1])
local p = KEYS[4]
local off = tonumber(redis.call('HGET', p, 'tz_offset') or '0')
local qs = tonumber(redis.call('HGET', p, 'quiet_start') or '-1')
local qe = tonumber(redis.call('HGET', p, 'quiet_end') or '-1')
local cap = tonumber(redis.call('HGET', p, 'cap_' .. ARGV[4]) or ARGV[6])
local local_t = now + off * 3600
local hour = math.floor((local_t % 86400) / 3600)
if ARGV[2] ~= '1' and qs >= 0 and qe >= 0 then
  local inq
  if qs < qe then inq = (hour >= qs and hour < qe) else inq = (hour >= qs or hour < qe) end
  if inq then return {'QUIET_HOURS', ''} end
end
if redis.call('EXISTS', KEYS[1]) == 1 then return {'DUP_CAMPAIGN', ''} end
local ck = KEYS[2] .. ':' .. math.floor(local_t / 86400)
local used = redis.call('INCR', ck)
if used == 1 then redis.call('EXPIRE', ck, 172800) end
if used > cap then
  redis.call('DECR', ck)
  return {'CAP_EXCEEDED', ''}
end
local idem = ARGV[5] .. ':' .. ARGV[3] .. ':' .. ARGV[4]
redis.call('SET', KEYS[1], idem, 'EX', tonumber(ARGV[7]))
redis.call('SADD', KEYS[5], idem)
redis.call('XADD', KEYS[3], '*', 'idem', idem, 'user', ARGV[3], 'channel', ARGV[4],
           'campaign', ARGV[5], 'payload', ARGV[8])
return {'APPROVED', idem}
"""


def connect(url, db=None):
    return redis.Redis.from_url(url, decode_responses=True, db=db) if db is not None \
        else redis.Redis.from_url(url, decode_responses=True)


def set_profile(r, user, tz_offset=0, quiet=None, caps=None):
    """quiet = (start_hour, end_hour) in local time, or None. caps = {channel: daily cap}."""
    fields = {"tz_offset": int(tz_offset), "quiet_start": -1, "quiet_end": -1}
    if quiet is not None:
        fields["quiet_start"], fields["quiet_end"] = quiet
    for ch, c in (caps or {}).items():
        fields[f"cap_{ch}"] = int(c)
    r.hset(f"profile:{user}", mapping=fields)


def decide(r, script, user, channel, campaign, payload=None, now=None, critical=False,
           default_cap=5, dedupe_ttl=7 * 86400):
    if channel not in CHANNELS:
        raise ValueError(f"unknown channel {channel!r}")
    now = int(time.time() if now is None else now)
    decision, idem = script(
        keys=[f"dedupe:{campaign}:{user}", f"cap:{user}:{channel}", OUTBOX, f"profile:{user}", APPROVED],
        args=[now, "1" if critical else "0", user, channel, campaign, default_cap, dedupe_ttl,
              json.dumps(payload or {}, sort_keys=True)])
    return decision, idem


def register(r):
    return r.register_script(LUA)
