"""FastAPI front door. Each request is one Lua EVAL; the API holds no state of its own."""
import os
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from notifygate import service

app = FastAPI(title="NotifyGate")
_r = service.connect(os.environ.get("REDIS_URL", "redis://127.0.0.1:6380/2"))
_script = service.register(_r)


class ProfileIn(BaseModel):
    tz_offset: int = Field(0, ge=-12, le=14)
    quiet_start: Optional[int] = Field(None, ge=0, le=23)
    quiet_end: Optional[int] = Field(None, ge=0, le=23)
    caps: dict[str, int] = {}


class NotifyIn(BaseModel):
    user: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    channel: Literal["email", "sms", "push"]
    campaign: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    payload: dict = {}
    critical: bool = False
    now: Optional[int] = None


@app.post("/profiles/{user}")
def put_profile(user: str, body: ProfileIn):
    quiet = None
    if body.quiet_start is not None and body.quiet_end is not None:
        quiet = (body.quiet_start, body.quiet_end)
    service.set_profile(_r, user, body.tz_offset, quiet, body.caps)
    return {"user": user, "ok": True}


@app.post("/notify")
def notify(body: NotifyIn):
    decision, idem = service.decide(_r, _script, body.user, body.channel, body.campaign,
                                    body.payload, body.now, body.critical)
    return {"decision": decision, "idempotency_key": idem or None}


@app.get("/healthz")
def healthz():
    _r.ping()
    return {"ok": True}
