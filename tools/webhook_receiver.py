"""Mock webhook receiver for local demos.

POST /webhook          always answers 200
POST /webhook/flaky    answers 500 to the first two deliveries of each payment, then 200
POST /webhook/fail     always answers 500 (the message ends up in the DLQ)
GET  /webhooks         everything received so far
"""

import logging
from collections import Counter
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [webhook] %(message)s")
logger = logging.getLogger("webhook")

app = FastAPI(title="Webhook receiver")
received: list[dict[str, Any]] = []
flaky_hits: Counter[str] = Counter()


async def _record(request: Request, mode: str) -> dict[str, Any]:
    body = await request.json()
    received.append({"mode": mode, "headers": dict(request.headers), "body": body})
    logger.info("%s: %s", mode, body)
    return body


@app.post("/webhook")
async def ok(request: Request) -> dict[str, str]:
    await _record(request, "ok")
    return {"status": "received"}


@app.post("/webhook/flaky")
async def flaky(request: Request) -> JSONResponse:
    body = await _record(request, "flaky")
    flaky_hits[body["payment_id"]] += 1
    if flaky_hits[body["payment_id"]] <= 2:
        return JSONResponse({"status": "temporary error"}, status_code=500)
    return JSONResponse({"status": "received"})


@app.post("/webhook/fail")
async def fail(request: Request) -> JSONResponse:
    await _record(request, "fail")
    return JSONResponse({"status": "error"}, status_code=500)


@app.get("/webhooks")
async def list_webhooks() -> list[dict[str, Any]]:
    return received
