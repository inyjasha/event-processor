import httpx
import os
from pathlib import Path
from dotenv import load_dotenv
import math
import simplejson


ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=False)

ADVANTAGE_URL = os.environ["ADVANTAGE_URL"]
ADVANTAGE_TOKEN = os.environ["ADVANTAGE_TOKEN"]
PAYOUT_CURRENCY = os.environ["PAYOUT_CURRENCY"]
CLICK_SPEND_CURRENCY = os.environ["CLICK_SPEND_CURRENCY"]
HTTP_TIMEOUT = float(os.environ["ADVANTAGE_HTTP_TIMEOUT"])

if not math.isfinite(HTTP_TIMEOUT) or HTTP_TIMEOUT <= 0:
    raise ValueError(
        "ADVANTAGE_HTTP_TIMEOUT должен быть положительным конечным числом"
    )

if not ADVANTAGE_URL.strip():
    raise ValueError("ADVANTAGE_URL не должен быть пустым")

if not ADVANTAGE_TOKEN.strip():
    raise ValueError("ADVANTAGE_TOKEN не должен быть пустым")

def create_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"Authorization": f"Bearer {ADVANTAGE_TOKEN}"},
        timeout=HTTP_TIMEOUT,
        follow_redirects=False,
    )

def build_payload(task: dict) -> dict:
    return {
        "clid": task["clid"],
        "payout": task["payout"],
        "click_spend": task["click_spend"],
        "click_ts": task["click_ts"].isoformat(),
        "payment_ts": task["ts"].isoformat(),
        "payout_currency": PAYOUT_CURRENCY,
        "click_spend_currency": CLICK_SPEND_CURRENCY,
    }

def serialize_payload(payload: dict) -> str:
    return simplejson.dumps(
        payload,
        use_decimal=True,
        ensure_ascii=False,
        allow_nan=False,
    )

async def send_payment(
        client: httpx.AsyncClient,
        task: dict,
) -> int:
    payload = build_payload(task)
    body = serialize_payload(payload)
    response = await client.post(
        ADVANTAGE_URL,
        content=body.encode("utf-8"),
        headers = {"Content-Type": "application/json",}
    )
    return response.status_code