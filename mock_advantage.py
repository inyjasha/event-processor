from fastapi import FastAPI
from decimal import Decimal
from pydantic import AwareDatetime, BaseModel

app = FastAPI(title="AdVantage — локальный тестовый сервер")


class AdvantageEvent(BaseModel):
    clid: str
    payout: Decimal
    click_spend: Decimal
    click_ts: AwareDatetime
    payment_ts: AwareDatetime
    payout_currency: str
    click_spend_currency: str

@app.post("/api/v3/click/", status_code=200)
async def receive_event(event: AdvantageEvent):
    return {
        "accepted": True,
        "clid": event.clid,
    }