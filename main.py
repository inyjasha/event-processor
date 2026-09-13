from fastapi import FastAPI, Request
from schemas import ClickEvent, PaymentEvent
from contextlib import asynccontextmanager
from database import create_pool
from repository import insert_click, insert_payment

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool()

    try:
        app.state.db_pool = pool
        yield
    finally:
        await pool.close()


app = FastAPI(title = "Event Processor", lifespan=lifespan)


@app.get("/health")
def health_check():
    return {"status":"ok"}


@app.post("/events/clicks", status_code=202)
async def receive_click(event:ClickEvent, request: Request):
    pool = request.app.state.db_pool

    async with pool.acquire(timeout=5) as connection:
        was_inserted = await insert_click(connection, event)
    return {"clid": event.clid, "inserted": was_inserted}

@app.post("/events/payments", status_code=202)
async def receive_payment(event: PaymentEvent, request: Request):
    pool = request.app.state.db_pool

    async with pool.acquire(timeout=5) as connection:
        was_inserted = await insert_payment(connection, event)

    return {"clid": event.clid, "inserted": was_inserted}