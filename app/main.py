from fastapi import FastAPI

from app.balances import router as balances_router
from app.generation import router as generations_router
from app.payments import router as payments_router
from app.provider_flow import router as provider_webhooks_router

app = FastAPI(title="Integer Generation Service", version="0.4.0")
app.include_router(payments_router)
app.include_router(balances_router)
app.include_router(generations_router)
app.include_router(provider_webhooks_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
