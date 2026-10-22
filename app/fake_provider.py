import os
from fastapi import FastAPI
app = FastAPI(title=f"Fake provider {os.getenv('PROVIDER_NAME', 'unknown')}")
@app.get("/health")
async def health():
    return {"status": "ok"}
