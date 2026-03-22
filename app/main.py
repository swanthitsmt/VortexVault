from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import init_db
from app.routers import api, web

app = FastAPI(title=settings.app_name, default_response_class=ORJSONResponse)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

app.include_router(api.router)
app.include_router(web.router)


@app.on_event("startup")
async def startup_event() -> None:
    await init_db()
