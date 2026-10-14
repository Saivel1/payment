from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.config import get_settings, require_secret
from app.db import engine


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    require_secret(get_settings().api_key, "API_KEY")  # fail fast instead of 401-ing every request
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    docs_enabled = get_settings().docs_enabled
    app = FastAPI(
        title="Payments Service",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )
    app.include_router(payments_router)
    return app


app = create_app()
