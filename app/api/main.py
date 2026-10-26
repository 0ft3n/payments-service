from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.api.routes import router as payments_router
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.log import setup_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.settings = settings
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    app = FastAPI(title="Payments Service", version="1.0.0", lifespan=lifespan)
    app.include_router(payments_router)

    @app.get("/health", tags=["service"])
    async def health() -> dict[str, str]:
        async with app.state.session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
