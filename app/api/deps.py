import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings, require_secret
from app.db import SessionFactory

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    expected = require_secret(get_settings().api_key, "API_KEY")
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
