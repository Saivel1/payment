from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

engine = create_async_engine(get_settings().database_url.get_secret_value(), pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
