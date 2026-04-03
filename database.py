# database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import os

# Pega a URL do docker-compose. Se rodar fora do Docker, usa um fallback local.
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://admin:123456@localhost:5433/routes" # Fallback para rodar na sua máquina via DBeaver
)

# Cria a engine de conexão assíncrona
engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,   # valida conexão antes de usar — evita reconnect silencioso
    pool_recycle=1800,    # recicla conexões ociosas a cada 30min
    pool_size=10,         # conexões persistentes no pool
    max_overflow=20,      # conexões extras em pico
)

# Fabrica as sessões do banco de dados
SessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# Dependência que injetaremos no main.py
async def get_db():
    async with SessionLocal() as session:
        yield session