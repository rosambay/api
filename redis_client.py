# redis_client.py
import redis.asyncio as redis
import os

# Em produção, use variáveis de ambiente (Ex: 'redis://localhost:6379')
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/2")

# REDIS_URL = os.getenv("REDIS_URL", "redis://meu-redis:6379")

# Cria o pool de conexão (otimizado para async)
redis_pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)

def get_redis():
    return redis.Redis(connection_pool=redis_pool)