import bcrypt
import hashlib
from datetime import datetime, timedelta, timezone
from jose import jwt
from loguru import logger
from config import settings

# Não usamos mais passlib. Usamos bcrypt puro.

def get_password_hash(password: str) -> str:
    """
    Gera o hash da senha usando Bcrypt puro.
    1. Converte para SHA256 (fixo 64 chars) para evitar erro de limite de 72 bytes.
    2. Gera o Salt e Hash via Bcrypt.
    3. Retorna String (para salvar no banco).
    """
    if not password:
        raise ValueError("Password cannot be empty")

    # Passo 1: Normalização de tamanho com SHA256
    # O bcrypt falha se o input tiver > 72 bytes. O SHA256 sempre gera 64 bytes (hex).
    pwd_bytes = password.encode('utf-8')
    pwd_safe = hashlib.sha256(pwd_bytes).hexdigest().encode('utf-8')

    # Passo 2: Gerar Salt e Hash
    # bcrypt.hashpw retorna bytes (ex: b'$2b$12$...')
    salt = bcrypt.gensalt()
    hashed_bytes = bcrypt.hashpw(pwd_safe, salt)

    # Passo 3: Decodificar para String para salvar no Postgres (TEXT)
    return hashed_bytes.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica a senha comparando com o hash do banco.
    """
    if not plain_password or not hashed_password:
        return False

    try:
        # Passo 1: Reproduzir a normalização SHA256
        pwd_bytes = plain_password.encode('utf-8')
        pwd_safe = hashlib.sha256(pwd_bytes).hexdigest().encode('utf-8')

        # Passo 2: Garantir que o hash do banco esteja em bytes
        hashed_bytes = hashed_password.encode('utf-8')

        # Passo 3: Verificar
        return bcrypt.checkpw(pwd_safe, hashed_bytes)
    except Exception as e:
        logger.warning(f"Erro ao verificar senha (hash inválido ou corrompido): {e}")
        return False

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)