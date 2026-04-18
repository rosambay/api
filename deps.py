from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from loguru import logger
from config import settings

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        dataToken = {
            "clientId":     payload.get("clientId"),
            "userId":       payload.get("userId"),
            "userName":     payload.get("userName"),
            "superUser":    payload.get("superUser"),
            "clientUid":    payload.get("clientUid"),
            "clientDomain": payload.get("clientDomain"),
        }
        logger.info(f"Token decodificado: {dataToken}")
        if dataToken["clientId"] is None or dataToken["userId"] is None or dataToken["superUser"] is None:
            raise credentials_exception
    except jwt.ExpiredSignatureError:
        raise credentials_exception
    except JWTError:
        raise credentials_exception
    return dataToken
