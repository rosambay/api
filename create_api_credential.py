"""
Cria um registro em client_api_credentials para o client_id informado.

Uso:
    python create_api_credential.py
    python create_api_credential.py --client-id 2 --description "Sistema X"

Saída: api_key_id e client_secret em texto (guarde o secret — não é recuperável).
"""

import asyncio
import uuid
import sys
import argparse
from datetime import datetime

# Adiciona o diretório da API no path para importar os módulos do projeto
sys.path.insert(0, ".")

from sqlalchemy.ext.asyncio import AsyncSession
from database import SessionLocal
import models
from auth import get_password_hash


async def create_credential(client_id: int, description: str | None) -> None:
    api_key_id     = str(uuid.uuid4())
    client_secret  = str(uuid.uuid4())          # segredo gerado aleatoriamente
    secret_hash    = get_password_hash(client_secret)
    now            = datetime.now()

    cred = models.ClientApiCredentials(
        client_id    = client_id,
        api_key_id   = api_key_id,
        secret_hash  = secret_hash,
        description  = description,
        active       = 1,
        created_by   = "script",
        created_date = now,
        modified_by  = "script",
        modified_date= now,
    )

    async with SessionLocal() as db:
        db.add(cred)
        await db.commit()

    print("\n" + "="*60)
    print("  Credencial criada com sucesso!")
    print("="*60)
    print(f"  client_id     : {client_id}")
    print(f"  api_key_id    : {api_key_id}")
    print(f"  client_secret : {client_secret}")
    if description:
        print(f"  description   : {description}")
    print("="*60)
    print("  ATENÇÃO: guarde o client_secret agora.")
    print("  Ele não pode ser recuperado depois.\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cria credencial de API para um client_id.")
    parser.add_argument("--client-id",   type=int, default=1,    help="ID do cliente (padrão: 1)")
    parser.add_argument("--description", type=str, default=None, help="Descrição opcional da credencial")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(create_credential(args.client_id, args.description))
