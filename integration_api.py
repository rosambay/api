"""
API de integração para atualização de jobs por sistemas externos.
Autenticação: OAuth 2.0 Client Credentials Flow.

Fluxo:
  1. POST /token  { client_id: <api_key_id>, client_secret: <secret> }
     → { access_token, token_type, expires_in }
  2. POST /jobs   Authorization: Bearer <token>
     → upsert de job (INSERT ou UPDATE) com notificação SSE
"""

import uuid
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from duckdb import description
import redis.asyncio as Redis
from fastapi import Depends, FastAPI, Form, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

import database
import models
import redis_client
from auth import verify_password
from config import settings

# ─── App ─────────────────────────────────────────────────────────────────────

app = FastAPI(title="Routes Integration API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "PATCH", "GET"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

_INTEGRATION_TOKEN_EXPIRE = 60 * 24  # 24 horas

# ─── Schemas ─────────────────────────────────────────────────────────────────

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = _INTEGRATION_TOKEN_EXPIRE * 60


class JobUpsertRequest(BaseModel):
    job_id: str = Field(..., description="ID do tarefa no sistema do cliente")
    team_id: str = Field(..., description="ID da equipe da tarefa no sistema do cliente")
    desc_team: str = Field(..., description="Descrição da equipe da tarefa no sistema do cliente")
    team_geocode_lat: Optional[str] = Field(None, description="Latitude da equipe, se houver, da tarefa no sistema do cliente")
    team_geocode_long: Optional[str] = Field(None, description="Longitude da equipe, se houver, da tarefa no sistema do cliente")
    team_modified_date: datetime = Field(..., description="Data/hora de última modificação da equipe, se houver, da tarefa no sistema do cliente")
    resource_id: Optional[str] = Field(None, description="ID do recurso, se houver, da tarefa no sistema do cliente")
    resource_name: Optional[str] = Field(None, description="Nome do recurso, se houver, da tarefa no sistema do cliente")
    resource_actual_geocode_lat: Optional[str] = Field(None, description="Latitude atual do recurso, se houver, da tarefa no sistema do cliente")
    resource_actual_geocode_long: Optional[str] = Field(None, description="Longitude atual do recurso, se houver, da tarefa no sistema do cliente")
    resource_geocode_lat_from: Optional[str] = Field(None, description="Latitude de origem, se houver, da tarefa no sistema do cliente")
    resource_geocode_long_from: Optional[str] = Field(None, description="Longitude de origem, se houver, da tarefa no sistema do cliente")
    resource_geocode_lat_at: Optional[str] = Field(None, description="Latitude de destino, se houver, da tarefa no sistema do cliente")
    resource_geocode_long_at: Optional[str] = Field(None, description="Longitude de destino, se houver, da tarefa no sistema do cliente")
    resource_work_status_flag: Optional[int] = Field(None, description="Flag de status de trabalho (work_status_flag) da tarefa no sistema do cliente")
    resource_modified_data: datetime = Field(..., description="Data/hora de última modificação do recurso, se houver, da tarefa no sistema do cliente")
    job_status_id: str = Field(..., description="ID da situação (status) da tarefa no sistema do cliente")
    desc_job_status: str = Field(..., description="Descrição da situação (status) da tarefa no sistema do cliente")
    style_id: Optional[str] = Field(None, description="ID do estilo (style) da tarefa no sistema do cliente")
    status_modified_date: datetime = Field(..., description="Data/hora de última modificação da situação (status) da tarefa no sistema do cliente")
    job_type_id: str = Field(..., description="ID do tipo de tarefa no sistema do cliente")
    desc_job_type: str = Field(..., description="Descrição do tipo de tarefa no sistema do cliente")
    type_modified_date: datetime = Field(..., description="Data/hora de última modificação do tipo da tarefa no sistema do cliente")    
    place_id: str = Field(..., description="ID do local da tarefa no sistema do cliente")
    trade_name: str = Field(..., description="Nome comercial do local da tarefa, se houver, no sistema do cliente")
    cnpj: str = Field(..., description="CNPJ do local da tarefa, se houver, no sistema do cliente")
    place_modified_date: datetime = Field(..., description="Data/hora de última modificação do local da tarefa no sistema do cliente")
    address_id: str = Field(..., description="ID do endereço da tarefa no sistema do cliente")
    address: str = Field(..., description="Endereço completo da tarefa no sistema do cliente")  
    city: str = Field(..., description="Cidade do local da tarefa, se houver, no sistema do cliente")
    state: str = Field(..., description="Estado do local da tarefa, se houver, no sistema do cliente")
    zip_code: str = Field(..., description="CEP do local da tarefa, se houver, no sistema do cliente")
    geocode_lat: Optional[str] = Field(None, description="Latitude do local da tarefa, se houver, no sistema do cliente")
    geocode_long: Optional[str] = Field(None, description="Longitude do local da tarefa, se houver, no sistema do cliente")
    address_modified_date: datetime = Field(..., description="Data/hora de última modificação do endereço da tarefa no sistema do cliente")
    plan_start_date: datetime = Field(..., description="Data/hora planejada de início da tarefa no sistema do cliente")
    plan_end_date: datetime = Field(..., description="Data/hora planejada de término da tarefa no sistema do cliente")
    actual_start_date: Optional[datetime] = Field(None, description="Data/hora real de início da tarefa, se houver, no sistema do cliente")
    actual_end_date: Optional[datetime] = Field(None, description="Data/hora real de término da tarefa, se houver, no sistema do cliente")
    real_time_service: Optional[int] = Field(None, description="Tempo de serviço em segundos da tarefa, se houver, no sistema do cliente")
    plan_time_service: Optional[int] = Field(None, description="Tempo de serviço planejado em segundos da tarefa, se houver, no sistema do cliente")    
    limit_start_date: Optional[datetime] = Field(None, description="Data/hora limite de início da tarefa, se houver, no sistema do cliente")
    limit_end_date: Optional[datetime] = Field(None, description="Data/hora limite de término da tarefa, se houver, no sistema do cliente")
    priority: Optional[int] = Field(None, description="Prioridade da tarefa, se houver, no sistema do cliente")
    time_setup: Optional[int] = Field(None, description="Tempo de configuração em segundos da tarefa, se houver, no sistema do cliente")
    time_overlap: Optional[int] = Field(None, description="Tempo de sobreposição em segundos da tarefa, se houver, no sistema do cliente")
    distance: Optional[int] = Field(None, description="Distância em metros da tarefa, se houver, no sistema do cliente")
    time_distance: Optional[int] = Field(None, description="Tempo de deslocamento em segundos da tarefa, se houver, no sistema do cliente")
    complements: Optional[Any] = None


class JobUpsertResponse(BaseModel):
    job_id: int
    client_job_id: str
    action: str  # "INSERT" | "UPDATE"

    class Config:
        from_attributes = True


class JobUpsertError(BaseModel):
    client_job_id: str
    error: str


class JobBatchResponse(BaseModel):
    processed: int
    inserted: int
    updated: int
    results: list[JobUpsertResponse]
    errors: list[JobUpsertError]


# ─── Auth helpers ─────────────────────────────────────────────────────────────

def _create_integration_token(client_id: int, api_key_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=_INTEGRATION_TOKEN_EXPIRE)
    payload = {"sub": api_key_id, "clientId": client_id, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


async def get_integration_client(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(database.get_db),
) -> dict:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        client_id: int = payload.get("clientId")
        api_key_id: str = payload.get("sub")
        if not client_id or not api_key_id:
            raise exc
    except JWTError:
        raise exc

    result = await db.execute(
        select(models.ClientApiCredentials).where(
            models.ClientApiCredentials.client_id == client_id,
            models.ClientApiCredentials.api_key_id == api_key_id,
            models.ClientApiCredentials.active == 1,
        )
    )
    cred = result.scalar_one_or_none()
    if not cred:
        raise exc

    return {"clientId": client_id, "apiKeyId": api_key_id}


# ─── Notificação SSE ──────────────────────────────────────────────────────────

async def _notify_sessions(r: Redis, client_id: int, team_id: int, action: str, job_data: dict):
    payload_json = json.dumps({"type": "job", "action": action, "dados": job_data})
    async for chave in r.scan_iter("session:*"):
        session = chave.split(":")[1]
        result = await r.get(chave)
        if not result:
            continue
        for entry in json.loads(result):
            if entry.get("client_id") == client_id and entry.get("team_id") == team_id:
                queue_key = f"notify:{session}:queue"
                await r.rpush(queue_key, payload_json)
                await r.expire(queue_key, 3600)
                await r.publish(f"notify:{session}:notify", "check_queue")
                break


# ─── Resolução de IDs de cliente → internos ───────────────────────────────────

async def _resolve_job_status_id(db: AsyncSession, client_id: int, client_job_status_id: str) -> int:
    result = await db.execute(
        select(models.JobStatus.job_status_id).where(
            models.JobStatus.client_id == client_id,
            models.JobStatus.client_job_status_id == client_job_status_id,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=422, detail=f"client_job_status_id '{client_job_status_id}' não encontrado")
    return row


async def _resolve_job_type_id(db: AsyncSession, client_id: int, client_job_type_id: str) -> int:
    result = await db.execute(
        select(models.JobType.job_type_id).where(
            models.JobType.client_id == client_id,
            models.JobType.client_job_type_id == client_job_type_id,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=422, detail=f"client_job_type_id '{client_job_type_id}' não encontrado")
    return row


# ─── Serialização segura do job para o payload SSE ───────────────────────────

def _job_to_dict(job: models.Jobs) -> dict:
    def _fmt(v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    return {c.name: _fmt(getattr(job, c.name)) for c in job.__table__.columns}


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/token", response_model=TokenResponse)
async def token(
    client_id: str = Form(...),
    client_secret: str = Form(...),
    grant_type: str = Form(default="client_credentials"),
    db: AsyncSession = Depends(database.get_db),
):
    if grant_type != "client_credentials":
        raise HTTPException(status_code=400, detail="grant_type deve ser 'client_credentials'")

    result = await db.execute(
        select(models.ClientApiCredentials).where(
            models.ClientApiCredentials.api_key_id == client_id,
            models.ClientApiCredentials.active == 1,
        )
    )
    cred = result.scalar_one_or_none()
    if not cred or not verify_password(client_secret, cred.secret_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="client_id ou client_secret inválidos",
        )

    token_str = _create_integration_token(cred.client_id, cred.api_key_id)
    logger.info(f"[integration] Token emitido para api_key_id={client_id} client_id={cred.client_id}")
    return TokenResponse(access_token=token_str)


@app.post("/jobs", response_model=JobBatchResponse, status_code=200)
async def upsert_jobs(
    body: list[JobUpsertRequest],
    db: AsyncSession = Depends(database.get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return JobBatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]
    api_tag   = f"API:{current['apiKeyId'][:8]}"
    now       = datetime.now()
    _REQUIRED = {"team_id", "job_status_id", "job_type_id", "address_id", "place_id", "plan_start_date", "plan_end_date"}
    _EXCLUDE  = {"client_job_status_id", "client_job_type_id"}

    # ── Resolve client_job_status_id em lote ─────────────────────────────────
    raw_status_ids = {item.client_job_status_id for item in body if item.client_job_status_id and item.job_status_id is None}
    status_map: dict[str, int] = {}
    if raw_status_ids:
        rows = await db.execute(
            select(models.JobStatus.client_job_status_id, models.JobStatus.job_status_id).where(
                models.JobStatus.client_id == client_id,
                models.JobStatus.client_job_status_id.in_(raw_status_ids),
            )
        )
        status_map = {r[0]: r[1] for r in rows.all()}

    # ── Resolve client_job_type_id em lote ───────────────────────────────────
    raw_type_ids = {item.client_job_type_id for item in body if item.client_job_type_id and item.job_type_id is None}
    type_map: dict[str, int] = {}
    if raw_type_ids:
        rows = await db.execute(
            select(models.JobType.client_job_type_id, models.JobType.job_type_id).where(
                models.JobType.client_id == client_id,
                models.JobType.client_job_type_id.in_(raw_type_ids),
            )
        )
        type_map = {r[0]: r[1] for r in rows.all()}

    # ── Busca todos os jobs existentes em uma única query ────────────────────
    all_client_job_ids = [item.client_job_id for item in body]
    existing_result = await db.execute(
        select(models.Jobs).where(
            models.Jobs.client_id == client_id,
            models.Jobs.client_job_id.in_(all_client_job_ids),
        )
    )
    existing_map: dict[str, models.Jobs] = {j.client_job_id: j for j in existing_result.scalars().all()}

    # ── Processa cada item ────────────────────────────────────────────────────
    results: list[JobUpsertResponse] = []
    errors:  list[JobUpsertError]    = []
    # team_id → set de actions ocorridas (para SSE deduplicado)
    team_actions: dict[int, set[str]] = {}
    # team_id → lista de jobs atualizados (apenas UPDATEs, para notificação individual)
    team_updates: dict[int, list[models.Jobs]] = {}

    for item in body:
        try:
            # Resolve IDs via mapa pré-carregado
            if item.job_status_id is None and item.client_job_status_id:
                sid = status_map.get(item.client_job_status_id)
                if sid is None:
                    raise ValueError(f"client_job_status_id '{item.client_job_status_id}' não encontrado")
                item = item.model_copy(update={"job_status_id": sid})

            if item.job_type_id is None and item.client_job_type_id:
                tid = type_map.get(item.client_job_type_id)
                if tid is None:
                    raise ValueError(f"client_job_type_id '{item.client_job_type_id}' não encontrado")
                item = item.model_copy(update={"job_type_id": tid})

            job = existing_map.get(item.client_job_id)

            if job:
                updates = item.model_dump(exclude_unset=True, exclude=_EXCLUDE)
                for field, value in updates.items():
                    setattr(job, field, value)
                job.modified_by   = api_tag
                job.modified_date = now
                action = "UPDATE"
                team_updates.setdefault(job.team_id, []).append(job)
            else:
                provided = set(item.model_dump(exclude_unset=True).keys())
                missing  = _REQUIRED - provided
                if missing:
                    raise ValueError(f"Campos obrigatórios ausentes: {', '.join(sorted(missing))}")
                fields = item.model_dump(exclude_none=True, exclude=_EXCLUDE)
                job = models.Jobs(
                    **fields,
                    client_id    = client_id,
                    uid          = uuid.uuid4(),
                    created_by   = api_tag,
                    created_date = now,
                    modified_by  = api_tag,
                    modified_date= now,
                )
                db.add(job)
                existing_map[item.client_job_id] = job  # evita duplicata dentro do mesmo lote
                action = "INSERT"

            team_actions.setdefault(job.team_id, set()).add(action)
            results.append(JobUpsertResponse(job_id=job.job_id or 0, client_job_id=item.client_job_id, action=action))

        except Exception as exc:
            errors.append(JobUpsertError(client_job_id=item.client_job_id, error=str(exc)))

    await db.commit()

    # Atualiza job_id nos INSERTs (gerado pelo banco após commit)
    for res in results:
        if res.action == "INSERT":
            refreshed = existing_map.get(res.client_job_id)
            if refreshed:
                await db.refresh(refreshed)
                res.job_id = refreshed.job_id

    # ── Notificações SSE deduplicadas por equipe ──────────────────────────────
    # Se houver qualquer INSERT na equipe → manda um INSERT (frontend recarrega tudo).
    # Se só UPDATEs → manda um UPDATE por job alterado.
    try:
        for team_id, actions in team_actions.items():
            if "INSERT" in actions:
                await _notify_sessions(r, client_id, team_id, "INSERT", {})
            else:
                for job in team_updates.get(team_id, []):
                    await _notify_sessions(r, client_id, team_id, "UPDATE", _job_to_dict(job))
    except Exception as e:
        logger.warning(f"[integration] Falha ao notificar SSE: {e}")

    inserted = sum(1 for res in results if res.action == "INSERT")
    updated  = sum(1 for res in results if res.action == "UPDATE")
    logger.info(f"[integration] batch client_id={client_id} inserted={inserted} updated={updated} errors={len(errors)}")
    return JobBatchResponse(processed=len(results), inserted=inserted, updated=updated, results=results, errors=errors)


@app.patch("/jobs/{client_job_id}", response_model=JobUpsertResponse)
async def update_job(
    client_job_id: str,
    body: JobUpsertRequest,
    db: AsyncSession = Depends(database.get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    client_id = current["clientId"]

    if body.client_job_status_id and body.job_status_id is None:
        body = body.model_copy(update={"job_status_id": await _resolve_job_status_id(db, client_id, body.client_job_status_id)})
    if body.client_job_type_id and body.job_type_id is None:
        body = body.model_copy(update={"job_type_id": await _resolve_job_type_id(db, client_id, body.client_job_type_id)})

    result = await db.execute(
        select(models.Jobs).where(
            models.Jobs.client_id == client_id,
            models.Jobs.client_job_id == client_job_id,
        )
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    updates = body.model_dump(exclude_unset=True, exclude={"client_job_id", "client_job_status_id", "client_job_type_id"})
    for field, value in updates.items():
        setattr(job, field, value)
    job.modified_by   = f"API:{current['apiKeyId'][:8]}"
    job.modified_date = datetime.now()

    await db.commit()
    await db.refresh(job)

    try:
        await _notify_sessions(r, client_id, job.team_id, "UPDATE", _job_to_dict(job))
    except Exception as e:
        logger.warning(f"[integration] Falha ao notificar SSE: {e}")

    logger.info(f"[integration] job UPDATE client_id={client_id} job_id={job.job_id}")
    return JobUpsertResponse(job_id=job.job_id, client_job_id=job.client_job_id, action="UPDATE")
