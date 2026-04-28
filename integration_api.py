import os
import traceback
import asyncio
import sys
import json
import polars as pl
import duckdb
from duckdb_client import get_duckdb
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
import redis.asyncio as Redis
from fastapi import Depends, FastAPI, Form, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from datetime import datetime, timedelta, date
from database import get_db, SessionLocal, engine, Base
import models
import redis_client
from auth import verify_password
from config import settings
from contextlib import asynccontextmanager
import integration_schemas as schemas
from services import  get_route_distance_block, logs , geocode_mapbox

SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://admin:123456@localhost:5433/routes" # Fallback para rodar na sua máquina via DBeaver
)

# ─── App ─────────────────────────────────────────────────────────────────────

def serializador_customizado(obj):
    # Se for data ou data/hora, usa o formato ISO
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Se for outro tipo desconhecido, gera erro (comportamento padrão)
    raise TypeError(f"Tipo {type(obj)} não é serializável")

async def nofifyTeamWebSocket(r, data, semafore):
    
    async with semafore:
        async for chave in r.scan_iter("session:*"):
            session = chave.split(':')[1]
            result =  await r.get(chave)
            if result:
                for x in json.loads(result):
                    if x['team_id'] == data.registro_json['team_id']:
                        logger.debug(f"Enviando atualização para sessão {session} do WebSocket - Equipe: {data.registro_json['team_id']}")
                        chave_queue = f"notify:{session}:queue"
                        payload = {
                            "type": 'JOB',
                            "action": data.merge_action,
                            "dados": data.registro_json or {}
                        }
                        payload_json = json.dumps(payload)
                        await r.rpush(chave_queue, payload_json)
                        await r.expire(chave_queue, 3600)
                        await r.publish(f"notify:{session}:notify", "check_queue")

async def nofifyResourceWebSocket(r, data, semafore):
    
    async with semafore:
        async for chave in r.scan_iter("session:*"):
            session = chave.split(':')[1]
            result =  await r.get(chave)
            if result:
                for x in json.loads(result):
                    for res in x['resources']:
                        # print(res)
                        if res['resource_id'] == data.registro_json['resource_id']:
                            logger.debug(f"Enviando atualização para sessão {session} do WebSocket - Resource: {data.registro_json['resource_id']}")
                            chave_queue = f"notify:{session}:queue"
                            payload = {
                                "type": 'RESOURCE',
                                "action": data.merge_action,
                                "dados": data.registro_json or {}
                            }
                            payload_json = json.dumps(payload)
                            await r.rpush(chave_queue, payload_json)
                            await r.expire(chave_queue, 3600)
                            await r.publish(f"notify:{session}:notify", "check_queue")

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        
        # await conn.run_sync(Base.metadata.drop_all)
        # logger.info("Tabelas do banco dropadas com sucesso!")
        # await r.flushall()
        # logger.info("Cache Redis limpo com sucesso!")

        await conn.run_sync(Base.metadata.create_all)
        logger.info("Tabelas do banco verificadas/criadas com sucesso!")

    # r = redis_client.get_redis()
    # await r.flushall()
    
    yield


app = FastAPI(title="Routes Integration API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "PATCH", "GET"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

_INTEGRATION_TOKEN_EXPIRE = 60 * 24  # 24 horas

# ─── Auth helpers ─────────────────────────────────────────────────────────────

def _create_integration_token(client_id: int, api_key_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=_INTEGRATION_TOKEN_EXPIRE)
    payload = {"sub": api_key_id, "clientId": client_id, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


async def get_integration_client(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
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

async def calcDistance(client_id):
    try:
        logger.warning('Iniciando cálculo de distâncias...')
    #vamos verificar se existe alguma tarefa concluida sem distância calculada
        stmt = text("""
        WITH q1 AS 
        (
            SELECT 
                j.client_id,
                j.team_id,
                j.resource_id,
                j.actual_start_date,
                j.plan_start_date,
                DATE_TRUNC('day',COALESCE(j.actual_start_date, j.plan_start_date)) AS fix_start_date,
                CASE 
                    WHEN j.actual_start_date IS NOT NULL AND j.actual_end_date IS NOT NULL
                        THEN
                            CAST(EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date)) AS INTEGER)
                    ELSE
                            CAST(EXTRACT(EPOCH FROM (j.plan_end_date - j.plan_start_date)) AS INTEGER)
                END AS duration,
                a.geocode_long,
                a.geocode_lat,
                -- Verifica se o ponto de chegada do recurso é nulo, se for, não tem ponto de chegada
                CASE 
                    WHEN COALESCE(r.geocode_lat_end, t.geocode_lat) IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_lat) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                        COALESCE(r.geocode_lat_end, t.geocode_lat)
                END AS geocode_lat_end,
                CASE 
                    WHEN COALESCE(r.geocode_long_end, t.geocode_long) IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_long) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                        COALESCE(r.geocode_long_end, t.geocode_long)
                END AS geocode_long_end,
                -- Verifica se o ponto de chegada do recurso é nulo, se for, não tem ponto de partida
                CASE 
                    WHEN COALESCE(r.geocode_lat_start, t.geocode_lat) IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_lat) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                        COALESCE(r.geocode_lat_start, t.geocode_lat)
                END AS geocode_lat_start,
                CASE 
                    WHEN COALESCE(r.geocode_long_start, t.geocode_long) IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_long) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                        COALESCE(r.geocode_long_start, t.geocode_long)
                END AS geocode_long_start,
                --verifica se existe algum distance nulo no periodo, então processa
                bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_distance,
                --verifica se existe algum geocode está vazio e não processa
                bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_geo
            FROM jobs j
                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
            WHERE js.internal_code_status = 'CONCLU'
              AND j.client_id = :client_id
        )
        SELECT
            q1.client_id,
            q1.team_id,
            q1.resource_id,
            q1.fix_start_date,
            (
            jsonb_build_array(jsonb_build_array(MAX(q1.geocode_long_start)::NUMERIC, MAX(q1.geocode_lat_start)::NUMERIC))
            ||
            jsonb_agg(jsonb_build_array(q1.geocode_long::NUMERIC, q1.geocode_lat::NUMERIC) ORDER BY q1.actual_start_date nulls last, q1.plan_start_date) 
            || 
            jsonb_build_array(jsonb_build_array(MAX(q1.geocode_long_end)::NUMERIC, MAX(q1.geocode_lat_end)::NUMERIC))
            ) AS rota_completa
         FROM q1
        WHERE null_distance = true
          AND null_geo = false
        GROUP BY q1.client_id, q1.team_id, q1.resource_id, q1.fix_start_date
        ORDER BY q1.client_id, q1.team_id, q1.resource_id, q1.fix_start_date;
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt, {"client_id": client_id})
            rows = result.mappings().all()

        if not rows:
            return

        logger.warning(f'Calculando distâncias para {len(rows)} rotas em paralelo...')

        geo_results = await asyncio.gather(
            *[get_route_distance_block(row.rota_completa) for row in rows]
        )

        merge_stmt = text("""
            MERGE INTO jobs AS u
                USING (
                    WITH qjson AS
                    (
                        SELECT
                            y.*,
                            FIRST_VALUE(distance) OVER (ORDER BY linha ASC) AS first_distance,
                            FIRST_VALUE(distance) OVER (ORDER BY linha DESC) AS last_distance,
                            FIRST_VALUE(duration) OVER (ORDER BY linha ASC) AS first_duration,
                            FIRST_VALUE(duration) OVER (ORDER BY linha DESC) AS last_duration
                         FROM (
                            SELECT
                                ROW_NUMBER() over() linha,
                                x.*
                              FROM
                                jsonb_to_recordset(:dados_json)
                                AS x(
                                    duration NUMERIC,
                                    distance NUMERIC)
                            ) y order by linha ASC
                    ),
                    q1 as
                    (
                        select
                            ROW_NUMBER() over() linha,
                            j.*
                        from (
                            select j.client_id, j.job_id, a.geocode_lat, a.geocode_long, j.actual_start_date,
                                bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_distance,
                                bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_geo
                             FROM jobs j
                                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                                JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
                            WHERE COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
                            AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'
                            AND js.internal_code_status = 'CONCLU'
                            AND j.client_id = :client_id
                            AND j.team_id = :team_id
                            AND j.resource_id = :resource_id
                        order by j.actual_start_date nulls last, j.plan_start_date) j
                    ),
                    qjob as (
                        select *
                          from q1
                         where null_distance = true
                           AND null_geo = false
                    )
                    SELECT b.client_id, b.job_id, a.distance, a.duration, a.last_distance, a.last_duration, a.first_distance, a.first_duration
                      FROM qjson a
                      join qjob b on b.linha = a.linha) as t
            ON u.client_id = t.client_id AND u.job_id = t.job_id
            WHEN MATCHED THEN
            UPDATE SET
                distance = t.distance
                ,time_distance = t.duration
                ,first_distance = t.first_distance
                ,first_time_distance = t.first_duration
                ,last_distance = t.last_distance
                ,last_time_distance = t.last_duration
            RETURNING
                  to_jsonb(u) AS registro_json
            """)


        for row, geoResult in zip(rows, geo_results):
            clientId = row.client_id
            logger.info(f'Aplicando distância: clientId={clientId}, teamId={row.team_id}, resourceId={row.resource_id}, date={row.fix_start_date}')
            # asyncio.create_task(logs(clientId=clientId, log='Resultado do distance', logJson=geoResult))
            async with SessionLocal() as db:
                merge_result = await db.execute(merge_stmt, {
                    "client_id": clientId,
                    "team_id": row.team_id,
                    "p_date": row.fix_start_date,
                    "resource_id": row.resource_id,
                    "dados_json": json.dumps(geoResult),
                })
            # merge_rows = merge_result.mappings().all()
            # asyncio.create_task(logs(clientId=clientId, log='Resultado do MERGE distance', logJson=[dict(r) for r in merge_rows]))

                await db.commit()

    except Exception as e:
        logger.error(f"[calcDistance] Erro ao processar postgres: {e}")          

async def getSnapTime(r, client_id):
    clientKey = f'snap_time:{client_id}'
    payload = await r.get(clientKey)
    if not payload:
        tempDT = (datetime.now() - timedelta(days=(365*2)))
        tempJobDT = (datetime.now() - timedelta(days=5))
        dateTime = tempDT.strftime('%Y-%m-%d ') + '00:00:00'
        dateTimeJob = tempJobDT.strftime('%Y-%m-%d ') + '00:00:00'
        payload = json.dumps({
            "styles" : dateTime,
            "address" : dateTime,
            "teams" : dateTime,
            "team_members": dateTime,
            "resources" : dateTime,
            "resource_windows" : dateTime,
            "places" : dateTime,
            "actual_geopos" : dateTime,
            "logintime" : dateTime,
            "jobs": dateTimeJob,
            "job_types" : dateTime,
            "job_status" : dateTime,
            "priority" : dateTime,
        })
        
        await r.set(clientKey,payload)
    return payload

async def setSnapTime(r, client_id, snap, snapTime):
    clientKey = f'snap_time:{client_id}'
    payload = await r.get(clientKey)
    if not payload:
        payload = await getSnapTime(r,client_id)

    payload_dict = json.loads(payload)
    payload_dict[f'{snap}'] = snapTime
    await r.set(clientKey,json.dumps(payload_dict))
    return

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/token", response_model=schemas.TokenResponse)
async def token(
    client_id: str = Form(...),
    client_secret: str = Form(...),
    grant_type: str = Form(default="client_credentials"),
    db: AsyncSession = Depends(get_db),
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
    return schemas.TokenResponse(access_token=token_str)

@app.get("/snaps", status_code=200)
async def get_snaps(
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    client_id = current["clientId"]
    payload = await getSnapTime(r,client_id)
    print(type(payload))
    return payload

@app.get("/queries", status_code=200)
async def get_queries(
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    payload = {'jobs':"""
        SELECT TOP 2000
                CONCAT(t.request_id,'|',t.task_id) AS client_job_id,
                t.team_id client_team_id,
                t.desc_team,
                CONVERT(VARCHAR(19), CAST(t.team_modified_dttm AS DATETIME2), 120) team_modified_date,
                t.person_id AS client_resource_id,
                concat(p.first_name,COALESCE(p.middle_name,' '),p.last_name) AS resource_name,
                p.status client_resource_status_id,
                p.desc_status resource_desc_status,  
                CONVERT(VARCHAR(19), CAST(p.modified_dttm AS DATETIME2), 120) AS resource_modified_date,                                         
                t.task_status AS client_status_id, 
                case when mmd.message_text is null then t.desc_task_status else mmd.message_text end desc_status,
                t.item_style_id client_style_id, 
                CONVERT(VARCHAR(19), CAST(t.task_status_modified_dttm AS DATETIME2), 120) status_modified_date,
                t.task_type AS client_type_id,
                t.desc_task_type AS desc_type,
                CONVERT(VARCHAR(19), CAST(t.task_type_modified_dttm AS DATETIME2), 120) type_modified_date,
                t.place_id client_place_id,
                t.trade_name,
                t.cnpj,
                CONVERT(VARCHAR(19), CAST(t.place_modified_dttm AS DATETIME2), 120) place_modified_date,
                t.address_id client_address_id,
                t.address,
                t.city,
                t.state_prov state, 
                t.zippost zip_code,
                t.geocode_lat, 
                t.geocode_long,
                CONVERT(VARCHAR(19), CAST(t.address_modified_dttm AS DATETIME2), 120) address_modified_date,
                CONVERT(VARCHAR(19), CAST(t.plan_start_dttm AS DATETIME2), 120) plan_start_date,
                CONVERT(VARCHAR(19), CAST(t.plan_end_dttm AS DATETIME2), 120) plan_end_date,
                CONVERT(VARCHAR(19), CAST(t.actual_start_dttm AS DATETIME2), 120) actual_start_date,
                CONVERT(VARCHAR(19), CAST(t.actual_end_dttm AS DATETIME2), 120) actual_end_date,
                COALESCE(t.work_duration,0) * 60 AS real_time_service,
                COALESCE(t.plan_task_dur_min,0) * 60 AS plan_time_service,
                CONVERT(VARCHAR(19), CAST(t.created_dttm AS DATETIME2), 120) limit_start_date,
                t.sla limit_end_date,
                COALESCE(cp.ranking,0) as priority,
                CONVERT(VARCHAR(19), CAST(t.created_dttm AS DATETIME2), 120) created_date, 
                CONVERT(VARCHAR(19), CAST(t.modified_dttm AS DATETIME2), 120) modified_date

            FROM dbo.c_task_routes_vw t WITH (NOEXPAND)
                LEFT JOIN dbo.c_person_vw p WITH (NOEXPAND) ON t.person_id = p.person_id
                LEFT JOIN metrix_message_def mmd ON t.desc_message_id = mmd.message_id AND locale_code = 'PT-BR' AND mmd.message_type = 'CODE'
                LEFT JOIN dbo.C_PRIORITY_VW cp ON t.contr_type = cp.contr_type
            WHERE t.modified_dttm >= CAST('{dateTime}' AS datetime)
                ORDER BY t.modified_dttm ASC 
    """,
    'styles':"""
        select  item_style_id client_style_id, 
                    COALESCE(background, '#FFFFFF') AS background,
                    COALESCE(foreground, '#000000') AS foreground,
                    CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) AS modified_date
              from METRIX_ITEM_STYLE_VIEW
            where (modified_dttm >= CAST('{dateTime}' AS datetime))
    """,
    'resources':"""
            select 
                p.person_id client_resource_id, 
                concat(p.first_name,COALESCE(p.middle_name,' '),p.last_name) AS resource_name,
                p.status client_status_id,
                p.desc_status,  
                CASE WHEN p.work_status = 'OFF SHIFT' THEN 1 ELSE 0 END as off_shift_flag,                                        
                p.geocode_lat geocode_lat_actual, 
                p.geocode_long geocode_long_actual, 
                pg.geocode_lat_from geocode_lat_start,
                pg.geocode_long_from geocode_long_start, 
                pg.geocode_lat_at geocode_lat_end, 
                pg.geocode_long_at geocode_long_end,
                CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) as modified_date
              from C_PERSON_VW p WITH (NOEXPAND)
              LEFT JOIN c_person_geocode_vw pg ON p.person_id = pg.person_id
            where p.modified_dttm >= CAST('{dateTime}' AS datetime)
              and p.person_type = 'TECNICO'
    """,
    'resource_windows':"""
        SELECT  person_id client_resource_id, 
                    work_cal_time_id client_resource_window_id,
                    day_code week_day,
                    description,
                    CONVERT(VARCHAR(8), start_tm, 108) start_time,
                    CONVERT(VARCHAR(8), stop_tm, 108) end_time,
                    CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) modified_date
             FROM dbo.C_WORK_TIME_VW a WITH (NOEXPAND)
            WHERE a.modified_dttm >=  CAST('{dateTime}' AS datetime)
    """,
    'address':"""
            SELECT  address_id client_address_id,
                    address,
                    geocode_lat,
                    geocode_long,
                    city,
                    state_prov state,
                    zippost,
                    CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) as modified_date
                from C_ADDRESS_VW a WITH (NOEXPAND)
                where a.modified_dttm >= CAST('{dateTime}' AS datetime)
    """,
    'places':"""
        SELECT  place_id client_place_id,
                trade_name,
                cnpj,
                CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) as modified_date
        from C_PLACE_VW a WITH (NOEXPAND)
        where a.modified_dttm >= CAST('{dateTime}' AS datetime)
    """,
    'teams':"""
        SELECT 
            t.team_id client_team_id, 
            t.description team_name, 
            a.geocode_lat,
            a.geocode_long,
            CONVERT(VARCHAR(19), CAST(t.modified_dttm AS DATETIME2), 120) as modified_date
            FROM dbo.team t
            join dbo.place p on p.place_id = t.place_id
            left join dbo.place_address pa ON p.place_id = pa.place_id and pa.address_type = 'DEFAULT'
            LEFT join dbo.address a on pa.address_id  = a.address_id
            where t.modified_dttm >= CAST('{dateTime}' AS datetime)
    """,
    'team_members':"""
        SELECT 
            team_id client_team_id, 
            person_id client_resource_id, 
            CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) as modified_date
            FROM dbo.c_team_member_vw WITH (NOEXPAND)
        WHERE modified_dttm >=  CAST('{dateTime}' AS datetime)
            AND person_type = 'TECNICO'
    """,
    'job_types':"""
        SELECT 
            code_value client_job_type_id, 
            description desc_job_type, 
            CONVERT(VARCHAR(19), CAST(modified_dttm AS DATETIME2), 120) as modified_date
            FROM dbo.GLOBAL_CODE_TABLE
            WHERE modified_dttm >= CAST('{dateTime}' AS datetime)
            AND CODE_NAME = 'TASK_TYPE'
            AND ACTIVE = 'Y'
    """,
    'job_status':"""
        SELECT 
              t.task_status client_job_status_id, 
              CASE 
                  WHEN mmd.message_text IS NULL THEN t.description 
                  ELSE mmd.message_text 
              END AS desc_job_status,
              t.item_style_id client_style_id, 
              CONVERT(VARCHAR(19), CAST(t.modified_dttm AS DATETIME2), 120) as modified_date
          FROM dbo.task_status t
          LEFT JOIN metrix_message_def mmd 
              ON t.desc_message_id = mmd.message_id 
              AND mmd.locale_code = 'PT-BR' 
              AND mmd.message_type = 'CODE'
          WHERE t.modified_dttm >= CAST('{dateTime}' AS datetime)
            AND t.active = 'Y'
    """}
    return payload

@app.post("/jobs", response_model=schemas.BatchResponse, status_code=200)
async def upsert_jobs(
    body: list[schemas.JobUpsertRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]
    api_tag   = f"API:{current['apiKeyId'][:8]}"
    now       = datetime.now()

    print("Cliente ...", client_id)

    inconsistencies = []
    contador_insert = 0
    contador_update = 0
    

    df_dados = pl.DataFrame(body, infer_schema_length=None)

    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    temp_fields = ["client_team_id", "desc_team", "team_modified_date"]
    df_temp = df_dados.unique(temp_fields)
    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)
   
    stmt = text(f"""
        MERGE INTO teams AS u
        USING (
            SELECT x.* FROM jsonb_to_recordset(:dados_json)
            AS x(   client_team_id text,
                    desc_team text,
                    team_modified_date TIMESTAMP
            ) ) AS t
        ON u.client_id = :client_id AND u.client_team_id = t.client_team_id
        WHEN NOT MATCHED THEN
            INSERT (client_id,client_team_id, team_name, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_team_id, t.desc_team, 'INTEGRATION', NOW(), 'INTEGRATION', t.team_modified_date)
        RETURNING
            u.team_id, merge_action(), to_jsonb(u) AS registro_json
        """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    # await db.commit()
    for row in result:
        logger.info(f"ID: {row.team_id} | Ação Team realizada: {row.merge_action}")

    
    
    ## status da tarefa
    temp_fields = ["client_status_id", "desc_status", "client_style_id", "status_modified_date"]
    df_temp = df_dados.unique(temp_fields)
    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text(f"""
        MERGE INTO job_status AS u
        USING (SELECT DISTINCT x.*, s.style_id
                FROM jsonb_to_recordset(:dados_json)
                AS x(
                    client_status_id text,
                    desc_status text,
                    client_style_id text,
                    status_modified_date TIMESTAMP)
                LEFT JOIN styles s ON x.client_style_id = s.client_style_id AND s.client_id = :client_id
                ) AS t
        ON u.client_id = :client_id AND u.client_job_status_id = t.client_status_id
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_job_status_id, style_id, description, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_status_id, t.style_id, t.desc_status, 'INTEGRATION', NOW(), 'INTEGRATION', t.status_modified_date)
        RETURNING
            u.job_status_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    for row in result:
        logger.info(f"ID: {row.job_status_id} | Ação JOB STATUS realizada: {row.merge_action}")

    ## tipo da tarefa
    temp_fields = ["client_type_id", "desc_type", "type_modified_date"]
    df_temp = df_dados.unique(temp_fields)
    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text(f"""
        MERGE INTO job_types AS u
        USING (
            SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
            AS x(   client_type_id text,
                    desc_type text,
                    type_modified_date TIMESTAMP
            ) ) AS t
        ON u.client_id = :client_id AND u.client_job_type_id = t.client_type_id
        WHEN NOT MATCHED THEN
            INSERT (client_id,client_job_type_id, description, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_type_id, t.desc_type, 'INTEGRATION', NOW(), 'INTEGRATION', t.type_modified_date)
        RETURNING
            u.job_type_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    for row in result:
        logger.info(f"ID: {row.job_type_id} | Ação JOB TYPE realizada: {row.merge_action}")

    ## Places
    temp_fields = ["client_place_id", "trade_name", "cnpj", "place_modified_date"]
    df_temp = df_dados.unique(temp_fields)
    last_snap = df_temp["place_modified_date"].max()
    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text(f"""
        MERGE INTO places AS u
        USING (
            SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
            AS x(   client_place_id text,
                    trade_name text,
                    cnpj text,
                    place_modified_date TIMESTAMP
            ) ) AS t
        ON u.client_id = :client_id AND u.client_place_id = t.client_place_id
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_place_id, trade_name, cnpj, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_place_id, t.trade_name, t.cnpj, 'INTEGRATION', NOW(), 'INTEGRATION', t.place_modified_date)
        RETURNING
            u.place_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    for row in result:
        logger.info(f"ID: {row.place_id} | Ação PLACE realizada: {row.merge_action}")

    ## Address
    temp_fields = ["client_address_id", "address", "city", "state", "zip_code", "geocode_lat", "geocode_long", "address_modified_date"]
    df_temp = df_dados.unique(temp_fields)
    last_snap = df_temp["address_modified_date"].max()


    coords_validas = (
        pl.col("geocode_lat").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat").is_not_null() & pl.col("geocode_long").is_not_null()

    for row in df_temp.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_address_id"],
            "message": f"Coordenadas de chegada do recurso estão fora do intervalo válido - {row['client_address_id']}, Latitude:{row['geocode_lat']}, Longitude {row['geocode_long']}"
        })

    df_temp = df_temp.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat")).otherwise(None).alias("geocode_lat"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long")).otherwise(None).alias("geocode_long"),
    ])

    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text(f"""
        MERGE INTO address AS u
        USING (SELECT DISTINCT x.*
                FROM jsonb_to_recordset(:dados_json)
                AS x(
                    client_address_id text,
                    address text,
                    geocode_lat text,
                    geocode_long text,
                    city text,
                    state text,
                    zip_post text,
                    address_modified_date TIMESTAMP)
            ) AS t
        ON u.client_id = :client_id AND u.client_address_id = t.client_address_id
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_address_id, address, geocode_lat, geocode_long, city , state_prov, zippost, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_address_id, t.address, t.geocode_lat, t.geocode_long, t.city, t.state, t.zip_post, 'INTEGRATION', NOW(), 'INTEGRATION', t.address_modified_date)
        RETURNING
            u.address_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    # for row in result:
    #     logger.info(f"ID: {row.address_id} | Ação ADDRESS realizada: {row.merge_action}")

    ## Jobs
    fields = list(schemas.JobUpsertRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()
    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text(f"""
        MERGE INTO jobs AS u
        USING (SELECT x.*,
                t.team_id AS team_id,
                r.resource_id,
                a.address_id,
                p.place_id,
                jt.job_type_id,
                js.job_status_id,
                COALESCE(py.priority,0) AS priority_val
                FROM jsonb_to_recordset(:dados_json)
                AS x( client_job_id text,
                        priority text,
                        client_team_id text,
                        client_resource_id text,
                        client_address_id text,
                        client_place_id text,
                        client_type_id text,
                        client_status_id text,
                        plan_start_date TIMESTAMP,
                        plan_end_date TIMESTAMP,
                        actual_start_date TIMESTAMP,
                        actual_end_date TIMESTAMP,
                        real_time_service NUMERIC,
                        plan_time_service integer,
                        limit_start_date TIMESTAMP,
                        limit_end_date TIMESTAMP,
                        created_date TIMESTAMP,
                        modified_date TIMESTAMP)
                LEFT JOIN resources r ON x.client_resource_id = r.client_resource_id AND :client_id = r.client_id
                JOIN teams t ON t.client_team_id = x.client_team_id AND t.client_id = :client_id
                join address a ON a.client_address_id = x.client_address_id AND a.client_id = :client_id
                JOIN job_types jt ON jt.client_job_type_id = x.client_type_id AND jt.client_id = :client_id
                JOIN job_status js ON js.client_job_status_id = x.client_status_id AND js.client_id = :client_id
                JOIN places p ON p.client_place_id = x.client_place_id AND p.client_id = :client_id
                LEFT JOIN priority py ON x.priority = py.client_priority_id AND :client_id = py.client_id
                ) AS t
        ON u.client_id = :client_id AND u.client_job_id = t.client_job_id
        WHEN MATCHED AND (
                        u.team_id IS DISTINCT FROM t.team_id
                    OR u.resource_id IS DISTINCT FROM t.resource_id
                    OR u.address_id IS DISTINCT FROM t.address_id
                    OR u.place_id IS DISTINCT FROM t.place_id
                    OR u.job_type_id IS DISTINCT FROM t.job_type_id
                    OR u.job_status_id IS DISTINCT FROM t.job_status_id
                    OR u.created_date IS DISTINCT FROM t.created_date
                    OR u.work_duration IS DISTINCT FROM t.real_time_service::INTEGER
                    OR u.plan_start_date IS DISTINCT FROM t.plan_start_date
                    OR u.plan_end_date IS DISTINCT FROM t.plan_end_date
                    OR u.actual_start_date IS DISTINCT FROM t.actual_start_date
                    OR u.actual_end_date IS DISTINCT FROM t.actual_end_date
                    OR u.time_limit_end IS DISTINCT FROM t.limit_end_date
                    OR u.time_limit_start IS DISTINCT FROM t.limit_start_date
                    OR u.time_service IS DISTINCT FROM t.plan_time_service
                    OR u.priority IS DISTINCT FROM t.priority_val
                    ) and u.modified_date <= t.modified_date THEN
            UPDATE SET team_id = t.team_id
                ,resource_id = t.resource_id
                ,address_id = t.address_id
                ,place_id = t.place_id
                ,job_type_id = t.job_type_id
                ,job_status_id = t.job_status_id
                ,work_duration = t.real_time_service::INTEGER
                ,plan_start_date = t.plan_start_date
                ,plan_end_date = t.plan_end_date
                ,actual_start_date = t.actual_start_date
                ,actual_end_date = t.actual_end_date
                ,time_limit_end = t.limit_end_date
                ,time_limit_start = t.limit_start_date
                ,time_service = t.plan_time_service
                ,priority = t.priority_val
                ,modified_date = t.modified_date
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_job_id, team_id, resource_id, address_id, place_id, job_type_id, job_status_id, work_duration, plan_start_date, plan_end_date, actual_start_date, actual_end_date, time_limit_end, time_limit_start, time_service, priority, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_job_id, t.team_id, t.resource_id, t.address_id, t.place_id, t.job_type_id, t.job_status_id, t.real_time_service::INTEGER, t.plan_start_date, t.plan_end_date, t.actual_start_date, t.actual_end_date, t.limit_end_date, t.limit_start_date, t.plan_time_service, t.priority_val, 'INTEGRATION', t.created_date, 'INTEGRATION', t.modified_date)
        RETURNING
            to_jsonb(u) AS registro_json, merge_action(), u.job_id
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()
    sem = asyncio.Semaphore(10)
    for row in result:
        asyncio.create_task(nofifyTeamWebSocket(r, row, sem))
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1

    await setSnapTime(r,client_id, 'jobs', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    if contador_insert > 0:
        tasks = asyncio.all_tasks()
        nomes_ativos = [task.get_name() for task in tasks]
        if f'calcDistance:{client_id}' not in nomes_ativos:
            asyncio.create_task(calcDistance(client_id=client_id),name=f'calcDistance:{client_id}')  
    
    contador = contador_insert + contador_update
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/styles", response_model=schemas.BatchResponse, status_code=200)
async def upsert_styles(
    body: list[schemas.StyleUpsertRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)

    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    fields = list(schemas.StyleUpsertRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()
    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)
        
    stmt = text(f"""
        MERGE INTO styles AS u
        USING (SELECT x.*
                FROM jsonb_to_recordset(:dados_json)
                AS x(   client_style_id text,
                        background text,
                        foreground text,
                        modified_date TIMESTAMP
                    )
                ) AS t
        ON u.client_id = :client_id AND u.client_style_id = t.client_style_id
        WHEN MATCHED AND (
                        u.background IS DISTINCT FROM t.background
                    OR u.foreground IS DISTINCT FROM t.foreground
                    ) and u.modified_date <= t.modified_date THEN
            UPDATE SET background = t.background
                ,foreground = t.foreground
                ,modified_by = 'INTEGRATION'
                ,modified_date = t.modified_date
        WHEN NOT MATCHED THEN
            INSERT (client_id, client_style_id, background, foreground, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_style_id, t.background, t.foreground,'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
        RETURNING
            u.style_id, merge_action(), to_jsonb(u) AS registro_json
        """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()
    
    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1

    await setSnapTime(r,client_id, 'styles', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update
    
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/resources", response_model=schemas.BatchResponse, status_code=200)
async def upsert_resources(
    body: list[schemas.ResourcesUpsertRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)

    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    temp_fields = ["client_status_id", "desc_status"]
    df_temp = df_dados.unique(temp_fields)
    records = df_temp.select(temp_fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)
        
    stmt = text(f"""
        MERGE INTO resource_status AS u
        USING (
            SELECT x.* FROM jsonb_to_recordset(:dados_json)
            AS x(   client_status_id text,
                    desc_status text
            ) ) AS t
        ON u.client_id = :client_id AND u.client_resource_status_id = t.client_status_id
        WHEN NOT MATCHED THEN
            INSERT (client_id,client_resource_status_id, description, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_status_id, t.desc_status, 'INTEGRATION', NOW(), 'INTEGRATION', NOW())
        RETURNING
            u.resource_status_id, merge_action(), to_jsonb(u) AS registro_json
        """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    # await db.commit()
    for row in result:
        logger.info(f"ID: {row.resource_status_id} | Ação Status do Recurso realizada: {row.merge_action}")



    coords_validas = (
        pl.col("geocode_lat_actual").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long_actual").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat_actual").is_not_null() & pl.col("geocode_long_actual").is_not_null()

    for row in df_dados.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_resource_id"],
            "message": f"Coordenadas atuais do recurso estão fora do intervalo válido - {row['client_resource_id']}, Latitude:{row['geocode_lat_actual']}, Longitude {row['geocode_long_actual']}"
        })

    df_dados = df_dados.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat_actual")).otherwise(None).alias("geocode_lat_actual"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long_actual")).otherwise(None).alias("geocode_long_actual"),
    ])

    coords_validas = (
        pl.col("geocode_lat_start").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long_start").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat_start").is_not_null() & pl.col("geocode_long_start").is_not_null()

    for row in df_dados.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_resource_id"],
            "message": f"Coordenadas de partida do recurso estão fora do intervalo válido - {row['client_aclient_resource_iddress_id']}, Latitude:{row['geocode_lat_start']}, Longitude {row['geocode_long_start']}"
        })

    df_dados = df_dados.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat_start")).otherwise(None).alias("geocode_lat_start"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long_start")).otherwise(None).alias("geocode_long_start"),
    ])

    coords_validas = (
        pl.col("geocode_lat_end").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long_end").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat_end").is_not_null() & pl.col("geocode_long_end").is_not_null()

    for row in df_dados.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_resource_id"],
            "message": f"Coordenadas de chegada do recurso estão fora do intervalo válido - {row['client_resource_id']}, Latitude:{row['geocode_lat_end']}, Longitude {row['geocode_long_end']}"
        })

    df_dados = df_dados.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat_end")).otherwise(None).alias("geocode_lat_end"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long_end")).otherwise(None).alias("geocode_long_end"),
    ])

    fields = list(schemas.ResourcesUpsertRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()
    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

            
    stmt = text(f"""
        MERGE INTO resources AS u
        USING (SELECT x.*, rs.resource_status_id
                FROM jsonb_to_recordset(:dados_json)
                AS x(   client_resource_id text,
                        resource_name text,
                        client_status_id text,
                        off_shift_flag INTEGER,
                        off_shift_start_time TIME,
                        off_shift_end_time TIME,
                        geocode_lat_actual text, 
                        geocode_long_actual text,
                        geocode_lat_start text,
                        geocode_long_start text,
                        geocode_lat_end text,
                        geocode_long_end text,
                        modified_date TIMESTAMP
                    )
                JOIN resource_status rs on rs.client_id = :client_id and rs.client_resource_status_id = x.client_status_id
                ) AS t
        ON u.client_id = :client_id AND u.client_resource_id = t.client_resource_id
        WHEN MATCHED AND (u.description IS DISTINCT FROM t.resource_name
                    OR u.resource_status_id IS DISTINCT FROM t.resource_status_id
                    OR u.off_shift_flag IS DISTINCT FROM t.off_shift_flag
                    OR u.off_shift_start_time IS DISTINCT FROM t.off_shift_start_time
                    OR u.off_shift_end_time IS DISTINCT FROM t.off_shift_end_time
                    OR u.geocode_lat_actual IS DISTINCT FROM COALESCE(t.geocode_lat_actual, u.geocode_lat_actual)
                    OR u.geocode_long_actual IS DISTINCT FROM COALESCE(t.geocode_long_actual, u.geocode_long_actual)
                    OR u.geocode_lat_start IS DISTINCT FROM COALESCE(t.geocode_lat_start, u.geocode_lat_start)
                    OR u.geocode_long_start IS DISTINCT FROM COALESCE(t.geocode_long_start, u.geocode_long_start)
                    OR u.geocode_lat_end IS DISTINCT FROM COALESCE(t.geocode_lat_end, u.geocode_lat_end)
                    OR u.geocode_long_end IS DISTINCT FROM COALESCE(t.geocode_long_end, u.geocode_long_end)
                    ) and u.modified_date <= t.modified_date THEN
            UPDATE SET 
                description = t.resource_name,
                resource_status_id = t.resource_status_id,
                off_shift_flag = t.off_shift_flag,
                off_shift_start_time = t.off_shift_start_time,
                off_shift_end_time = t.off_shift_end_time,
                geocode_lat_actual = COALESCE(t.geocode_lat_actual, u.geocode_lat_actual), 
                geocode_long_actual = COALESCE(t.geocode_long_actual, u.geocode_long_actual),
                geocode_lat_start = COALESCE(t.geocode_lat_start, u.geocode_lat_start),
                geocode_long_start = COALESCE(t.geocode_long_start, u.geocode_long_start),
                geocode_lat_end = COALESCE(t.geocode_lat_end, u.geocode_lat_end),
                geocode_long_end = COALESCE(t.geocode_long_end, u.geocode_long_end),
                modified_date = t.modified_date

        WHEN NOT MATCHED THEN
            INSERT (client_id, client_resource_id, description, resource_status_id, geocode_lat_actual, geocode_long_actual, geocode_lat_start, geocode_long_start, geocode_lat_end, geocode_long_end, off_shift_flag, off_shift_start_time, off_shift_end_time, created_by, created_date, modified_by, modified_date, modified_date_geo, modified_date_login)
            VALUES (:client_id, t.client_resource_id, t.resource_name, t.resource_status_id, t.geocode_lat_actual, t.geocode_long_actual, t.geocode_lat_start, t.geocode_long_start, t.geocode_lat_end, t.geocode_long_end, t.off_shift_flag, t.off_shift_start_time, t.off_shift_end_time,'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date, t.modified_date, t.modified_date)
        RETURNING
            u.resource_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()
    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1

    await setSnapTime(r,client_id, 'resources', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/resource_windows", response_model=schemas.BatchResponse, status_code=200)
async def upsert_resource_windows(
    body: list[schemas.ResourceWindowsUpsertRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    fields = list(schemas.ResourceWindowsUpsertRequest.model_fields.keys())
    df_dados = pl.DataFrame(body, infer_schema_length=None)

    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    last_snap = df_dados["modified_date"].max()
    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)


        
    stmt = text(f"""
        MERGE INTO resource_windows AS u
        USING (SELECT
                x.client_resource_id,
                x.client_resource_window_id,
                r.resource_id,
                week_day::INTEGER as week_day,
                x.description,
                x.start_time::TIME as start_time,
                x.end_time::TIME as end_time,
                x.modified_date
                FROM jsonb_to_recordset(:dados_json)
                AS x(
                    client_resource_id text,
                    client_resource_window_id text,
                    week_day text,
                    description text,
                    start_time text,
                    end_time text,
                    modified_date TIMESTAMP)
                JOIN resources r ON r.client_resource_id = x.client_resource_id AND r.client_id = :client_id
                ) AS t
        ON u.client_id = :client_id
                AND u.resource_id = t.resource_id
                AND u.client_rw_id = t.client_resource_window_id
        WHEN MATCHED AND (
                    u.description IS DISTINCT FROM t.description
                    OR u.start_time IS DISTINCT FROM t.start_time
                    OR u.end_time IS DISTINCT FROM t.end_time
                    OR u.week_day IS DISTINCT FROM t.week_day
                    )  and u.modified_date <= t.modified_date THEN 
            UPDATE SET description = t.description
                ,start_time = t.start_time
                ,end_time = t.end_time
                ,week_day = t.week_day
                ,modified_date = t.modified_date
                ,modified_by = 'INTEGRATION'
        WHEN NOT MATCHED THEN
            INSERT (client_id, resource_id, client_rw_id, description, start_time, end_time, week_day, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.resource_id, t.client_resource_window_id, t.description, t.start_time, t.end_time, t.week_day,'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
        RETURNING
            u.rw_id, merge_action(), to_jsonb(u) AS registro_json
    """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()
    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1

    await setSnapTime(r,client_id, 'resource_windows', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update

    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/address", response_model=schemas.BatchResponse, status_code=200)
async def upsert_address(
    body: list[schemas.AddressRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)
    
    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    fields = list(schemas.AddressRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()

    coords_validas = (
        pl.col("geocode_lat").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat").is_not_null() & pl.col("geocode_long").is_not_null()

    for row in df_dados.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_address_id"],
            "message": f"Coordenadas de chegada do recurso estão fora do intervalo válido - {row['client_address_id']}, Latitude:{row['geocode_lat']}, Longitude {row['geocode_long']}"
        })

    df_dados = df_dados.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat")).otherwise(None).alias("geocode_lat"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long")).otherwise(None).alias("geocode_long"),
    ])

    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)

    stmt = text("""
        MERGE INTO address AS u
        USING (
            SELECT DISTINCT x.*
            FROM jsonb_to_recordset(:dados_json)
            AS x(
                client_address_id text,
                address           text,
                city              text,
                state             text,
                zippost           text,
                geocode_lat       text,
                geocode_long      text,
                modified_date     TIMESTAMP
            )
            ORDER BY modified_date
        ) AS t
        ON u.client_id = :client_id AND u.client_address_id = t.client_address_id
        WHEN MATCHED AND (
               u.geocode_lat  IS DISTINCT FROM COALESCE(t.geocode_lat,  u.geocode_lat)
            OR u.geocode_long IS DISTINCT FROM COALESCE(t.geocode_long, u.geocode_long)
            OR u.address      IS DISTINCT FROM t.address
            OR u.city         IS DISTINCT FROM t.city
            OR u.state_prov   IS DISTINCT FROM t.state
            OR u.zippost      IS DISTINCT FROM t.zippost
        ) AND u.modified_date <= t.modified_date THEN
            UPDATE SET
                 geocode_lat   = COALESCE(t.geocode_lat,  u.geocode_lat)
                ,geocode_long  = COALESCE(t.geocode_long, u.geocode_long)
                ,address       = t.address
                ,city          = t.city
                ,state_prov    = t.state
                ,zippost       = t.zippost
                ,modified_by   = 'INTEGRATION'
                ,modified_date = t.modified_date
        RETURNING
            u.address_id, merge_action(), u.client_address_id
    """)

    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()

    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1
        logger.info(f"ID: {row.address_id} | {row.merge_action} address: {row.client_address_id}")

    if last_snap:
        await setSnapTime(r, client_id, 'address', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/places", response_model=schemas.BatchResponse, status_code=200)
async def upsert_places(
    body: list[schemas.PlacesRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)
    if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    fields = list(schemas.PlacesRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()
    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)
    stmt = text("""
        MERGE INTO places AS u
        USING (
            SELECT DISTINCT x.*
            FROM jsonb_to_recordset(:dados_json)
            AS x(
                client_place_id text,
                trade_name      text,
                cnpj            text,
                modified_date   TIMESTAMP
            )
            ORDER BY modified_date
        ) AS t
        ON u.client_id = :client_id AND u.client_place_id = t.client_place_id
        WHEN MATCHED AND (
               u.trade_name IS DISTINCT FROM t.trade_name
            OR u.cnpj IS DISTINCT FROM COALESCE(t.cnpj, u.cnpj)
        ) AND u.modified_date <= t.modified_date THEN
            UPDATE SET
                 trade_name    = t.trade_name
                ,cnpj          = COALESCE(t.cnpj, u.cnpj)
                ,modified_by   = 'INTEGRATION'
                ,modified_date = t.modified_date
        RETURNING
            u.place_id, merge_action(), u.client_place_id
    """)

    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()

    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1
        logger.info(f"ID: {row.place_id} | {row.merge_action} place: {row.client_place_id}")

    if last_snap:
        await setSnapTime(r, client_id, 'places', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/teams", response_model=schemas.BatchResponse, status_code=200)
async def upsert_teams(
    body: list[schemas.TeamsRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    if not body:
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

    client_id = current["clientId"]

    inconsistencies = []
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)
    if df_dados.is_empty():
        return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
    fields = list(schemas.TeamsRequest.model_fields.keys())
    last_snap = df_dados["modified_date"].max()
    coords_validas = (
        pl.col("geocode_lat").cast(pl.Float64, strict=False).is_between(-40, 10) &
        pl.col("geocode_long").cast(pl.Float64, strict=False).is_between(-80, -30)
    )
    tem_coords = pl.col("geocode_lat").is_not_null() & pl.col("geocode_long").is_not_null()

    for row in df_dados.filter(tem_coords & ~coords_validas).iter_rows(named=True):
        inconsistencies.append({
            "type": "WARNING",
            "id": row["client_team_id"],
            "message": f"Coordenadas de chegada do recurso estão fora do intervalo válido - {row['client_team_id']}, Latitude:{row['geocode_lat']}, Longitude {row['geocode_long']}"
        })

    df_dados = df_dados.with_columns([
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_lat")).otherwise(None).alias("geocode_lat"),
        pl.when(tem_coords & coords_validas).then(pl.col("geocode_long")).otherwise(None).alias("geocode_long"),
    ])

    records = df_dados.select(fields).to_dicts()
    dados_json = json.dumps(records, default=serializador_customizado)
    stmt = text("""
        MERGE INTO teams AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              client_team_id text,
                              team_name text,
                              geocode_lat text,
                              geocode_long text,
                              modified_date TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_team_id = t.client_team_id
                  WHEN MATCHED AND (
                              u.team_name IS DISTINCT FROM t.team_name
                              OR u.geocode_lat IS DISTINCT FROM COALESCE(t.geocode_lat,  u.geocode_lat)
                              OR u.geocode_long IS DISTINCT FROM COALESCE(t.geocode_long,  u.geocode_long)
                              ) AND u.modified_date <= t.modified_date THEN
                      UPDATE SET team_name = t.team_name
                          ,geocode_lat = COALESCE(t.geocode_lat,  u.geocode_lat)
                          ,geocode_long = COALESCE(t.geocode_long,  u.geocode_long)
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_date
                  RETURNING
                       u.team_id, merge_action(), to_jsonb(u) AS registro_json
    """)

    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    await db.commit()

    for row in result:
        if row.merge_action == 'INSERT':
            contador_insert += 1
        else:
            contador_update += 1
        logger.info(f"ID: {row.team_id} | {row.merge_action} Team: {row.client_place_id}")

    if last_snap:
        await setSnapTime(r, client_id, 'teams', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

    contador = contador_insert + contador_update
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/team_members", response_model=schemas.BatchResponse, status_code=200)
async def upsert_team_members(
    body: list[schemas.TeamMembersRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador = 0
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)
        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
        
        fields = list(schemas.TeamMembersRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO team_members AS u
            USING (
                SELECT
                    tm.client_id,
                    tm.team_id,
                    r.resource_id,
                    x.modified_date
                FROM jsonb_to_recordset(:dados_json) AS x(
                    client_team_id text,
                    client_resource_id text,
                    modified_date TIMESTAMP
                )
                JOIN teams tm ON tm.client_team_id = x.client_team_id and tm.client_id = :client_id
                JOIN resources r ON r.client_resource_id = x.client_resource_id and r.client_id = :client_id
            ) AS t
            ON u.client_id = t.client_id AND u.resource_id = t.resource_id AND u.team_id = t.team_id
            WHEN NOT MATCHED THEN
                INSERT (client_id, team_id, resource_id, created_by,created_date, modified_by, modified_date)
                VALUES (t.client_id, t.team_id, t.resource_id, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
            RETURNING
                to_jsonb(u) AS registro_json,
                merge_action(),
                u.uid;
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()

        for row in result:
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1
            logger.info(f"ID: {row.uid} | {row.merge_action}  {row.registro_json}")

        if last_snap:
            await setSnapTime(r, client_id, 'team_members', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/job_types", response_model=schemas.BatchResponse, status_code=200)
async def upsert_job_types(
    body: list[schemas.JobTypesRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador = 0
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)

        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
    
        fields = list(schemas.JobTypesRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO job_types AS u
            USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                    AS x(
                        client_job_type_id text,
                        desc_job_type text,
                        modified_date TIMESTAMP)
                    ) AS t
            ON u.client_id = :client_id AND u.client_job_type_id = t.client_job_type_id
            WHEN MATCHED AND ( u.description IS DISTINCT FROM t.desc_job_type )
                    AND u.modified_date <= t.modified_date THEN
                UPDATE SET description = t.desc_job_type
                    ,modified_by = 'INTEGRATION'
                    ,modified_date = t.modified_date
            RETURNING
                u.job_type_id, merge_action(), to_jsonb(u) AS registro_json
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()

        for row in result:
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1
            logger.info(f"ID: {row.job_type_id} | {row.merge_action}  {row.registro_json}")

        if last_snap:
            await setSnapTime(r, client_id, 'job_types', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/job_status", response_model=schemas.BatchResponse, status_code=200)
async def upsert_job_status(
    body: list[schemas.JobStatusRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador = 0
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)
        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
        
        fields = list(schemas.JobStatusRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO job_status AS u
            USING (SELECT x.*, s.style_id
                    FROM jsonb_to_recordset(:dados_json)
                    AS x(
                        client_job_status_id text,
                        desc_job_status text,
                        client_style_id text,
                        modified_date TIMESTAMP)
                    JOIN styles s ON s.client_style_id = x.client_style_id AND s.client_id = :client_id
                    ) AS t
            ON u.client_id = :client_id AND u.client_job_status_id = t.client_job_status_id
            WHEN MATCHED AND (
                    u.description IS DISTINCT FROM t.desc_job_status
                    OR u.style_id IS DISTINCT FROM COALESCE(t.style_id,u.style_id) )
                    AND u.modified_date <= t.modified_date THEN
                UPDATE SET description = t.desc_job_status
                    ,style_id = COALESCE(t.style_id,u.style_id)
                    ,modified_by = 'INTEGRATION'
                    ,modified_date = t.modified_date
            RETURNING
                u.job_status_id, merge_action(), to_jsonb(u) AS registro_json
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()

        for row in result:
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1
            logger.info(f"ID: {row.job_status_id} | {row.merge_action}  {row.registro_json}")

        if last_snap:
            await setSnapTime(r, client_id, 'job_status', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/resource_logged", response_model=schemas.BatchResponse, status_code=200)
async def upsert_logged_in_out(
    body: list[schemas.ResourceLoggedInOutRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador = 0
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)
        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
        
        fields = list(schemas.ResourceLoggedInOutRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO resources AS u
            USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                    AS x(
                        client_resource_id text, 
                        logged_in TIMESTAMP, 
                        logged_out TIMESTAMP, 
                        modified_date TIMESTAMP)
                    ) AS t
            ON u.client_id = :client_id AND u.client_resource_id = t.client_resource_id
            WHEN MATCHED AND (
                            u.logged_in IS DISTINCT FROM t.logged_in
                        OR u.logged_out IS DISTINCT FROM t.logged_out
                        ) AND u.modified_date_login <= t.modified_date THEN
                UPDATE SET logged_in = t.logged_in
                    ,logged_out = t.logged_out
                    ,modified_date_login = t.modified_date
                RETURNING
                    to_jsonb(u) AS registro_json, merge_action(), u.resource_id
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()
        
        sem = asyncio.Semaphore(10)
        for row in result:
            asyncio.create_task(nofifyResourceWebSocket(r, row, sem))
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1

        if last_snap:
            await setSnapTime(r, client_id, 'logintime', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/resource_actual_geopos", response_model=schemas.BatchResponse, status_code=200)
async def upsert_actual_geopos(
    body: list[schemas.ResourceActualGeoPosRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador = 0
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)
        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
        
        fields = list(schemas.ResourceActualGeoPosRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO resources AS u
            USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                    AS x(
                        client_resource_id text, 
                        geocode_lat text, 
                        geocode_long text, 
                        modified_date TIMESTAMP)
                    ) AS t
            ON u.client_id = :client_id AND u.client_resource_id = t.client_resource_id
            WHEN MATCHED AND (
                            u.geocode_lat_actual IS DISTINCT FROM COALESCE(t.geocode_lat,u.geocode_lat_actual) 
                        OR u.geocode_long_actual IS DISTINCT FROM COALESCE(t.geocode_long,u.geocode_long_actual)
                        ) AND u.modified_date_geo <= t.modified_date THEN
                UPDATE SET geocode_lat_actual = COALESCE(t.geocode_lat,u.geocode_lat_actual) 
                    ,geocode_long_actual = COALESCE(t.geocode_long,u.geocode_long_actual)
                    ,modified_date_geo = t.modified_date
                RETURNING
                    to_jsonb(u) AS registro_json, merge_action(), u.resource_id
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()

        sem = asyncio.Semaphore(10)
        for row in result:
            asyncio.create_task(nofifyResourceWebSocket(r, row, sem))

            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1


        if last_snap:
            await setSnapTime(r, client_id, 'actual_geopos', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)

@app.post("/priority", response_model=schemas.BatchResponse, status_code=200)
async def upsert_priority(
    body: list[schemas.PriorityRequest],
    db: AsyncSession = Depends(get_db),
    r: Redis = Depends(redis_client.get_redis),
    current: dict = Depends(get_integration_client),
):
    try:
        if not body:
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])

        client_id = current["clientId"]

        inconsistencies = []
        contador_insert = 0
        contador_update = 0

        df_dados = pl.DataFrame(body, infer_schema_length=None)
        if df_dados.is_empty():
            return schemas.BatchResponse(processed=0, inserted=0, updated=0, results=[], errors=[])
        
        fields = list(schemas.PriorityRequest.model_fields.keys())
        last_snap = df_dados["modified_date"].max()
        records = df_dados.select(fields).to_dicts()
        dados_json = json.dumps(records, default=serializador_customizado)
        stmt = text("""
            MERGE INTO priority AS u
            USING (SELECT
                        x.*
                        FROM jsonb_to_recordset(:dados_json)
                        AS x(
                            client_priority_id text,
                            priority integer, 
                            modified_date TIMESTAMP) 
                    ) AS t
            ON u.client_id = :client_id AND u.client_priority_id = t.client_priority_id
            WHEN MATCHED AND (
                        u.priority IS DISTINCT FROM COALESCE(t.priority, u.priority)
                        )  THEN
                UPDATE SET 
                    priority = COALESCE(t.priority, u.priority)
                    ,modified_by = 'INTEGRATION'
                    ,modified_date = t.modified_date
            WHEN NOT MATCHED THEN
                INSERT (client_id, client_priority_id, priority, created_by, created_date, modified_by, modified_date)
                VALUES (:client_id, t.client_priority_id, t.priority, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
            RETURNING
                u.priority_id, merge_action(), to_jsonb(u) AS registro_json
        """)

        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        await db.commit()

        for row in result:
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1
            logger.info(f"ID: {row.priority_id} | {row.merge_action}  {row.registro_json}")

        if last_snap:
            await setSnapTime(r, client_id, 'priority', last_snap.strftime('%Y-%m-%d %H:%M:%S'))

        contador = contador_insert + contador_update
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        detalhes = traceback.extract_tb(exc_traceback)[0]
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
         
    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)