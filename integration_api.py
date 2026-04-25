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
import polars as pl
import duckdb
import asyncio
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

# ─── App ─────────────────────────────────────────────────────────────────────

def serializador_customizado(obj):
    # Se for data ou data/hora, usa o formato ISO
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Se for outro tipo desconhecido, gera erro (comportamento padrão)
    raise TypeError(f"Tipo {type(obj)} não é serializável")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        
        # await conn.run_sync(Base.metadata.drop_all)
        # logger.info("Tabelas do banco dropadas com sucesso!")
        # await r.flushall()
        # logger.info("Cache Redis limpo com sucesso!")

        await conn.run_sync(Base.metadata.create_all)
        logger.info("Tabelas do banco verificadas/criadas com sucesso!")

    r = redis_client.get_redis()
    await r.flushall()
    
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

async def getSnapTime(r, client_id):
    clientKey = f'snap_time:{client_id}'
    payload = await r.get(clientKey)
    if not payload:
        tempDT = (datetime.now() - timedelta(days=(365*2)))
        tempJobDT = (datetime.now() - timedelta(days=5))
        dateTime = tempDT.strftime('%Y-%m-%d ') + '00:00:00'
        dateTimeJob = tempJobDT.strftime('%Y-%m-%d ') + '00:00:00'
        payload = {
            "styles" : dateTime,
            "address" : dateTime,
            "teams" : dateTime,
            "resources" : dateTime,
            "resource_windows" : dateTime,
            "places" : dateTime,
            "geotime" : dateTime,
            "logintime" : dateTime,
            "jobs": dateTimeJob,
            "job_types" : dateTime,
            "job_status" : dateTime,
        }
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
    snapkey   = f"snap_time:{client_id}"
    payload = await getSnapTime(r,client_id)
    print(type(payload))
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
    df_dados = pl.DataFrame(body, infer_schema_length=None)

    duck.execute("CREATE TEMP TABLE JOBS_TEMP AS SELECT * FROM df_dados")
    ## dados do team
    result = duck.execute("""
        SELECT DISTINCT 
            client_team_id, 
            desc_team AS team_name,
            team_modified_date AS modified_date,
            max(team_modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM JOBS_TEMP
        ORDER BY team_modified_date ASC
    """).pl().to_dicts()
    
    dados_json = json.dumps(result, default=serializador_customizado)
    
    stmt = text(f"""
        MERGE INTO teams AS u
        USING (
            SELECT x.* FROM jsonb_to_recordset(:dados_json)
            AS x(   client_team_id text,
                    team_name text,
                    modified_date TIMESTAMP
            ) ) AS t
        ON u.client_id = :client_id AND u.client_team_id = t.client_team_id
        WHEN NOT MATCHED THEN
            INSERT (client_id,client_team_id, team_name, created_by, created_date, modified_by, modified_date)
            VALUES (:client_id, t.client_team_id, t.team_name, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
        RETURNING
            u.team_id, merge_action(), to_jsonb(u) AS registro_json
        """)
    result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    # await db.commit()
    for row in result:
        logger.info(f"ID: {row.team_id} | Ação Team realizada: {row.merge_action}")

    # ## resources
    # result = duck.execute("""
    #     SELECT DISTINCT 
    #         client_resource_id, 
    #         resource_name,
    #         resource_modified_date AS modified_date,
    #         max(resource_modified_date)  OVER (PARTITION BY 1)  last_snap 
    #      FROM JOBS_TEMP
    #     WHERE client_resource_id IS NOT NULL
    #     order by resource_modified_date ASC
    # """).pl().to_dicts()

    # if result:
    #     dados_json = json.dumps(result, default=serializador_customizado)

    #     stmt = text(f"""
    #         MERGE INTO resources AS u
    #         USING (
    #             SELECT x.* 
    #             FROM jsonb_to_recordset(:dados_json)
    #             AS x( client_resource_id text,
    #                     resource_name text,
    #                     modified_date TIMESTAMP
    #                 ) 
    #             ) AS t
    #         ON u.client_id = :client_id AND u.client_resource_id = t.client_resource_id
    #         WHEN NOT MATCHED THEN
    #             INSERT (client_id,client_resource_id, description, created_by, created_date, modified_by, modified_date)
    #             VALUES (:client_id, t.client_resource_id, t.resource_name, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
    #         RETURNING
    #             u.resource_id, merge_action(), to_jsonb(u) AS registro_json
    #     """)
    #     result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
    #     for row in result:
    #         logger.info(f"ID: {row.resource_id} | Ação Resource realizada: {row.merge_action}")
    
    ## status da tarefa
    result = duck.execute("""
        SELECT DISTINCT 
            client_status_id, 
            desc_status,
            client_style_id, 
            status_modified_date AS modified_date,
            max(status_modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM JOBS_TEMP
        ORDER BY status_modified_date ASC
    """).pl().to_dicts()

    if result:
        dados_json = json.dumps(result, default=serializador_customizado)

        stmt = text(f"""
            MERGE INTO job_status AS u
                  USING (SELECT DISTINCT x.*, s.style_id
                          FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              client_status_id text,
                              desc_status text,
                              client_style_id text,
                              modified_date TIMESTAMP)
                          LEFT JOIN styles s ON x.client_style_id = s.client_style_id AND s.client_id = :client_id
                          ) AS t
                  ON u.client_id = :client_id AND u.client_job_status_id = t.client_status_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_job_status_id, style_id, description, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.client_status_id, t.style_id, t.desc_status, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
                  RETURNING
                      u.job_status_id, merge_action(), to_jsonb(u) AS registro_json
        """)
        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        for row in result:
            logger.info(f"ID: {row.job_status_id} | Ação JOB STATUS realizada: {row.merge_action}")

    ## tipo da tarefa
    result = duck.execute("""
        SELECT DISTINCT 
            client_type_id,
            desc_type,
            type_modified_date AS modified_date,
            max(type_modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM JOBS_TEMP
        ORDER BY type_modified_date ASC
    """).pl().to_dicts()

    if result:
        dados_json = json.dumps(result, default=serializador_customizado)

        stmt = text(f"""
            MERGE INTO job_types AS u
                  USING (
                      SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
                      AS x(   client_type_id text,
                              desc_type text,
                              modified_date TIMESTAMP
                      ) ) AS t
                  ON u.client_id = :client_id AND u.client_job_type_id = t.client_type_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_job_type_id, description, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.client_type_id, t.desc_type, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
                  RETURNING
                       u.job_type_id, merge_action(), to_jsonb(u) AS registro_json
        """)
        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        for row in result:
            logger.info(f"ID: {row.job_type_id} | Ação JOB TYPE realizada: {row.merge_action}")

    ## Places
    result = duck.execute("""
        SELECT DISTINCT 
            client_place_id, 
            trade_name,
            cnpj,              
            place_modified_date AS modified_date,
            max(place_modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM JOBS_TEMP
        ORDER BY place_modified_date ASC
    """).pl().to_dicts()

    if result:
        dados_json = json.dumps(result, default=serializador_customizado)

        stmt = text(f"""
            MERGE INTO places AS u
                  USING (
                      SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
                      AS x(   client_place_id text,
                              trade_name text,
                              cnpj text,
                              modified_date TIMESTAMP
                      ) ) AS t
                  ON u.client_id = :client_id AND u.client_place_id = t.client_place_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_place_id, trade_name, cnpj, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.client_place_id, t.trade_name, t.cnpj, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
                  RETURNING
                       u.place_id, merge_action(), to_jsonb(u) AS registro_json
        """)
        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        for row in result:
            logger.info(f"ID: {row.place_id} | Ação PLACE realizada: {row.merge_action}")

    ## Address
    result = duck.execute("""
        SELECT DISTINCT 
            client_address_id, 
            address,
            city,    
            state,
            zip_code,
            geocode_lat,
            geocode_long,
            address_modified_date AS modified_date,
            max(address_modified_date)  OVER (PARTITION BY 1)  last_snap 
         FROM JOBS_TEMP
        ORDER BY address_modified_date
    """).pl().to_dicts()

    if result:
        last_snap = result[0]['last_snap'].strftime('%Y-%m-%d %H:%M:%S')
        for row in result:
            try:
                client_address_id = row['client_address_id']
                geocode_lat = row['geocode_lat']
                geocode_long = row['geocode_long']
                if geocode_lat and geocode_long:
                    lat = float(geocode_lat)
                    long = float(geocode_long)
                    if not (-40 <= lat <= 10 and -80 <= long <= -30):
                        raise ValueError
                else:
                    raise ValueError
            except ValueError:
                inconsistencies.append({"type":"WARNING", "id":client_address_id, "message":f"Coordenadas fora do intervalo válido para o endereço -  {client_address_id}, Latitude:{geocode_lat}, Longitude {geocode_long}"})
        
        print(inconsistencies)

        dados_json = json.dumps(result, default=serializador_customizado)

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
                          modified_date TIMESTAMP)
                    ) AS t
              ON u.client_id = :client_id AND u.client_address_id = t.client_address_id
              WHEN NOT MATCHED THEN
                  INSERT (client_id, client_address_id, address, geocode_lat, geocode_long, city , state_prov, zippost, created_by, created_date, modified_by, modified_date)
                  VALUES (:client_id, t.client_address_id, t.address, t.geocode_lat, t.geocode_long, t.city, t.state, t.zip_post, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_date)
              RETURNING
                  u.address_id, merge_action(), to_jsonb(u) AS registro_json
        """)
        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        # for row in result:
        #     logger.info(f"ID: {row.address_id} | Ação ADDRESS realizada: {row.merge_action}")

    ## Jobs
    result = duck.execute("""
        SELECT j.*, max(modified_date)  OVER (PARTITION BY 1)  last_snap
         FROM JOBS_TEMP j
        ORDER BY modified_date
    """).pl().to_dicts()

    if result:
        last_snap = result[0]['last_snap'].strftime('%Y-%m-%d %H:%M:%S')
        
        dados_json = json.dumps(result, default=serializador_customizado)

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
        contador = 0
        contador_insert = 0
        contador_update = 0
        for row in result:
            if row.merge_action == 'INSERT':
                contador_insert += 1
            else:
                contador_update += 1

        await setSnapTime(r,client_id, 'jobs', last_snap)
            # logger.info(f"ID: {row.job_id} | Ação JOB realizada: {row.merge_action}")


    await db.rollback()

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
    contador = 0
    contador_insert = 0
    contador_update = 0

    df_dados = pl.DataFrame(body, infer_schema_length=None)

    duck.execute("CREATE TEMP TABLE STYLES_TEMP AS SELECT * FROM df_dados")
    ## dados do team
    result = duck.execute("""
        SELECT 
            client_style_id, 
            background,
            foreground,
            modified_date,
            max(modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM STYLES_TEMP
        ORDER BY modified_date
    """).pl().to_dicts()

    if result:
        last_snap = result[0]['last_snap'].strftime('%Y-%m-%d %H:%M:%S')

        dados_json = json.dumps(result, default=serializador_customizado)
        
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

        await setSnapTime(r,client_id, 'styles', last_snap)


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
    df_dados = pl.DataFrame(
        body, 
        infer_schema_length=None
    )

    contador = 0
    contador_insert = 0
    contador_update = 0

    duck.execute("CREATE TEMP TABLE RESOURCES_TEMP AS SELECT * FROM df_dados")
    ## Criar os status dos recursos
    result = duck.execute("""
        SELECT DISTINCT 
            client_status_id as client_resource_status_id, 
            desc_status
         FROM RESOURCES_TEMP
        ORDER BY client_status_id ASC
    """).pl().to_dicts()
    
    if result:
        
        dados_json = json.dumps(result, default=serializador_customizado)
        
        stmt = text(f"""
            MERGE INTO resource_status AS u
            USING (
                SELECT x.* FROM jsonb_to_recordset(:dados_json)
                AS x(   client_resource_status_id text,
                        desc_status text
                ) ) AS t
            ON u.client_id = :client_id AND u.client_resource_status_id = t.client_resource_status_id
            WHEN NOT MATCHED THEN
                INSERT (client_id,client_resource_status_id, description, created_by, created_date, modified_by, modified_date)
                VALUES (:client_id, t.client_resource_status_id, t.desc_status, 'INTEGRATION', NOW(), 'INTEGRATION', NOW())
            RETURNING
                u.resource_status_id, merge_action(), to_jsonb(u) AS registro_json
            """)
        result = await db.execute(stmt, {"dados_json": dados_json, "client_id": client_id})
        # await db.commit()
        for row in result:
            logger.info(f"ID: {row.resource_status_id} | Ação Status do Recurso realizada: {row.merge_action}")


    result = duck.execute("""
        SELECT 
            client_resource_id, 
            resource_name,
            client_status_id,
            desc_status,
            off_shift_flag,
            off_shift_start_time,
            off_shift_end_time,
            geocode_lat_actual, 
            geocode_long_actual,
            geocode_lat_start,
            geocode_long_start,
            geocode_lat_end,
            geocode_long_end,
            modified_date,
            max(modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM RESOURCES_TEMP
        ORDER BY modified_date
    """).pl().to_dicts()
    
    if result:
        for row in result:
            client_resource_id = row['client_resource_id']
            geocode_lat_actual = row['geocode_lat_actual']
            geocode_long_actual = row['geocode_long_actual']
            geocode_lat_start = row['geocode_lat_start']
            geocode_long_start = row['geocode_long_start']
            geocode_lat_end = row['geocode_lat_end']
            geocode_long_end = row['geocode_long_end']
            try:
                if geocode_lat_actual and geocode_long_actual:
                    lat = float(geocode_lat_actual)
                    long = float(geocode_long_actual)
                    if not (-40 <= lat <= 10 and -80 <= long <= -30):
                        raise ValueError
            except ValueError:
                inconsistencies.append({"type":"WARNING", "id":client_resource_id, "message":f"Coordenadas atuais do recurso estão fora do intervalo válido - {client_resource_id}, Latitude:{geocode_lat_actual}, Longitude {geocode_long_actual}"})
                duck.execute("UPDATE RESOURCES_TEMP set geocode_lat_actual = NULL, geocode_long_actual = NULL WHERE client_resource_id = ?",[client_resource_id])
            
            try:
                if geocode_lat_start and geocode_long_start:
                    lat = float(geocode_lat_start)
                    long = float(geocode_long_start)
                    if not (-40 <= lat <= 10 and -80 <= long <= -30):
                        raise ValueError
            except ValueError:
                inconsistencies.append({"type":"WARNING", "id":client_resource_id, "message":f"Coordenadas de partida do recurso estão fora do intervalo válido - {client_resource_id}, Latitude:{geocode_lat_start}, Longitude {geocode_long_start}"})
                duck.execute("UPDATE RESOURCES_TEMP set geocode_lat_start = NULL, geocode_long_start = NULL WHERE client_resource_id = ?",[client_resource_id])
            
            try:
                if geocode_lat_end and geocode_long_end:
                    lat = float(geocode_lat_end)
                    long = float(geocode_long_end)
                    if not (-40 <= lat <= 10 and -80 <= long <= -30):
                        raise ValueError
            except ValueError:
                inconsistencies.append({"type":"WARNING", "id":client_resource_id, "message":f"Coordenadas de chegada do recurso estão fora do intervalo válido - {client_resource_id}, Latitude:{geocode_lat_end}, Longitude {geocode_long_end}"})
                duck.execute("UPDATE RESOURCES_TEMP set geocode_lat_end = NULL, geocode_long_end = NULL WHERE client_resource_id = ?",[client_resource_id])
        
        print('Total de inconsistencias ...',len(inconsistencies))

        if len(inconsistencies) > 0:
            result = duck.execute("""
                SELECT 
                    client_resource_id, 
                    resource_name,
                    client_status_id,
                    desc_status,
                    off_shift_flag,
                    off_shift_start_time,
                    off_shift_end_time,
                    geocode_lat_actual, 
                    geocode_long_actual,
                    geocode_lat_start,
                    geocode_long_start,
                    geocode_lat_end,
                    geocode_long_end,
                    modified_date,
                    max(modified_date)  OVER (PARTITION BY 1)  last_snap  
                FROM RESOURCES_TEMP
                ORDER BY modified_date
            """).pl().to_dicts()
            
        if result:
            last_snap = result[0]['last_snap'].strftime('%Y-%m-%d %H:%M:%S')
            dados_json = json.dumps(result, default=serializador_customizado)
            
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

            await setSnapTime(r,client_id, 'resources', last_snap)

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
    df_dados = pl.DataFrame(
        body, 
        infer_schema_length=None
    )

    contador = 0
    contador_insert = 0
    contador_update = 0

    duck.execute("CREATE TEMP TABLE RESOURCE_WINDOWS_TEMP AS SELECT * FROM df_dados")
    
    result = duck.execute("""
        SELECT 
            client_resource_id, 
            client_resource_window_id,
            week_day,
            description,
            start_time,
            end_time,
            modified_date,
            max(modified_date)  OVER (PARTITION BY 1)  last_snap  
         FROM RESOURCE_WINDOWS_TEMP
        ORDER BY modified_date
    """).pl().to_dicts()
    
    if result:
        last_snap = result[0]['last_snap'].strftime('%Y-%m-%d %H:%M:%S')

        dados_json = json.dumps(result, default=serializador_customizado)
        
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

        await setSnapTime(r,client_id, 'resource_windows', last_snap)

    return schemas.BatchResponse(processed=contador, inserted=contador_insert, updated=contador_update, results=[], errors=inconsistencies)
