import asyncio
import redis.asyncio as Redis
import database
import redis_client
import json
from datetime import date, datetime
import models, schemas
from auth import verify_password, create_access_token
from database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, update, delete, exists
from sqlalchemy.future import select 
from fastapi import FastAPI, Depends, HTTPException, status,  Response, Request, Query, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import  List, Optional
from jose import JWTError, jwt
# from notification_service import criar_e_enviar_notificacao
from datetime import datetime, timedelta, timezone
from config import settings
from loguru import logger
import httpx

async def optimize_routes_vroom(payload: dict) -> dict:
    logger.debug(
        "VROOM request | vehicles={} jobs={}",
        len(payload.get("vehicles", [])),
        len(payload.get("jobs", [])),
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                settings.vroom_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        if response.status_code != 200:
            logger.error(
                "VROOM HTTP {} | url={} | body={}",
                response.status_code,
                settings.vroom_url,
                response.text[:2000],
            )
            response.raise_for_status()

        result = response.json()

        unassigned = result.get("unassigned", [])
        routes     = result.get("routes", [])
        logger.info(
            "VROOM ok | routes={} unassigned={}",
            len(routes),
            len(unassigned),
        )
        if unassigned:
            logger.warning("VROOM unassigned jobs: {}", [u.get("id") for u in unassigned])

        return result

    except httpx.TimeoutException as exc:
        logger.error("VROOM timeout (60s) | url={} | {}", settings.vroom_url, exc)
        raise

    except httpx.ConnectError as exc:
        logger.error("VROOM connection error | url={} | {}", settings.vroom_url, exc)
        raise

    except httpx.HTTPStatusError as exc:
        logger.error(
            "VROOM HTTP error {} | url={} | response={}",
            exc.response.status_code,
            settings.vroom_url,
            exc.response.text[:2000],
        )
        raise

    except Exception as exc:
        logger.exception("VROOM unexpected error | url={} | {}", settings.vroom_url, exc)
        raise


async def get_route_distance(coordinates: List[List[float]]) -> dict:
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coordinates)
    url = f"https://routes.imagineit.com.br/routes/route/v1/driving/{coords_str}"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, params={"overview": "false"})
        response.raise_for_status()

    data = response.json()

    if data.get("code") != "Ok" or not data.get("routes"):
        raise ValueError(f"OSRM retornou erro: {data.get('code')}")

    distance_meters = data["routes"][0]["distance"]
    distance_time = data["routes"][0]["duration"]
    return {
        "distance_meters": distance_meters,
        "distance_km": round(distance_meters / 1000, 3),
        "distance_time": distance_time,
        "distance_time_minutes": round(distance_time / 60, 2)
    }

from typing import List

async def get_route_distance_block(coordinates: List[List[float]]) -> List[dict]:
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coordinates)
    url = f"https://routes.imagineit.com.br/routes/route/v1/driving/{coords_str}?overview=false"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, params={"overview": "false"})
        response.raise_for_status()

    # Extrai o JSON
    data = response.json()

    if data.get("code") != "Ok" or not data.get("routes"):
        raise ValueError(f"OSRM retornou erro: {data.get('code')}")

    # USA A VARIÁVEL 'data' E NÃO A 'response'
    legs = data['routes'][0]['legs']

    # 'legs' já é uma lista de dicionários, pode retornar direto
    return legs

async def cleanSessionsRedis(r: Redis, session_web_id: str):
    # await r.delete(f"session:{session_web_id}")
    # await r.delete(f"filter:{session_web_id}")
    # await dropUserSession(session_web_id)
    return True

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:9000", 
        "http://127.0.0.1:9000",
        "https://mtlfsmtst.avcweb.com.br",
        "https://mtlfsm.avcweb.com.br",
        "https://mtlfsmsup.avcweb.com.br"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=86400
)

from fastapi.security import OAuth2PasswordBearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def get_user_from_token(token: str):
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        
        session = payload.get("session_id")
        
        if not session:
            raise HTTPException(status_code=401, detail="Token sem identificação de usuário (sub/id)")
            
        return str(session)
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

def format_sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        dataToken = {
            "clientId": payload.get("clientId"),
            "userId": payload.get("userId"),
            "userName": payload.get("userName"),
            "superUser": payload.get("superUser"),
            "clientUid": payload.get("clientUid"),
            "clientDomain": payload.get("clientDomain")
        }
        logger.info(f"Token decodificado: {dataToken}")
        if dataToken["clientId"] is None or dataToken["userId"] is None or dataToken["superUser"] is None:
            raise credentials_exception
        
    except jwt.ExpiredSignatureError:
        raise credentials_exception
    except JWTError:
        raise credentials_exception

    return dataToken


@app.get("/events")  # Recebemos o ID na URL
async def events_endpoint(
    request: Request, 
    token: str = Query(...),
    r: Redis = Depends(redis_client.get_redis)):

    session_web_id = get_user_from_token(token)

    pubsub = r.pubsub()

    async def process_queue():
        messages = []

        while True:

            msg_json = await r.lpop(f"notify:{session_web_id}:queue")
            if not msg_json:
                break
            messages.append(msg_json)
        
        if messages:
            for m in messages:
                yield f"data: {m}\n\n"

    async def event_generator():
        try:
            await pubsub.subscribe(f"notify:{session_web_id}:notify")
            
            async for msg in process_queue():
                yield msg

            while True:
                if await request.is_disconnected():
                    break

                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=30)
                    
                    if message and message['data'] == 'check_queue':
                        async for msg in process_queue():
                            yield msg
                    
                    else:
                        yield 'data: {"type": "PING", "status": "conectado"}\n\n'

                except (Redis.ConnectionError, RuntimeError):
                    break    
                except Exception as e:
                    logger.error(f"Erro no loop SSE: {e}")
                    break

        except asyncio.CancelledError:
            logger.info(f"Session {session_web_id} desconectou.")
            asyncio.create_task(cleanSessionsRedis(r, session_web_id))
        finally:
            await pubsub.unsubscribe(f"notify:{session_web_id}:notify")
            await r.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ROTAS DE AUTENTICAÇÃO

@app.post("/login", response_model=schemas.Token)
async def login(form_data: schemas.LoginRequest, request: Request, r: Redis = Depends(redis_client.get_redis), db: AsyncSession = Depends(get_db)):
    logger.info(f"Login attempt for domain: {form_data.domain}, user: {form_data.user} from IP: {request.client.host}")
    client_ip = request.client.host
    rate_key = f"login_rate:{client_ip}"
    attempts = await r.incr(rate_key)
    if attempts == 1:
        await r.expire(rate_key, 60)
    if attempts > 10:
        ttl = await r.ttl(rate_key)
        raise HTTPException(status_code=429, detail=f"Muitas tentativas. Tente novamente em {ttl}s.")
    
    domain = form_data.domain
    user = form_data.user
    pwd = form_data.pwd
    
    logger.info(f"Verificando credenciais para domínio: {domain}, usuário: {user}")
    rDomains = await db.execute(select(models.Clients).where(models.Clients.domain == domain))
    clientDb = rDomains.scalars().first()

    if not clientDb:
        logger.warning(f"Domínio não encontrado: {domain}")
        raise HTTPException(status_code=401, detail="Credenciais inválidas para o domínio especificado")
    
    clientId = clientDb.client_id
    clientUid = clientDb.uid
    clientDomain = clientDb.domain

    logger.info(f"Domínio encontrado: {domain} (clientId: {clientId}). Verificando usuário: {user}")
    rUsers = await db.execute(select(models.Users).where(models.Users.user_name == user, models.Users.client_id == clientId))
    userDb = rUsers.scalars().first()

    if not userDb or not verify_password(pwd, userDb.passwd):
        raise HTTPException(status_code=401, detail="Credenciais inválidas para o domínio e usuário especificado")
    
    superUser = userDb.super_user

    dataToken = {
        "userId": userDb.user_id,
        "userName": userDb.user_name,
        "superUser": superUser,
        "clientId": clientId,
        "clientUid": str(clientUid),
        "clientDomain": clientDomain
    }
    logger.info(f"Gerando token de acesso para usuário: {userDb.user_name} (userId: {userDb.user_id}) no domínio: {clientDomain}")
    access_token = create_access_token(data=dataToken)
    logger.info(f"Login successful for user: {userDb.user_name} in domain: {clientDomain}")
    return {"access_token": access_token, "token_type": "bearer"}

# Rota protegida

@app.get("/client", response_model=schemas.ViewClientResponse)
async def getCurrentClient(
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):

    rClient = await db.execute(select(models.Clients).where(models.Clients.client_id == current_user["clientId"]))
    ClientDb = rClient.scalars().first()

    if not ClientDb:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")

    # 2. Segurança: Verifica se pertence à mesma família (domínio)
    if ClientDb.domain != current_user["clientDomain"]:
        raise HTTPException(status_code=403, detail="Acesso negado a este cliente")

    return ClientDb

@app.get("/teams", response_model=List[schemas.ViewTeamResponse])
async def getTeams(
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["userId"]
    client_id = current_user["clientId"]

    result = await db.execute(
        select(models.Teams)
        .join(models.UserTeam, 
              (models.UserTeam.team_id == models.Teams.team_id) & 
              (models.UserTeam.client_id == models.Teams.client_id))
        .where(
            models.UserTeam.user_id == user_id,
            models.UserTeam.client_id == client_id
        )
    )
    teamsDb = result.scalars().all()

    return teamsDb

@app.get("/styles", response_model=List[schemas.ViewStylesResponse])
async def getStyles(
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
        
    result = await db.execute(select(models.Styles).where(models.Styles.client_id == clientId))
    
    stylesDb = result.scalars().all()

    return stylesDb


@app.get("/resourcewindows", response_model=List[schemas.ViewResourceWindowsResponse])
async def getResourceWindows(
    week_day: int = Query(..., description="Dia da semana Inteiro"),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["userId"]
    client_id = current_user["clientId"]

    result = await db.execute(
        select(models.ResourceWindows)
        .join(models.TeamMembers,
              (models.TeamMembers.resource_id == models.ResourceWindows.resource_id) &
              (models.TeamMembers.client_id == models.ResourceWindows.client_id))
        .join(models.UserTeam,
              (models.UserTeam.team_id == models.TeamMembers.team_id) &
              (models.UserTeam.client_id == models.TeamMembers.client_id))
        .where(
            models.UserTeam.user_id == user_id,
            models.UserTeam.client_id == client_id,
            models.ResourceWindows.week_day == week_day
        )
        .distinct()
    )
    resourceWindowDb = result.scalars().all()

    return resourceWindowDb

@app.get("/resourcestree")
async def getResourcesTree(
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["userId"]
    client_id = current_user["clientId"]

    try:
        # Tenta executar uma query simples no banco
        smtp = text("""
          with q1 as (
            select 
            client_id,
            resource_id,
            uid as resource_uid,
            client_resource_id,
            description,
            actual_geocode_lat,
            actual_geocode_long,
            geocode_lat_from,
            geocode_long_from,
            geocode_lat_at,
            geocode_long_at,
            fl_off_shift,
            time_setup,
            time_service,
            time_overlap,
            logged_in,
            logged_out,
            modified_date
            from resources
          )
          select t.team_id,
          t.uid AS team_uid,
          t.client_team_id,
          t.team_name,
          jsonb_agg(to_jsonb(q1) ORDER BY q1.description) AS resources
          from teams t
          join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
          join team_members tm on tm.client_id = t.client_id and  tm.team_id = t.team_id
          join q1 on q1.client_id = t.client_id and q1.resource_id = tm.resource_id
          where ut.client_id = :client_id and ut.user_id = :user_id
          group by t.team_id,
          t.uid,
          t.client_team_id,
          t.team_name
        """).bindparams(client_id=client_id, user_id=user_id)
                            
        result = await db.execute(smtp)
        rows = result.mappings().all()
        print(type(rows))
        arvore = [dict(row) for row in rows]
        return arvore
        
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")

@app.get("/jobs")
async def getJobsResources(
    p_date: str = Query(..., description="Data no formato YYYY-MM-DD"),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]

    try:
        # Tenta executar uma query simples no banco
        smtp = text("""
          select 
            'ORIGIN' as type,
            j.job_id,
            j.client_job_id,
            j.team_id, 
            j.resource_id, 
            r.client_resource_id,
            j.job_status_id, 
            js.description AS status_description,
            js.internal_code_status,
            js.style_id,
            j.job_type_id, 
            jt.description as type_description,
            j.address_id, 
            a.client_address_id, 
            a.geocode_lat, 
            a.geocode_long, 
            a.address, 
            a.city, 
            a.state_prov, 
            a.zippost, 
            a.time_setup,
            p.trade_name, 
            p.cnpj,
            j.time_setup, 
            j.time_service, 
            j.plan_start_date, 
            j.plan_end_date, 
            j.actual_start_date, 
            j.actual_end_date, 
            CASE WHEN j.actual_start_date is not null then j.actual_start_date else j.plan_start_date end as start_date,
            CASE WHEN j.actual_end_date is not null then j.actual_end_date else j.plan_end_date end as start_date,
            j.time_limit_start, 
            j.time_limit_end
            from jobs j
            join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
            join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
            join teams t on t.client_id = j.client_id and t.team_id = j.team_id
            join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
            join address a on a.client_id = j.client_id and a.address_id = j.address_id
            join places p on p.client_id = j.client_id and p.place_id = j.place_id
            left join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
            where ut.client_id = :client_id
              and ut.user_id = :user_id
              and (
                    (j.actual_start_date is null and j.plan_start_date >= cast(:p_date as date) and j.plan_start_date < cast(:p_date as date) + interval '1 day')
                  or (
                    j.actual_start_date is not null AND (j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day')
                    )
                  )
            order by j.team_id, j.resource_id nulls last,j.actual_start_date nulls last, j.plan_start_date
        """).bindparams(client_id=clientId, user_id=userId, p_date=p_date)
                            
        result = await db.execute(smtp)
        rows = result.mappings().all()

        res = [dict(row) for row in rows]
        return res
        
    except Exception as e:
        logger.error(f"[getJobsResources] Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")


@app.get("/openjobs")
async def getOpenJobs(
    simulation_id: Optional[int] = None,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):

    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    try:
      simulation_filter = ""
      bind_params: dict = {"client_id": clientId, "user_id": userId}
      if simulation_id is not None:
          simulation_filter = """
                and not exists (
                    select 1 from simulation_jobs sj
                    where sj.client_id = j.client_id
                      and sj.job_id = j.job_id
                      and sj.simulation_id = :simulation_id
                )"""
          bind_params["simulation_id"] = simulation_id

      smtp = text(f"""
        select
              j.client_id,
              j.job_id,
              j.client_job_id,
              j.team_id,
              j.resource_id,
              r.client_resource_id,
              r.description AS resource_name,
              r.geocode_lat_from,
              r.geocode_long_from,
              r.geocode_lat_at,
              r.geocode_long_at,
              j.job_status_id,
              js.description,
              j.job_type_id,
              j.address_id,
              a.geocode_lat,
              a.geocode_long,
              a.address,
              a.city,
              a.state_prov,
              j.place_id,
              p.trade_name,
              p.cnpj,
              j.time_setup,
              j.time_service,
              j.time_limit_start,
              j.time_limit_end,
              j.actual_start_date,
              j.actual_end_date,
              j.plan_start_date,
              j.plan_end_date,
              j.pp_resource_id,
              j.time_limit_end
              from jobs j
              join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
              join teams t on t.client_id = j.client_id and t.team_id = j.team_id
              join address a on a.client_id = j.client_id and a.address_id = j.address_id
              join places p on p.client_id = j.client_id and p.place_id = j.place_id
              join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
              left join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
              where ut.client_id = :client_id
                and ut.user_id = :user_id
                and (js.internal_code_status not in ('CONCLU', 'CANCEL', 'CLOSED') and  js.internal_code_status is not null)
                {simulation_filter}
            order by j.team_id, j.resource_id nulls last, j.actual_start_date, j.plan_start_date,  a.geocode_lat, a.geocode_long
        """).bindparams(**bind_params)
      result = await db.execute(smtp)
      rows = result.mappings().all()

      res = [dict(row) for row in rows]
      return res
        
    except Exception as e:
        logger.error(f"[getOpenJobs] Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")


@app.post("/schedulejobs")
async def scheduleJobs(
    body: schemas.ScheduleJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    resourceId = body.resource_id
    simulationId = body.simulation_id
    p_date = body.p_date
    listJobs = body.jobs
    action = body.action
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]

    logger.info(f"""Parâmetros: action - {action} resource_id - {resourceId}, simulation_id - {simulationId}, jobs - ({listJobs}),  p_date - {p_date}""")
    jobExists = True
    if action == 'I':
      logger.info("Criando os jobs da simulação ...")
      smtp = text(f"""
        MERGE INTO simulation_jobs u
            USING (SELECT client_id
                          ,job_id
                          ,client_job_id
                          ,team_id
                          ,job_status_id
                          ,job_type_id
                          ,address_id
                          ,place_id
                      FROM jobs
                    WHERE client_id = :client_id
                      AND job_id in ({','.join(str(j) for j in listJobs)})) t
                ON (    u.client_id = t.client_id
                    AND u.job_id = t.job_id
                    AND u.simulation_id = :simulation_id)
        WHEN NOT MATCHED
        THEN
          INSERT     (client_id
                    ,simulation_id
                    ,job_id
                    ,client_job_id
                    ,team_id
                    ,resource_id
                    ,job_status_id
                    ,job_type_id
                    ,address_id
                    ,place_id
                    ,created_by
                    ,created_date
                    ,modified_by
                    ,modified_date)
            VALUES (t.client_id
                  , :simulation_id
                  ,t.job_id
                  ,t.client_job_id
                  ,t.team_id
                  , :resource_id
                  ,t.job_status_id
                  ,t.job_type_id
                  ,t.address_id
                  ,t.place_id
                  , :user_name
                  ,now ()
                  , :user_name
                  ,now ());
      """).bindparams(client_id=clientId, user_name=userName, resource_id=resourceId, simulation_id = simulationId)
      
      result = await db.execute(smtp)
    if action == 'D':
       smtp = text(f"""
        DELETE FROM simulation_jobs
          WHERE client_id = :client_id
            AND simulation_id = :simulation_id
            AND job_id in ({','.join(str(j) for j in listJobs)})
        """).bindparams(client_id=clientId, simulation_id = simulationId)
       result = await db.execute(smtp)
       await db.commit()
       jobExists = await db.scalar(select(
            exists().where(
                models.SimulationJobs.client_id == clientId,
                models.SimulationJobs.simulation_id  == simulationId,
            )
        ))
       if not jobExists:
          return []

       
    # await db.commit()
    
    
    logger.info("Criando os resources da simulação ...")
    smtp = text(f"""
        MERGE INTO simulation_resources u
            USING (SELECT client_id, resource_id
                      FROM resources
                    WHERE     client_id = :client_id
                          AND resource_id = :resource_id) t
                ON (    u.client_id = t.client_id
                    AND u.resource_id = t.resource_id
                    AND u.simulation_id = :simulation_id)
        WHEN NOT MATCHED
        THEN
          INSERT     (client_id
                    ,simulation_id
                    ,resource_id
                    ,created_by
                    ,created_date
                    ,modified_by
                    ,modified_date)
            VALUES (t.client_id
                  , :simulation_id
                  ,t.resource_id
                  , :user_name
                  ,now ()
                  , :user_name
                  ,now ());
    """).bindparams(client_id=clientId, user_name=userName, resource_id=resourceId, simulation_id = simulationId)
    result = await db.execute(smtp)
    # await db.commit()
    
    logger.info("Iniciado Calculo das rotas ...")
    smtp = text(f"""
          WITH q1 AS (
              SELECT j.client_id
                    ,j.job_id
                    ,r.resource_id
                    ,r.time_overlap
                    ,j.address_id
                    ,a.geocode_lat::NUMERIC geocode_lat
                    ,a.geocode_long::NUMERIC geocode_long
                    ,COALESCE (j.time_setup, jt.time_setup, r.time_setup, t.time_setup)                          AS time_setup
                    ,COALESCE (j.time_service, jt.time_service, r.time_service, t.time_service)                    AS time_service
                    ,COALESCE (jt.priority, 0)                                                     AS priority
                    --,EXTRACT (epoch FROM COALESCE (aw.start_time, rw.start_time))::INTEGER AS start_time
                    0 AS start_time,
                    ,EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59.9999' AS TIME)))::INTEGER AS end_time
                FROM jobs  j
                    JOIN simulation_jobs sj ON sj.client_id = j.client_id AND sj.job_id = j.job_id
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                    JOIN resources r ON r.client_id = j.client_id AND r.resource_id = j.resource_id
                    JOIN resource_windows rw
                      ON     rw.client_id = r.client_id
                          AND rw.resource_id = r.resource_id
                          AND rw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
                    LEFT JOIN address_windows aw
                      ON     aw.client_id = j.client_id
                          AND aw.address_id = j.address_id
                          AND aw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
              WHERE     sj.client_id = :client_id
                    AND sj.simulation_id = :simulation_id
                    AND r.resource_id = :resource_id
          ),
          MapeamentoEnderecos AS (
              SELECT 
                  q1.*,
                  COUNT(job_id) OVER (
                      PARTITION BY client_id, geocode_lat, geocode_long
                  ) AS quantidade_jobs_mesmo_endereco,
                  ROW_NUMBER() OVER (
                      PARTITION BY client_id, geocode_lat, geocode_long
                      ORDER BY job_id ASC
                  ) AS ordem_job
              FROM q1),
            jobs_data AS (
            SELECT 
                client_id,
                job_id,
                resource_id,
                address_id,
                geocode_lat,
                geocode_long,
                time_setup,
                time_service + (time_overlap * (quantidade_jobs_mesmo_endereco -1)) as time_service,
                priority,
                start_time,
                end_time
            FROM MapeamentoEnderecos
            WHERE ordem_job = 1),
              vehicles_data AS (
                select 
                  r.resource_id,
                  r.description,
                  r.geocode_lat_from::NUMERIC,
                  r.geocode_long_from::NUMERIC,
                  r.geocode_lat_at::NUMERIC,
                  r.geocode_long_at::NUMERIC, 
                  EXTRACT(EPOCH FROM
                  CASE 
                    WHEN CAST(:p_date as date) <= NOW() THEN
                      CASE 
                          WHEN COALESCE(rw.start_time, cast('00:00:00.9999' as time)) < NOW()::TIME 
                          THEN 
                              NOW()::TIME
                          ELSE
                          COALESCE(rw.start_time, cast('00:00:00' as time))
                      END
                    ELSE
                      COALESCE(rw.start_time, cast('00:00:00' as time))
                  END) ::INTEGER AS start_time,
                  EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then rw.end_time else cast('23:59:59.9999' as time) end) ::INTEGER AS end_time
                from resources r
                  join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM CAST('{p_date}' AS DATE)) + 1
                  where r.client_id = :client_id
                    and r.resource_id = :resource_id
              ),
              vehicles_json AS (
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', resource_id,
                          'description', description,
                          'start', json_build_array(geocode_long_from, geocode_lat_from),
                          'end', json_build_array(geocode_long_at, geocode_lat_at),
                          'time_window', json_build_array(start_time, end_time)
                      ))
                  ) AS array_vehicles
                  FROM vehicles_data
              ),
              jobs_json AS (
                  -- Agrupa todos os jobs em um array JSON
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', job_id,
                          'location', json_build_array(geocode_long, geocode_lat),
                          'setup', time_setup,
                          'service', time_service,
                          'time_windows', json_build_array(json_build_array(start_time, end_time)) 
                      ))
                  ) AS array_jobs
                  FROM jobs_data
              )
              SELECT json_build_object(
                  'vehicles', (SELECT array_vehicles FROM vehicles_json),
                  'jobs', (SELECT array_jobs FROM jobs_json)
              ) AS vroom_payload;
    """).bindparams(client_id=clientId, simulation_id=simulationId, p_date = p_date, resource_id = resourceId)

    result = await db.execute(smtp)
    rows = result.mappings().all()
    if rows and len(rows) > 0:
      vroom_payload = rows[0]['vroom_payload']
      logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
      print(vroom_payload)
      retorno = await optimize_routes_vroom(vroom_payload)
      listIds = [item['id'] for item in retorno.get("unassigned", [])]
      if listIds:
        smtp = text(f"""
          DELETE FROM simulation_jobs
            WHERE client_id = :client_id
              AND simulation_id = :simulation_id
              AND job_id in ({','.join(str(j) for j in listIds)})
          """).bindparams(client_id=clientId, simulation_id = simulationId)
        result = await db.execute(smtp)
        await db.commit()
        jobExists = await db.scalar(select(
              exists().where(
                  models.SimulationJobs.client_id == clientId,
                  models.SimulationJobs.simulation_id  == simulationId,
              )
          ))
        if not jobExists:
            return []

    steps = retorno['routes'][0]['steps']
    # somente_jobs = [step for step in steps if step.get('type') == 'job']
    somente_jobs = [step for step in steps]
    jsonJobs = json.dumps(somente_jobs)
    # print(jsonJobs)
    smtp = text(f""" 
          with qjson as (
          SELECT 
              id AS job_id,
              type,
              (location->>0)::NUMERIC AS geocode_long,
              (location->>1)::NUMERIC AS geocode_lat,
              arrival,
              duration,
              setup,
              service,
              waiting_time
          FROM 
              jsonb_to_recordset('{jsonJobs}') AS x(
                  id int,
                  type text,
                  location jsonb,
                  arrival int,
                  duration int,
                  setup int,
                  service int,
                  waiting_time int
              )),
          q1 AS (
              SELECT * FROM qjson
                WHERE type = 'job'),
          q2 as (
            select arrival from qjson
              where type = 'start'),
          q3 as (
            select arrival from qjson
              where type = 'end')
          SELECT   j.client_id,
                  j.job_id,
                  r.time_overlap,
                  a.geocode_lat::NUMERIC AS geocode_lat,
                  a.geocode_long::NUMERIC AS geocode_long,
                  r.geocode_lat_from,
                  r.geocode_long_from,
                  r.geocode_lat_at,
                  r.geocode_long_at,
                  COALESCE(j.time_service,jt.time_service,t.time_service) AS time_service,
                  q1.arrival,
                  q2.arrival AS arrival_start,
                  q3.arrival AS arrival_end,
                  q1.duration,
                  q1.setup,
                  q1.service,
                  q1.waiting_time
              FROM jobs j
                  JOIN simulation_jobs sj ON sj.client_id = j.client_id AND sj.job_id = j.job_id
                  JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                  JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                  JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                  JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                  JOIN resources r ON r.client_id = j.client_id AND r.resource_id = j.resource_id
                  JOIN q1 on q1.geocode_lat = a.geocode_lat::NUMERIC and q1.geocode_long = a.geocode_long::NUMERIC
                  CROSS JOIN q2
                  CROSS JOIN q3
              WHERE sj.client_id = :client_id
                  AND sj.simulation_id = :simulation_id
                  and r.resource_id = :resource_id
                order by q1.arrival, j.plan_start_date
        """).bindparams(client_id=clientId, simulation_id=simulationId, resource_id = resourceId)
    result = await db.execute(smtp)
    rows = result.mappings().all()
    rId = None
    geocode_long_at = None
    geocode_lat_at = None
    geo = []
    arrivalStart = None
    arrivalEnd = None
    for row in rows:
      if rId is None:
        geo.append([row.geocode_long_from,row.geocode_lat_from])
        geocode_long_at = row.geocode_long_at
        geocode_lat_at = row.geocode_lat_at
        arrivalStart = row.arrival_start
        arrivalEnd = row.arrival_end
        rId = 1
      geo.append([row.geocode_long,row.geocode_lat])
    geo.append([geocode_long_at,geocode_lat_at])
    geoResult = await get_route_distance_block(geo)
    simulatedWindowEndDate = None
    simulatedWindowDistanceEnd = 0
    simulatedWindowTimeDistanceEnd = 0
    vArrivalBefore = None
    totalOverlap = 0
    rOrder = 1
    distance = 0
    timeDistance = 0
    for row in rows:
      jobId = row.job_id
      vArrival = row.arrival
      vSetup = row.setup
      vService = row.time_service
      logger.info(f"Otimizando recurso {resourceId} - Job {jobId} , arrival: {vArrival} - arrival before: {vArrivalBefore} - Overlap - {row.time_overlap} total overlap {totalOverlap}")
      if vArrivalBefore == vArrival:
        totalOverlap += row.time_overlap
        vArrival = vArrival + totalOverlap
      else:
        vArrivalBefore = vArrival
        totalOverlap = 0

      vDtStart = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival)
      vDtEnd = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival + vSetup + vService)

      distance = round(geoResult[(rOrder - 1)]["distance"])
      timeDistance = round(geoResult[(rOrder - 1)]["duration"]) 

      await db.execute(
          update(models.SimulationJobs)
          .where(
              models.SimulationJobs.client_id == clientId,
              models.SimulationJobs.simulation_id == simulationId,
              models.SimulationJobs.job_id == jobId,
          )
          .values(
              simulated_window_start_date=vDtStart,
              simulated_window_end_date=vDtEnd,
              simulated_window_distance=distance,
              simulated_window_time_distance=timeDistance,
              simulated_window_time_setup=vSetup,
              simulated_window_order = rOrder,
              modified_by=userName,
              modified_date=datetime.now(),
          )
      )
      # await db.commit()
      rOrder += 1
      simulatedWindowEndDate = vDtEnd
        

      logger.info(f'Calculando distância Simulada Window Final ...')

      simulatedWindowDistanceEnd = round(geoResult[(rOrder-1)]["distance"])
      simulatedWindowTimeDistanceEnd = round(geoResult[(rOrder-1)]["duration"])
      simulatedWindowDistanceStart = round(geoResult[(0)]["distance"])
      simulatedWindowTimeDistanceStart = round(geoResult[(0)]["duration"])
     
      todayZero = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
      simulatedWindowEndDate = todayZero + timedelta(seconds=arrivalEnd)
      simulatedWindowStartDate = todayZero + timedelta(seconds=arrivalStart)

      await db.execute(
          update(models.SimulationResources)
          .where(
              models.SimulationResources.client_id == clientId,
              models.SimulationResources.simulation_id == simulationId,
              models.SimulationResources.resource_id == resourceId
          )
          .values(
              simulated_window_distance_start = simulatedWindowDistanceStart,
              simulated_window_start_date = simulatedWindowStartDate,
              simulated_window_time_distance_start = simulatedWindowTimeDistanceStart,
              simulated_window_distance_end = simulatedWindowDistanceEnd,
              simulated_window_end_date = simulatedWindowEndDate,
              simulated_window_time_distance_end = simulatedWindowTimeDistanceEnd,
              modified_by=userName,
              modified_date=datetime.now(),
          )
      )
      await db.commit()   

    smtp = text(f"""
        with q1 as(
            select 
              'SIMULATED' as type,
              j.job_id,
              j.client_job_id,
              j.team_id, 
              j.resource_id, 
              r.client_resource_id,
              sr.simulated_window_distance_end as resource_simulated_window_distance_end,
              sr.simulated_window_end_date as resource_simulated_window_end_date,
              sr.simulated_window_time_distance_end as resource_simulated_window_time_distance_end,
              sr.simulated_window_distance_start as resource_simulated_window_distance_start,
              sr.simulated_window_start_date as resource_simulated_window_start_date,
              sr.simulated_window_time_distance_start as resource_simulated_window_time_distance_start,
              j.simulated_window_order,
              j.job_status_id, 
              js.description AS status_description,
              js.internal_code_status,
              js.style_id,
              j.job_type_id, 
              jt.description as type_description,
              j.address_id, 
              a.client_address_id, 
              a.geocode_lat, 
              a.geocode_long, 
              a.address, 
              a.city, 
              a.state_prov, 
              a.zippost, 
              p.trade_name, 
              p.cnpj,
              j.simulated_window_time_setup,
              j.simulated_window_start_date,
              j.simulated_window_end_date,
              j.simulated_window_distance,
              j.simulated_window_time_distance
              from simulation_jobs j
              JOIN simulation_resources sr on sr.client_id = j.client_id and sr.simulation_id = j.simulation_id and sr.resource_id = j.resource_id
              join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
              join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
              join teams t on t.client_id = j.client_id and t.team_id = j.team_id
              join address a on a.client_id = j.client_id and a.address_id = j.address_id
              join places p on p.client_id = j.client_id and p.place_id = j.place_id
              join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
              where j.client_id = :client_id
                and j.simulation_id = :simulation_id
                and j.resource_id = :resource_id )
        select
            q1.team_id,
            q1.resource_id,
            q1.resource_simulated_window_distance_end,
            q1.resource_simulated_window_end_date,
            q1.resource_simulated_window_time_distance_end,
            q1.resource_simulated_window_distance_start,
            q1.resource_simulated_window_start_date,
            q1.resource_simulated_window_time_distance_start,
            jsonb_agg(to_jsonb(q1) ORDER BY q1.simulated_window_order) AS resources
          from q1
          group by q1.team_id,
            q1.resource_id,
            q1.resource_simulated_window_distance_end,
            q1.resource_simulated_window_end_date,
            q1.resource_simulated_window_time_distance_end,
            q1.resource_simulated_window_distance_start,
            q1.resource_simulated_window_start_date,
            q1.resource_simulated_window_time_distance_start
            
      """).bindparams(client_id=clientId, simulation_id = simulationId, resource_id = resourceId)
    logger.info('Iniciou terceira consulta...')             
    result = await db.execute(smtp)
    rows = result.mappings().all()

    res = [dict(row) for row in rows]
    logger.info('Finalizou terceira consulta...')             
    return res       
    # return {"status": "OK", "resource_id": body.resource_id, "simulation_id": body.simulation_id, "jobs_received": len(body.jobs)}


@app.post("/newroutes")
async def createNewRoutes(
    body: schemas.NewRoutesRequest,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Cria uma nova simulação por agendamento (fl_calc_arround=1).
    Recebe a data e o resource_id, persiste um registro em Simulation
    e retorna os dados criados para habilitar a sessão de agendamento no front-end.
    """
    userId   = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]

    p_date      = body.p_date
    resource_id = body.resource_id

    try:
        date.fromisoformat(p_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Data inválida. Use o formato YYYY-MM-DD.")

    try:
        # Determina o próximo número de sequência para este usuário/cliente
        rLast = await db.execute(
            select(models.Simulation)
            .where(
                models.Simulation.client_id == clientId,
                models.Simulation.user_id   == userId,
            )
            .order_by(models.Simulation.sequence.desc())
        )
        lastSimulation = rLast.scalars().first()
        nextSequence   = (lastSimulation.sequence + 1) if lastSimulation else 1

        now = datetime.now()

        newSimulation = models.Simulation(
            client_id       = clientId,
            user_id         = userId,
            simulation_date = date.fromisoformat(p_date),
            sequence        = nextSequence,
            fl_calc_arround = 1,
            created_by      = userName,
            created_date    = now,
            modified_by     = userName,
            modified_date   = now,
        )
        db.add(newSimulation)
        await db.commit()
        await db.refresh(newSimulation)

        logger.info(
            "[newroutes] Simulação criada | client={} user={} sim_id={} seq={} date={} resource={}",
            clientId, userId, newSimulation.simulation_id, nextSequence, p_date, resource_id,
        )

        return {
            "simulation_id":   newSimulation.simulation_id,
            "uid":             str(newSimulation.uid),
            "client_id":       newSimulation.client_id,
            "user_id":         newSimulation.user_id,
            "simulation_date": str(newSimulation.simulation_date),
            "sequence":        newSimulation.sequence,
            "fl_calc_arround": newSimulation.fl_calc_arround,
            "resource_id":     resource_id,
            "created_date":    newSimulation.created_date.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[newroutes] Erro ao criar simulação: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao criar simulação.")


@app.get("/simulationjobs")
async def getSimulationJobs(
    p_date: str = Query(..., description="Data no formato YYYY-MM-DD"),
    simulate_board: Optional[bool] = Query(None, description="Simular quadro de tarefas para a data especificada"),
    simulate_plan_date: Optional[bool] = Query(None, description="Simular data de plano para a data especificada"),
    resources: Optional[str] = Query(None, description="Simular um recurso para a data especificada"),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    
    if not resources:
      raise HTTPException(status_code=404, detail="Recursos não fornecido.")

    listResources = [int(x) for x in resources.split(',') if x]

    if simulate_board and simulate_plan_date:
      raise HTTPException(status_code=400, detail="Só pode ser feito um tipo de simulação por vez: quadro de tarefas (simulate_board) ou data de plano (simulate_plan_date).")

    if simulate_plan_date and not simulate_board:
      raise HTTPException(status_code=400, detail="Simulação por data de planejamento ainda não implementada.")
      
    if simulate_board:
      rSimulation = await db.execute(select(models.Simulation).where(models.Simulation.client_id == clientId, models.Simulation.user_id == userId).order_by(models.Simulation.sequence.desc()))
      simulationDb = rSimulation.scalars().first()
      print("Simulando data de plano para: ", p_date)
      newSimulation = models.Simulation(
                client_id = clientId,
                user_id = userId,
                sequence = (simulationDb.sequence + 1) if simulationDb else 1,
                simulation_date = date.fromisoformat(p_date),
                fl_calc_board = 1,
                created_by = userName,
                created_date = datetime.now(),
                modified_by = userName,
                modified_date = datetime.now()
            )
      db.add(newSimulation)
      await db.commit()

      await db.refresh(newSimulation) 
      
      simulationId = newSimulation.simulation_id

      for resourceId in listResources:
        smtp = text(f"""
            select 
              j.client_id,
              j.job_id,
              j.client_job_id,
              j.team_id, 
              j.resource_id,
              j.job_status_id,
              j.job_type_id,
              j.address_id,
              a.geocode_lat,
              a.geocode_long,
              r.geocode_lat_from,
              r.geocode_long_from,
              r.geocode_lat_at,
              r.geocode_long_at,
              j.place_id,
              j.time_setup,
              j.time_service,
              j.time_limit_start,
              j.time_limit_end,
              j.work_duration,
              EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date) )::INTEGER AS actual_time_service,
              j.actual_start_date,
              j.actual_end_date  
              from jobs j
              join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
              join teams t on t.client_id = j.client_id and t.team_id = j.team_id
              join address a on a.client_id = j.client_id and a.address_id = j.address_id
              join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
              join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
              where ut.client_id = :client_id
                and ut.user_id = :user_id
                and j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day'
              and j.resource_id = :resource_id
              and js.internal_code_status = 'CONCLU'
            order by j.team_id, j.resource_id, j.actual_start_date
        """).bindparams(client_id=clientId, user_id=userId, p_date=p_date, resource_id = resourceId)
        logger.info('Iniciou primeira consulta...')
        result = await db.execute(smtp)
        rows = result.mappings().all()
        if not rows:
          raise HTTPException(status_code=400, detail="Não há trabalho com status de concluido neste dia.")  
        rId = None
        geocode_long_at = None
        geocode_lat_at = None
        geo = []
        for row in rows:
          if rId is None:
            geo.append([row.geocode_long_from,row.geocode_lat_from])
            geocode_long_at = row.geocode_long_at
            geocode_lat_at = row.geocode_lat_at
            rId = 1
          geo.append([row.geocode_long,row.geocode_lat])

        geo.append([geocode_long_at,geocode_lat_at])
        logger.info('Pegou rotas...')
        geoResult = await get_route_distance_block(geo)
        logger.info('Finalizou rotas...')

        rOrder = 1
        actualEndDate = None
        actualDistanceEnd = 0
        actualTimeDistanceEnd = 0
        distance = 0
        timeDistance = 0
        for row in rows:
          print(rOrder)
          distance = round(geoResult[(rOrder - 1)]["distance"])
          timeDistance = round(geoResult[(rOrder - 1)]["duration"])
          logger.info(f'Calculando distância atual ... {row.job_id} - {distance} - {timeDistance}')

          # insere on banco e muda a data Start
          newSimulationJobs = models.SimulationJobs(
                client_id = clientId,
                simulation_id = simulationId,
                job_id = row.job_id,
                client_job_id = row.client_job_id,
                team_id  = row.team_id,
                resource_id  = row.resource_id,
                job_status_id  = row.job_status_id,
                job_type_id  = row.job_type_id,
                address_id = row.address_id,
                place_id  = row.place_id,
                time_setup = row.time_setup,
                actual_time_service = row.time_service,
                actual_work_duration = row.work_duration,
                actual_start_date = row.actual_start_date,
                actual_end_date = row.actual_end_date,
                time_limit_start = row.time_limit_start,
                time_limit_end = row.time_limit_end,
                actual_order = rOrder,
                actual_distance = distance,
                actual_time_distance = timeDistance,
                created_by = userName,
                created_date = datetime.now(),
                modified_by = userName,
                modified_date = datetime.now()
          )
          db.add(newSimulationJobs)
          await db.commit()
          rOrder += 1
          actualEndDate = row.actual_end_date

        ## criar o simulation_resources  
        
        logger.info(f'Calculando distância atual Final ... ')

        actualDistanceEnd = round(geoResult[(rOrder-1)]["distance"])
        actualTimeDistanceEnd = round(geoResult[(rOrder-1)]["duration"])
        actualEndDate = actualEndDate + timedelta(seconds=actualTimeDistanceEnd)

        newSimulationResource = models.SimulationResources(
                client_id = clientId,
                resource_id = resourceId,
                simulation_id = simulationId,
                actual_end_date = actualEndDate,
                actual_distance_end = actualDistanceEnd,
                actual_time_distance_end = actualTimeDistanceEnd,
                created_by = userName,
                created_date = datetime.now(),
                modified_by = userName,
                modified_date = datetime.now()
            )
        db.add(newSimulationResource)
        await db.commit()
        
        ## Após inserir os dados de simulação, podemos retornar os jobs simulados para o front-end ou processá-los conforme necessário. 

        smtp = text(f"""
          WITH jobs_data AS (
                select
                  j.client_id,
                  r.resource_id,
                  j.job_id,
                  a.geocode_lat::NUMERIC,
                  a.geocode_long::NUMERIC,
                  COALESCE(j.time_setup,jt.time_setup,t.time_setup) AS time_setup,
                  r.time_setup AS time_setup_resource,
                  EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date) )::INTEGER AS time_service,
                  r.time_service AS time_service_resource,
                  COALESCE(jt.priority,0) AS priority,
                  
                  EXTRACT(EPOCH FROM COALESCE(aw.start_time,rw.start_time)) ::INTEGER AS start_time,
                  EXTRACT(EPOCH FROM COALESCE(aw.end_time,rw.end_time)) ::INTEGER AS end_time,
                  EXTRACT(EPOCH FROM COALESCE(j.time_limit_start, j.created_date)) ::INTEGER AS job_window_time_start,
                  EXTRACT(EPOCH FROM time_limit_end) ::INTEGER AS job_window_time_end
                from jobs j
                  join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
                  join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
                  join address a on a.client_id = j.client_id and a.address_id = j.address_id
                  join teams t on t.client_id = j.client_id and t.team_id = j.team_id
                  join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
                  join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
                  join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW())) + 1
                  left join address_windows aw on aw.client_id = j.client_id and aw.address_id = j.address_id and aw.week_day = EXTRACT(DOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW())) + 1
                where ut.client_id = :client_id
                  and ut.user_id = :user_id
                  and r.resource_id = :resource_id
                  and js.internal_code_status = 'CONCLU'
                  and j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day'
                  /*(
                        (j.actual_start_date is null and j.plan_start_date >= cast(:p_date as date) and j.plan_start_date < cast(:p_date as date) + interval '1 day')
                      or (
                        j.actual_start_date is not null AND (j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day')
                        )
                      )*/
              ),
              vehicles_data AS (
                select 
                  r.resource_id,
                  r.description,
                  r.geocode_lat_from::NUMERIC,
                  r.geocode_long_from::NUMERIC,
                  r.geocode_lat_at::NUMERIC,
                  r.geocode_long_at::NUMERIC, 
                  EXTRACT(EPOCH FROM rw.start_time) ::INTEGER AS start_time,
                  EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then rw.end_time else cast('23:59:59.9999' as time) end ) ::INTEGER AS end_time
                from resources r
                  join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM CAST(:p_date AS DATE)) + 1
                  where exists (select 1 from jobs_data j
                                where j.client_id = r.client_id
                                  and j.resource_id = r.resource_id)
              ),
              vehicles_json AS (
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', resource_id,
                          'description', description,
                          'start', json_build_array(geocode_long_from, geocode_lat_from),
                          'end', json_build_array(geocode_long_at, geocode_lat_at),
                          'time_window', json_build_array(start_time, end_time)
                      ))
                  ) AS array_vehicles
                  FROM vehicles_data
              ),
              jobs_json AS (
                  -- Agrupa todos os jobs em um array JSON
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', job_id,
                          'location', json_build_array(geocode_long, geocode_lat),
                          'setup', time_setup,
                          'setup_per_type', time_setup_resource,
                          'service', time_service,
                          'service_per_type', time_service_resource,
                          'time_windows', json_build_array(json_build_array(start_time, end_time)) 
                      ))
                  ) AS array_jobs
                  FROM jobs_data
              )
              -- Envelopa tudo no objeto raiz
              SELECT json_build_object(
                  'vehicles', (SELECT array_vehicles FROM vehicles_json),
                  'jobs', (SELECT array_jobs FROM jobs_json)
              ) AS vroom_payload;
        """).bindparams(client_id=clientId, user_id=userId, p_date=p_date, resource_id = resourceId)
        logger.info('Primeira consulta...')
        result = await db.execute(smtp)
        rows = result.mappings().all()
        print(rows)
        if rows and len(rows) > 0:
          vroom_payload = rows[0]['vroom_payload']
          logger.info('Otimizando rotas Simulação Janela atual ...')
          retorno = await optimize_routes_vroom(vroom_payload)
  
          simulatedEndDate = None
          simulatedDistanceEnd = 0
          simulatedTimeDistanceEnd = 0
          rOrder = 1
          distance = 0
          timeDistance = 0
          for route in retorno.get("routes", []):
              geo = []
              for step in route.get("steps", []):
                geo.append([step.get("location", [None, None])[0],step.get("location", [None, None])[1]])
              
              logger.info('Segunda Pegou rotas...')
              geoResult = await get_route_distance_block(geo)
              logger.info('Segunda Finalizou rotas...')
              for step in route.get("steps", []):
                  if step.get("type") == "job":
                    jobId = int(step['id'])
                        
                    logger.info(f"Otimizando recurso {resourceId} - Job {step['id']} (tipo: {step.get('type')}, tempo de serviço: {step.get('service')})")
                    vArrival = int(step.get('arrival'))
                    vSetup = int(step.get('setup'))
                    vService = int(step.get('service'))
                    vDuration = int(step.get('duration'))
                    geocode_long = step.get("location", [None, None])[0]
                    geocode_lat = step.get("location", [None, None])[1]
                    vDtStart = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival)
                    vDtEnd = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival + vSetup + vService)

                    distance = round(geoResult[(rOrder - 1)]["distance"])
                    timeDistance = round(geoResult[(rOrder - 1)]["duration"])

                    await db.execute(
                        update(models.SimulationJobs)
                        .where(
                            models.SimulationJobs.client_id == clientId,
                            models.SimulationJobs.simulation_id == simulationId,
                            models.SimulationJobs.job_id == jobId,
                        )
                        .values(
                            simulated_start_date=vDtStart,
                            simulated_end_date=vDtEnd,
                            simulated_order = rOrder,
                            simulated_distance=distance,
                            simulated_time_distance=timeDistance,
                            modified_by=userName,
                            modified_date=datetime.now(),
                        )
                    )
                    await db.commit()
         
                    rOrder += 1
                    simulatedEndDate = vDtEnd
        
        
        logger.info(f'Calculando distância Simulação Final ...')
        simulatedDistanceEnd = round(geoResult[(rOrder-1)]["distance"])
        simulatedTimeDistanceEnd = round(geoResult[(rOrder-1)]["duration"])
        
        simulatedEndDate = simulatedEndDate + timedelta(seconds=simulatedTimeDistanceEnd)

        await db.execute(
            update(models.SimulationResources)
            .where(
                models.SimulationResources.client_id == clientId,
                models.SimulationResources.simulation_id == simulationId,
                models.SimulationResources.resource_id == resourceId
            )
            .values(
                simulated_distance_end = simulatedDistanceEnd,
                simulated_end_date = simulatedEndDate,
                simulated_time_distance_end = simulatedTimeDistanceEnd,
                modified_by=userName,
                modified_date=datetime.now(),
            )
        )
        await db.commit()

        logger.info('Finalizado Otimização das rotas Simulação Janela atual ...')

        ## Após inserir os dados de simulação, podemos retornar os jobs simulados para o front-end ou processá-los conforme necessário. 
        smtp = text(f"""
          WITH q1 AS (
              SELECT   j.client_id,
                      j.job_id,
                      r.resource_id,
                      r.time_overlap,
                      j.address_id,
                      a.geocode_lat::NUMERIC,
                      a.geocode_long::NUMERIC,
                      COALESCE(j.time_setup,jt.time_setup,t.time_setup) AS time_setup,
                      r.time_setup AS time_setup_resource,
                      COALESCE(j.time_service,jt.time_service,t.time_service) AS time_service,
                      r.time_service AS time_service_resource,
                      COALESCE(jt.priority,0) AS priority,
                      EXTRACT(EPOCH FROM COALESCE(aw.start_time,rw.start_time)) ::INTEGER AS start_time,
                      EXTRACT(EPOCH FROM COALESCE(aw.end_time,cast('23:59:59.9999' as time) )) ::INTEGER AS end_time
                      
              FROM jobs j
                  JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                  JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                  JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                  JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                  JOIN user_team ut ON ut.client_id = t.client_id AND ut.team_id = t.team_id
                  JOIN resources r ON j.client_id = r.client_id AND j.resource_id = r.resource_id
                  JOIN resource_windows rw ON rw.client_id = r.client_id AND rw.resource_id = r.resource_id AND rw.week_day = EXTRACT(DOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW())) + 1
                  LEFT JOIN address_windows aw ON aw.client_id = j.client_id AND aw.address_id = j.address_id AND aw.week_day = EXTRACT(DOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW())) + 1
              WHERE ut.client_id = :client_id
                  AND ut.user_id = :user_id
                  AND r.resource_id = :resource_id
                  AND js.internal_code_status = 'CONCLU'
                  AND j.actual_start_date >= CAST(:p_date AS date) AND j.actual_start_date < CAST(:p_date AS date) + INTERVAL '1 day'
          ),
          MapeamentoEnderecos AS (
              SELECT 
                  *,
                  -- 1. Conta o total de jobs para o mesmo address_id (e client_id, por segurança)
                  COUNT(job_id) OVER (
                      PARTITION BY client_id, geocode_lat, geocode_long
                  ) AS quantidade_jobs_mesmo_endereco,
                  
                  -- 2. Cria um ranking ordenando pelo menor job_id
                  ROW_NUMBER() OVER (
                      PARTITION BY client_id, geocode_lat, geocode_long
                      ORDER BY job_id ASC
                  ) AS ordem_job
              FROM q1),
            jobs_data AS (
            SELECT 
                client_id,
                job_id,
                resource_id,
                address_id,
                geocode_lat,
                geocode_long,
                time_setup,
                time_setup_resource,
                time_service + (time_overlap * (quantidade_jobs_mesmo_endereco -1)) as time_service,
                time_service_resource,
                priority,
                start_time,
                end_time
            FROM MapeamentoEnderecos
            WHERE ordem_job = 1),
              vehicles_data AS (
                select 
                  r.resource_id,
                  r.description,
                  r.geocode_lat_from::NUMERIC,
                  r.geocode_long_from::NUMERIC,
                  r.geocode_lat_at::NUMERIC,
                  r.geocode_long_at::NUMERIC, 
                  EXTRACT(EPOCH FROM rw.start_time) ::INTEGER AS start_time,
                  EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then rw.end_time else cast('23:59:59.9999' as time) end) ::INTEGER AS end_time
                from resources r
                  join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM CAST(:p_date AS DATE)) + 1
                  where exists (select 1 from jobs_data j
                                where j.client_id = r.client_id
                                  and j.resource_id = r.resource_id)
              ),
              vehicles_json AS (
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', resource_id,
                          'description', description,
                          'start', json_build_array(geocode_long_from, geocode_lat_from),
                          'end', json_build_array(geocode_long_at, geocode_lat_at),
                          'time_window', json_build_array(start_time, end_time)
                      ))
                  ) AS array_vehicles
                  FROM vehicles_data
              ),
              jobs_json AS (
                  -- Agrupa todos os jobs em um array JSON
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', job_id,
                          'location', json_build_array(geocode_long, geocode_lat),
                          'setup', time_setup,
                          'setup_per_type', time_setup_resource,
                          'service', time_service,
                          'service_per_type', time_service_resource,
                          'time_windows', json_build_array(json_build_array(start_time, end_time)) 
                      ))
                  ) AS array_jobs
                  FROM jobs_data
              )
              -- Envelopa tudo no objeto raiz
              SELECT json_build_object(
                  'vehicles', (SELECT array_vehicles FROM vehicles_json),
                  'jobs', (SELECT array_jobs FROM jobs_json)
              ) AS vroom_payload;
        """).bindparams(client_id=clientId, user_id=userId, p_date=p_date, resource_id = resourceId)

        result = await db.execute(smtp)
        rows = result.mappings().all()
        if rows and len(rows) > 0:
          vroom_payload = rows[0]['vroom_payload']
          logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
          retorno = await optimize_routes_vroom(vroom_payload)
        steps = retorno['routes'][0]['steps']
        somente_jobs = [step for step in steps if step.get('type') == 'job']
        jsonJobs = json.dumps(somente_jobs)
        
        smtp = text(f""" 
          with q1 as (
          SELECT 
              id AS job_id,
              type,
              (location->>0)::NUMERIC AS geocode_long,
              (location->>1)::NUMERIC AS geocode_lat,
              arrival,
              duration,
              setup,
              service,
              waiting_time
          FROM 
              jsonb_to_recordset('{jsonJobs}') AS x(
                  id int,
                  type text,
                  location jsonb,
                  arrival int,
                  duration int,
                  setup int,
                  service int,
                  waiting_time int
              ))
          SELECT   j.client_id,
                  j.job_id,
                  r.time_overlap,
                  a.geocode_lat::NUMERIC AS geocode_lat,
                  a.geocode_long::NUMERIC AS geocode_long,
                  r.geocode_lat_from,
                  r.geocode_long_from,
                  r.geocode_lat_at,
                  r.geocode_long_at,
                  COALESCE(j.time_service,jt.time_service,t.time_service) AS time_service,
                  q1.arrival,
                  q1.duration,
                  q1.setup,
                  q1.service,
                  q1.waiting_time
              FROM jobs j
                  JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                  JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                  JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                  JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                  JOIN user_team ut ON ut.client_id = t.client_id AND ut.team_id = t.team_id
                  JOIN resources r ON j.client_id = r.client_id AND j.resource_id = r.resource_id
                  JOIN q1 on q1.geocode_lat = a.geocode_lat::NUMERIC and q1.geocode_long = a.geocode_long::NUMERIC
              WHERE ut.client_id = :client_id
                  AND ut.user_id = :user_id
                  AND r.resource_id = :resource_id
                  AND js.internal_code_status = 'CONCLU'
                  AND j.actual_start_date >= CAST(:p_date AS date) AND j.actual_start_date < CAST(:p_date AS date) + INTERVAL '1 day'
                order by q1.arrival, j.actual_start_date
        """).bindparams(client_id=clientId, user_id=userId, p_date=p_date, resource_id = resourceId)
        result = await db.execute(smtp)
        rows = result.mappings().all()
        rId = None
        geocode_long_at = None
        geocode_lat_at = None
        geo = []
        for row in rows:
          if rId is None:
            geo.append([row.geocode_long_from,row.geocode_lat_from])
            geocode_long_at = row.geocode_long_at
            geocode_lat_at = row.geocode_lat_at
            rId = 1
          geo.append([row.geocode_long,row.geocode_lat])
        geo.append([geocode_long_at,geocode_lat_at])
        geoResult = await get_route_distance_block(geo)
        simulatedWindowEndDate = None
        simulatedWindowDistanceEnd = 0
        simulatedWindowTimeDistanceEnd = 0
        vArrivalBefore = None
        totalOverlap = 0
        rOrder = 1
        distance = 0
        timeDistance = 0
        for row in rows:
          jobId = row.job_id
          vArrival = row.arrival
          vSetup = row.setup
          vService = row.time_service
          logger.info(f"Otimizando recurso {resourceId} - Job {jobId} , arrival: {vArrival} - arrival before: {vArrivalBefore} - Overlap - {row.time_overlap} total overlap {totalOverlap}")
          if vArrivalBefore == vArrival:
            totalOverlap += row.time_overlap
            vArrival = vArrival + totalOverlap
          else:
            vArrivalBefore = vArrival
            totalOverlap = 0

          vDtStart = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival)
          vDtEnd = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival + vSetup + vService)

          distance = round(geoResult[(rOrder - 1)]["distance"])
          timeDistance = round(geoResult[(rOrder - 1)]["duration"]) 

          await db.execute(
              update(models.SimulationJobs)
              .where(
                  models.SimulationJobs.client_id == clientId,
                  models.SimulationJobs.simulation_id == simulationId,
                  models.SimulationJobs.job_id == jobId,
              )
              .values(
                  simulated_window_start_date=vDtStart,
                  simulated_window_end_date=vDtEnd,
                  simulated_window_distance=distance,
                  simulated_window_time_distance=timeDistance,
                  simulated_window_order = rOrder,
                  modified_by=userName,
                  modified_date=datetime.now(),
              )
          )
          await db.commit()
          rOrder += 1
          simulatedWindowEndDate = vDtEnd
          

        logger.info(f'Calculando distância Simulada Window Final ...')

        simulatedWindowDistanceEnd = round(geoResult[(rOrder-1)]["distance"])
        simulatedWindowTimeDistanceEnd = round(geoResult[(rOrder-1)]["duration"])
        simulatedWindowEndDate = simulatedWindowEndDate + timedelta(seconds=simulatedWindowTimeDistanceEnd)

        await db.execute(
            update(models.SimulationResources)
            .where(
                models.SimulationResources.client_id == clientId,
                models.SimulationResources.simulation_id == simulationId,
                models.SimulationResources.resource_id == resourceId
            )
            .values(
                simulated_window_distance_end = simulatedWindowDistanceEnd,
                simulated_window_end_date = simulatedWindowEndDate,
                simulated_window_time_distance_end = simulatedWindowTimeDistanceEnd,
                modified_by=userName,
                modified_date=datetime.now(),
            )
        )
        await db.commit()    
        
        logger.info('Finalizado Otimização das rotas Simulação Janela Default ...')
      
      smtp = text(f"""
        with q1 as(
            select 
              'SIMULATED' as type,
              j.job_id,
              j.client_job_id,
              j.team_id, 
              j.resource_id, 
              r.client_resource_id,
              sr.actual_distance_end as resource_actual_distance_end,
              sr.simulated_distance_end as resource_simulated_distance_end,
              sr.simulated_window_distance_end as resource_simulated_window_distance_end,
              sr.actual_end_date as resource_actual_end_date,
              sr.simulated_end_date as resource_simulated_end_date,
              sr.simulated_window_end_date as resource_simulated_window_end_date,
              sr.actual_time_distance_end as resource_actual_time_distance_end,
              sr.simulated_time_distance_end as resource_simulated_time_distance_end,
              sr.simulated_window_time_distance_end as resource_simulated_window_time_distance_end,
              j.actual_order,
              j.simulated_order,
              j.simulated_window_order,
              j.job_status_id, 
              js.description AS status_description,
              js.internal_code_status,
              js.style_id,
              j.job_type_id, 
              jt.description as type_description,
              j.address_id, 
              a.client_address_id, 
              a.geocode_lat, 
              a.geocode_long, 
              a.address, 
              a.city, 
              a.state_prov, 
              a.zippost, 
              a.time_setup,
              p.trade_name, 
              p.cnpj,
              j.time_setup,
              j.actual_time_service as time_service, 
              j.actual_start_date AS plan_start_date, 
              j.actual_end_date AS plan_end_date, 
              j.actual_start_date, 
              j.actual_end_date, 
              j.actual_start_date as start_date,
              j.actual_end_date as end_date,
              j.time_limit_start, 
              j.time_limit_end,
              j.actual_time_service,
              j.actual_work_duration,
              j.simulated_work_duration,
              j.simulated_start_date,
              j.simulated_end_date,
              j.simulated_window_start_date,
              j.simulated_window_end_date,
              j.actual_distance,
              j.simulated_distance,
              j.actual_time_distance,
              j.simulated_time_distance,
              j.simulated_window_distance,
              j.simulated_window_time_distance
              from simulation_jobs j
              JOIN simulation_resources sr on sr.client_id = j.client_id and sr.simulation_id = j.simulation_id and sr.resource_id = j.resource_id
              join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
              join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
              join teams t on t.client_id = j.client_id and t.team_id = j.team_id
              join address a on a.client_id = j.client_id and a.address_id = j.address_id
              join places p on p.client_id = j.client_id and p.place_id = j.place_id
              join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
              where j.client_id = :client_id
                and j.simulation_id = :simulation_id )
        select 
            q1.team_id,
            q1.resource_id,
            q1.resource_actual_distance_end,
            q1.resource_simulated_distance_end,
            q1.resource_simulated_window_distance_end,
            q1.resource_actual_end_date,
            q1.resource_simulated_end_date,
            q1.resource_simulated_window_end_date,
            q1.resource_actual_time_distance_end,
            q1.resource_simulated_time_distance_end,
            q1.resource_simulated_window_time_distance_end,      
            jsonb_agg(to_jsonb(q1) ORDER BY q1.team_id,q1.actual_start_date) AS resources
          from q1                  
          group by q1.team_id,
            q1.resource_id,
            q1.resource_actual_distance_end,
            q1.resource_simulated_distance_end,
            q1.resource_simulated_window_distance_end,
            q1.resource_actual_end_date,
            q1.resource_simulated_end_date,
            q1.resource_simulated_window_end_date,
            q1.resource_actual_time_distance_end,
            q1.resource_simulated_time_distance_end,
            q1.resource_simulated_window_time_distance_end
            
      """).bindparams(client_id=clientId, simulation_id = simulationId)
      logger.info('Iniciou terceira consulta...')             
      result = await db.execute(smtp)
      rows = result.mappings().all()

      res = [dict(row) for row in rows]
      logger.info('Finalizou terceira consulta...')
      return res
      # return vroom_payload



# ROTAS DE RESOURCES

@app.patch("/resources/{resource_id}", response_model=schemas.ResourceUpdateResponse)
async def updateResource(
    resource_id: int,
    body: schemas.ResourceUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    client_id = current_user["clientId"]
    user_name = current_user["userName"]

    result = await db.execute(
        select(models.Resources).where(
            models.Resources.client_id == client_id,
            models.Resources.resource_id == resource_id
        )
    )
    resource = result.scalar_one_or_none()

    if not resource:
        raise HTTPException(status_code=404, detail="Resource não encontrado")

    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=422, detail="Nenhum campo para atualizar")

    for field, value in update_data.items():
        setattr(resource, field, value)

    resource.modified_by = user_name
    resource.modified_date = datetime.now(timezone.utc).replace(tzinfo=None)

    await db.commit()
    await db.refresh(resource)

    return resource

