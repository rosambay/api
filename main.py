import asyncio
from turtle import distance
import redis.asyncio as Redis
import database
import redis_client
import json
from datetime import date, datetime
import models, schemas
from auth import verify_password, create_access_token
from database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, update
from sqlalchemy.future import select
from fastapi import FastAPI, Depends, HTTPException, status,  Response, Request, Query, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import  List, Optional
from jose import JWTError, jwt
# from notification_service import criar_e_enviar_notificacao
from datetime import datetime, timedelta
from config import settings
from loguru import logger
import httpx

async def optimize_routes_vroom(payload: dict) -> dict:
    """
    Envia um payload no formato VROOM para o servidor configurado em VROOM_URL
    e retorna a solução otimizada.

    Estrutura esperada do payload:
    {
        "vehicles": [ { "id", "start", "end", "time_window" } ],
        "jobs":     [ { "id", "location", "service", "time_windows" } ]
    }
    """
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            settings.vroom_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()

    return response.json()


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
            models.UserTeam.client_id == client_id
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

    if simulate_board and simulate_plan_date:
      raise HTTPException(status_code=404, detail="Só pode ser feito um tipo de simulação por vez: quadro de tarefas (simulate_board) ou data de plano (simulate_plan_date).")
      
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

          # INSERT INTO simulation_jobs (
          #   client_id, 
          #   simulation_id, 
          #   job_id, 
          #   client_job_id, 
          #   team_id, 
          #   resource_id, 
          #   job_status_id, 
          #   job_type_id, 
          #   address_id, 
          #   place_id, 
          #   time_setup,
          #   default_time_service,
          #   actual_work_duration,
          #   actual_start_date,
          #   actual_end_date,
          #   created_by, 
          #   created_date, 
          #   modified_by, 
          #   modified_date)

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
            j.place_id,
            j.time_setup,
            j.time_service * 60 AS time_service,
            j.time_limit_start,
            j.time_limit_end,
            j.actual_work_duration * 60 as actual_work_duration,
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
            and j.resource_id in ({resources})
            and js.internal_code_status = 'CONCLU'
          order by j.team_id, j.resource_id, j.actual_start_date
      """).bindparams(client_id=clientId, user_id=userId, p_date=p_date)
      result = await db.execute(smtp)
      rows = result.mappings().all()

      rId = None
      rOrder = 1
      for row in rows:
          if rId != row.resource_id:
            rId = row.resource_id
            geocode_long_before = row.geocode_long_from
            geocode_lat_before = row.geocode_lat_from

          minhas_coordenadas = [
              [geocode_long_before, geocode_lat_before],
              [row.geocode_long, row.geocode_lat]
          ]
          retorno = await get_route_distance(minhas_coordenadas)
          distance = round(retorno['distance_meters'])
          timeDistance = round(retorno['distance_time'])      

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
                default_time_service = row.time_service,
                actual_time_service = row.actual_time_service,
                actual_work_duration = row.actual_work_duration,
                actual_start_date = row.actual_start_date,
                actual_end_date = row.actual_end_date,
                time_limit_start = row.time_limit_start,
                time_limit_end = row.time_limit_end,
                order = rOrder,
                actual_distance = distance,
                actual_time_distance = timeDistance,
                created_by = userName,
                created_date = datetime.now(),
                modified_by = userName,
                modified_date = datetime.now()
          )
          db.add(newSimulationJobs)
          await db.commit()
          geocode_long_before = row.geocode_long
          geocode_lat_before = row.geocode_lat
          rOrder = rOrder + 1

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
                --COALESCE(j.time_service,jt.time_service,t.time_service) * 60 AS time_service,
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
                join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(ISODOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW()))
                left join address_windows aw on aw.client_id = j.client_id and aw.address_id = j.address_id and aw.week_day = EXTRACT(ISODOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW()))
              where ut.client_id = :client_id
                and ut.user_id = :user_id
                and r.resource_id in ({resources})
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
                --EXTRACT(EPOCH FROM rw.end_time) ::INTEGER AS end_time
                EXTRACT(EPOCH FROM cast('23:59:59.9999' as time)) ::INTEGER AS end_time
              from resources r
                join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(ISODOW FROM CAST(:p_date AS DATE))
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
      """).bindparams(client_id=clientId, user_id=userId, p_date=p_date)

      result = await db.execute(smtp)
      rows = result.mappings().all()
      if rows and len(rows) > 0:
        vroom_payload = rows[0]['vroom_payload']
        retorno = await optimize_routes_vroom(vroom_payload)
        # print("Payload para VROOM: ", retorno)
        for route in retorno.get("routes", []):
            # print(f"Rota para veículo {route['vehicle']}:")
            for step in route.get("steps", []):
                if step.get("type") == "start":
                    geocode_long_before = step.get("location", [None, None])[0]
                    geocode_lat_before = step.get("location", [None, None])[1]
                if step.get("type") == "job":
                  print(f"  - Job {step['id']} (tipo: {step.get('type')}, tempo de serviço: {step.get('service')})")
                  jobId = int(step['id'])
                  vArrival = int(step.get('arrival'))
                  vSetup = int(step.get('setup'))
                  vService = int(step.get('service'))
                  vDuration = int(step.get('duration'))
                  geocode_long = step.get("location", [None, None])[0]
                  geocode_lat = step.get("location", [None, None])[1]
                  vDtStart = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival)
                  vDtEnd = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival + vSetup + vService)
                  print(f'Job {jobId}: Start: {vDtStart} - End: {vDtEnd} (Duration: {str(timedelta(seconds=vDuration))} segundos)')
                  minhas_coordenadas = [
                      [geocode_long_before, geocode_lat_before],
                      [geocode_long, geocode_lat]
                  ]
                  retorno = await get_route_distance(minhas_coordenadas)
                  distance = round(retorno['distance_meters'])
                  timeDistance = round(retorno['distance_time'])    

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
                          simulated_distance=distance,
                          simulated_time_distance=timeDistance,
                          modified_by=userName,
                          modified_date=datetime.now(),
                      )
                  )
                  await db.commit()

                  geocode_long_before = geocode_long
                  geocode_lat_before = geocode_lat

# Simulação com a tempo de serviço padrão
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
                COALESCE(j.time_service,jt.time_service,t.time_service) * 60 AS time_service,
                --EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date) )::INTEGER AS time_service,
                r.time_service AS time_service_resource,
                COALESCE(jt.priority,0) AS priority,
                
                EXTRACT(EPOCH FROM COALESCE(aw.start_time,rw.start_time)) ::INTEGER AS start_time,
                EXTRACT(EPOCH FROM cast('23:59:59.9999' as time)) ::INTEGER AS end_time,
                --EXTRACT(EPOCH FROM COALESCE(aw.end_time,rw.end_time)) ::INTEGER AS end_time,
                EXTRACT(EPOCH FROM COALESCE(j.time_limit_start, j.created_date)) ::INTEGER AS job_window_time_start,
                EXTRACT(EPOCH FROM time_limit_end) ::INTEGER AS job_window_time_end
              from jobs j
                join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
                join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
                join address a on a.client_id = j.client_id and a.address_id = j.address_id
                join teams t on t.client_id = j.client_id and t.team_id = j.team_id
                join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
                join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
                join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(ISODOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW()))
                left join address_windows aw on aw.client_id = j.client_id and aw.address_id = j.address_id and aw.week_day = EXTRACT(ISODOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW()))
              where ut.client_id = :client_id
                and ut.user_id = :user_id
                and r.resource_id in ({resources})
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
                --EXTRACT(EPOCH FROM rw.end_time) ::INTEGER AS end_time
                EXTRACT(EPOCH FROM cast('23:59:59.9999' as time)) ::INTEGER AS end_time
              from resources r
                join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(ISODOW FROM CAST(:p_date AS DATE))
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
      """).bindparams(client_id=clientId, user_id=userId, p_date=p_date)

      result = await db.execute(smtp)
      rows = result.mappings().all()
      if rows and len(rows) > 0:
        vroom_payload = rows[0]['vroom_payload']
        print("Retorno para VROOM: ", vroom_payload)
        retorno = await optimize_routes_vroom(vroom_payload)
        for route in retorno.get("routes", []):
            # print(f"Rota para veículo {route['vehicle']}:")
            for step in route.get("steps", []):
                if step.get("type") == "start":
                    geocode_long_before = step.get("location", [None, None])[0]
                    geocode_lat_before = step.get("location", [None, None])[1]
                if step.get("type") == "job":
                  print(f"  - Job {step['id']} (tipo: {step.get('type')}, tempo de serviço: {step.get('service')})")
                  jobId = int(step['id'])
                  vArrival = int(step.get('arrival'))
                  vSetup = int(step.get('setup'))
                  vService = int(step.get('service'))
                  vDuration = int(step.get('duration'))
                  geocode_long = step.get("location", [None, None])[0]
                  geocode_lat = step.get("location", [None, None])[1]
                  vDtStart = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival)
                  vDtEnd = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=vArrival + vSetup + vService)
                  print(f'Job {jobId}: Start: {vDtStart} - End: {vDtEnd} (Duration: {str(timedelta(seconds=vDuration))} segundos)')
                  minhas_coordenadas = [
                      [geocode_long_before, geocode_lat_before],
                      [geocode_long, geocode_lat]
                  ]
                  retorno = await get_route_distance(minhas_coordenadas)
                  distance = round(retorno['distance_meters'])
                  timeDistance = round(retorno['distance_time'])    

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
                          modified_by=userName,
                          modified_date=datetime.now(),
                      )
                  )
                  await db.commit()

                  geocode_long_before = geocode_long
                  geocode_lat_before = geocode_lat

        # Aqui você pode chamar a função de otimização passando o payload
        # optimized_solution = await optimize_routes_vroom(vroom_payload)
        # print("Solução otimizada do VROOM: ", optimized_solution)
      # ## Fazemos o for para criar as tarefas simuladas
      # smtp = text(f"""
      #   select a.*,
      #     EXTRACT(EPOCH FROM 
      #       case 
      #           when a.pt_end_date is null 
      #             then 
      #               a.actual_start_date - a.start_day
      #           else 
      #             a.actual_start_date - a.pt_end_date
      #         end )::INTEGER AS actual_time_arrived
      #   from (
        # select 
        #       js.description AS status_description,
        #       j.team_id, 
        #       j.job_id,
        #       j.client_job_id,
        #       t.client_team_id,
        #       j.resource_id,
        #       j.job_status_id,
        #       j.job_type_id,
        #       j.address_id,
        #       j.place_id,
        #       r.client_resource_id,
        #       j.actual_start_date,
        #       j.actual_end_date,
        #       a.geocode_lat,
        #       a.geocode_long,
        #       j.time_setup,
        #       j.time_service,
        #       J.time_limit_start,
        #       J.time_limit_end,
        #       j.actual_work_duration * 60 as actual_work_duration,
        #       EXTRACT(ISODOW FROM j.actual_start_date) week_day,
        #       CASE 
        #         WHEN j.pt_geocode_lat is null
        #           THEN r.geocode_lat_from
        #         ELSE
        #           j.pt_geocode_lat
        #       END AS geocode_lat_before,
        #       CASE 
        #         WHEN j.pt_geocode_long is null
        #           THEN r.geocode_long_from
        #         ELSE
        #           j.pt_geocode_long
        #       END AS geocode_long_before,
        #       EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date) )::INTEGER AS actual_time_service,
        #       j.time_service * 60 AS time_service,
        #       j.pt_job_id,
        #       cast(j.actual_start_date as date) + rw.start_time AS start_day,
        #       cast(j.actual_start_date as date) + rw.end_time AS end_day,
        #       j.pt_end_date
        #       from jobs j
        #       join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
        #       join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
        #       join teams t on t.client_id = j.client_id and t.team_id = j.team_id
        #       join user_team ut on ut.client_id = t.client_id and ut.team_id = t.team_id
        #       join address a on a.client_id = j.client_id and a.address_id = j.address_id
        #       join places p on p.client_id = j.client_id and p.place_id = j.place_id
        #       join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
        #       join resource_windows rw on rw.client_id = j.client_id and rw.resource_id = j.resource_id and rw.week_day = EXTRACT(ISODOW FROM j.actual_start_date)
        #       where ut.client_id = :client_id
        #         and ut.user_id = :user_id
        #         and j.actual_start_date >= cast(:date as date) and j.actual_start_date < cast(:date as date) + interval '1 day'
        #        and j.resource_id in ({resources})
        #        and js.internal_code_status = 'CONCLU'
      #         order by j.team_id, j.resource_id nulls last,j.actual_start_date nulls last, j.plan_start_date
      #   ) as a
      # """).bindparams(client_id=clientId, user_id=userId, date=p_date)
      # result = await db.execute(smtp)
      # rows = result.mappings().all()
      # rId = None
      # rDtStartReal = None
      # rDtEndReal = None
      # rDtStartWindow = None
      # rDtEndWindow = None
      # rWorkDurationTotal = 0
      # rWorkDurationSimulado = 0
      # rOrder = 1
      # for row in rows:
      #     if rId != row.resource_id:
      #       print('-----------------------------')
      #       print('Recurso: ', row.client_resource_id)
      #       rId = row.resource_id
      #       rDtStartReal = row.start_day
      #       rDtStartWindow = row.start_day

      #     minhas_coordenadas = [
      #         [row.geocode_long_before, row.geocode_lat_before],
      #         [row.geocode_long, row.geocode_lat]
      #     ]
      #     retorno = await get_route_distance(minhas_coordenadas)
      #     distance = round(retorno['distance_meters'])
      #     timeDistance = round(retorno['distance_time'])      
      #     # print('Distance: ', distance)
      #     # print('Tempo  : ', timeDistance)
      #     # print('Tempo Atual  : ', row.actual_time_arrived)
      #     # print('Tempo de Serviço Atual : ', row.actual_time_service)
      #     # print('Tempo Janela de Serviço  : ', row.time_service)
      #     # print('Data e hora Iniciou  : ', row.actual_start_date)
      #     rDtStartReal = rDtStartReal + timedelta(seconds=timeDistance)
      #     rDtEndReal = rDtStartReal + timedelta(seconds=row.actual_time_service)
      #     rDtStartWindow = rDtStartWindow + timedelta(seconds=timeDistance)
      #     rDtEndWindow = rDtStartWindow + timedelta(seconds=row.time_service)
      #     rSimulatedWorkDuration = (row.actual_time_service or 0) + (timeDistance or 0)
      #     print('Data e hora Simulada Inicio  : ', rDtStartReal)
      #     print('Data e hora Simulada Janela Inicio  : ', rDtStartWindow)
      #     print('Data e hora Iniciou  : ', row.actual_start_date)
      #     print('Data e hora Simulada Fim  : ', rDtEndReal)
      #     print('Data e hora Simulada Janela Fim  : ', rDtEndWindow)
      #     print('Data e hora Fim  : ', row.actual_end_date)
      #     print('Work Duration  : ', row.actual_work_duration)
      #     print('Work Duration Simulado  : ', rSimulatedWorkDuration)
      #     rWorkDurationTotal = rWorkDurationTotal + (row.actual_work_duration or 0)
      #     rWorkDurationSimulado = rWorkDurationSimulado + (row.actual_time_service or 0) + (timeDistance or 0)
      #     # insere on banco e muda a data Start
      #     newSimulationJobs = models.SimulationJobs(
      #           client_id = clientId,
      #           simulation_id = simulationId,
      #           job_id = row.job_id,
      #           client_job_id = row.client_job_id,
      #           team_id  = row.team_id,
      #           resource_id  = row.resource_id,
      #           job_status_id  = row.job_status_id,
      #           job_type_id  = row.job_type_id,
      #           address_id = row.address_id,
      #           place_id  = row.place_id,
      #           time_setup = row.time_setup,
      #           default_time_service = row.time_service,
      #           actual_time_service = row.actual_time_service,
      #           actual_work_duration = row.actual_work_duration,
      #           simulated_work_duration = rSimulatedWorkDuration,
      #           actual_start_date = row.actual_start_date,
      #           actual_end_date = row.actual_end_date,
      #           simulated_start_date = rDtStartReal,
      #           simulated_end_date = rDtEndReal,
      #           simulated_window_start_date = rDtStartWindow,
      #           simulated_window_end_date = rDtEndWindow,
      #           time_limit_start = row.time_limit_start,
      #           time_limit_end = row.time_limit_end,
      #           order = rOrder,
      #           actual_distance = distance,
      #           simulated_distance = distance,
      #           actual_time_distance = row.actual_time_arrived,
      #           simulated_time_distance = timeDistance,
      #           created_by = userName,
      #           created_date = datetime.now(),
      #           modified_by = userName,
      #           modified_date = datetime.now()
      #     )
      #     db.add(newSimulationJobs)
      #     await db.commit()
      #     rDtStartReal = rDtEndReal
      #     rDtStartWindow = rDtEndWindow
      #     rOrder = rOrder + 1
      #     #           # print("Tempo de deslocamento estimado (em minutos): ", retorno, ' - ', row.actual_time_arrived)
      # print('Work Duration Total  : ', rWorkDurationTotal)
      # print('Work Duration Simulado  : ', rWorkDurationSimulado)    

      
      smtp = text(f"""
            select 
              'SIMULATED' as type,
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
              j.default_time_service as time_service, 
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
              join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
              join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
              join teams t on t.client_id = j.client_id and t.team_id = j.team_id
              join address a on a.client_id = j.client_id and a.address_id = j.address_id
              join places p on p.client_id = j.client_id and p.place_id = j.place_id
              join resources r on j.client_id = r.client_id and j.resource_id = r.resource_id
              where j.client_id = :client_id
                and j.simulation_id = :simulation_id
            order by j.team_id, j.resource_id,actual_start_date
      """).bindparams(client_id=clientId, simulation_id = simulationId)
                          
      result = await db.execute(smtp)
      rows = result.mappings().all()

      res = [dict(row) for row in rows]
      return res      
      # return vroom_payload

   
