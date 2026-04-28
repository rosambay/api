import uuid
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
from services import optimize_routes_vroom, get_route_distance_block, logs

async def cleanSessionsRedis(r: Redis, session: str):
    # await r.delete(f"session:{session}")
    # await r.delete(f"filter:{session}")
    # await dropUserSession(session)
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

def get_session_from_token(token: str):
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        
        session = payload.get("session")
        
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
            "clientDomain": payload.get("clientDomain"),
            "session": payload.get("session"),
            "rawToken": token,
        }
        logger.info(f"Token decodificado: {dataToken}")
        if dataToken["clientId"] is None or dataToken["userId"] is None or dataToken["superUser"] is None:
            raise credentials_exception
        
    except jwt.ExpiredSignatureError:
        raise credentials_exception
    except JWTError:
        raise credentials_exception

    return dataToken


@app.post("/events/ticket", status_code=201)
async def create_events_ticket(
    r: Redis = Depends(redis_client.get_redis),
    current_user: dict = Depends(get_current_user)
):
    token = current_user["rawToken"]
    ticket = str(uuid.uuid4())
    await r.set(f"ticket:{ticket}", token, ex=15)
    return {"ticket": ticket}


@app.get("/events")
async def events_endpoint(
    request: Request,
    ticket: str = Query(...),
    r: Redis = Depends(redis_client.get_redis)):

    token_bytes = await r.get(f"ticket:{ticket}")
    if not token_bytes:
        raise HTTPException(status_code=401, detail="Ticket inválido ou expirado")
    await r.delete(f"ticket:{ticket}")
    token = token_bytes if isinstance(token_bytes, str) else token_bytes.decode()

    session = get_session_from_token(token)

    pubsub = r.pubsub()

    async def process_queue():
        messages = []

        while True:

            msg_json = await r.lpop(f"notify:{session}:queue")
            if not msg_json:
                break
            messages.append(msg_json)
        
        if messages:
            for m in messages:
                yield f"data: {m}\n\n"

    async def event_generator():
        try:
            await pubsub.subscribe(f"notify:{session}:notify")
            
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
            logger.info(f"Session {session} desconectou.")
            asyncio.create_task(cleanSessionsRedis(r, session))
        finally:
            await pubsub.unsubscribe(f"notify:{session}:notify")
            await r.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ROTAS DE AUTENTICAÇÃO

@app.post("/login", response_model=schemas.Token)
async def login(form_data: schemas.LoginRequest, request: Request, r: Redis = Depends(redis_client.get_redis), db: AsyncSession = Depends(get_db)):
    logger.info(f"Login attempt for domain: {form_data.domain}, user: {form_data.user} from IP: {request.client.host}")
    client_ip = request.client.host
    rate_key = f"login_rate:{client_ip}"
    attempts = await r.incr(rate_key)
    token_expiry = settings.access_token_expire_minutes * 60
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
    session = str(uuid.uuid4())
    sessionKey = f"session:{session}"

    stmt = text("""
        with resources as(
            select 
                tm.team_id, 
                json_agg(json_build_object('resource_id',tm.resource_id)) as list
             FROM team_members tm
              JOIN user_team ut ON ut.client_id = tm.client_id AND ut.team_id = tm.team_id
             where tm.client_id = :client_id
             AND ut.user_id = :user_id
             group by tm.team_id
        )
        select  json_agg(json_build_object('team_id', team_id, 'resources',  list)) as res FROM resources
    """)
    resultDb = await db.execute(stmt,{"client_id":clientId, "user_id":userDb.user_id})
    result = resultDb.scalars().first()

    await r.setex(sessionKey, token_expiry, json.dumps(result)),
    
    dataToken = {
        "userId": userDb.user_id,
        "userName": userDb.user_name,
        "superUser": superUser,
        "clientId": clientId,
        "clientUid": str(clientUid),
        "clientDomain": clientDomain,
        "session": session,
    }
    logger.info(f"Gerando token de acesso para usuário: {userDb.user_name} (userId: {userDb.user_id}) no domínio: {clientDomain}")
    access_token = create_access_token(data=dataToken)
    logger.info(f"Login successful for user: {userDb.user_name} in domain: {clientDomain}")
    return {"access_token": access_token, "token_type": "bearer", "session": uuid.uuid4()}

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

@app.get("/jobstatus", response_model=List[schemas.JobStatusResponse])
async def getJobStatus(
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    result = await db.execute(
        select(models.JobStatus)
        .where(models.JobStatus.client_id == clientId)
        .order_by(models.JobStatus.job_status_id)
    )
    return result.scalars().all()

@app.patch("/jobstatus/{job_status_id}", response_model=schemas.JobStatusResponse)
async def updateJobStatus(
    job_status_id: int,
    body: schemas.JobStatusUpdateRequest,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId   = current_user["userName"]

    result = await db.execute(
        select(models.JobStatus).where(
            models.JobStatus.client_id    == clientId,
            models.JobStatus.job_status_id == job_status_id
        )
    )
    job_status = result.scalar_one_or_none()
    if not job_status:
        raise HTTPException(status_code=404, detail="Status não encontrado")

    updates = body.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(job_status, field, value)

    job_status.modified_by   = str(userId)
    job_status.modified_date = datetime.now()

    await db.commit()
    await db.refresh(job_status)
    return job_status

@app.patch("/jobs/{job_id}/reschedule", response_model=schemas.JobRescheduleResponse)
async def rescheduleJob(
    job_id: int,
    body: schemas.JobRescheduleRequest,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userName = current_user["userName"]

    if body.plan_end_date <= body.plan_start_date:
        raise HTTPException(status_code=422, detail="plan_end_date deve ser posterior a plan_start_date")

    result = await db.execute(
        select(models.Jobs).where(
            models.Jobs.client_id == clientId,
            models.Jobs.job_id    == job_id
        )
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    start = body.plan_start_date.replace(tzinfo=None)
    end   = body.plan_end_date.replace(tzinfo=None)

    job.plan_start_date = start
    job.plan_end_date   = end
    job.time_service    = int((end - start).total_seconds())
    job.modified_by     = str(userName)
    job.modified_date   = datetime.now()

    await db.commit()
    await db.refresh(job)
    return job

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
            geocode_lat_actual,
            geocode_long_actual,
            geocode_lat_start,
            geocode_long_start,
            geocode_lat_end,
            geocode_long_end,
            off_shift_flag,
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

    
    # for r in geoResult:
    #    print(r)
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
            j.distance,
            j.time_distance,
            p.trade_name, 
            p.cnpj,
            j.time_setup, 
            j.time_service, 
            COALESCE(j.ajustment_start_date, j.plan_start_date) as plan_start_date, 
            COALESCE(j.ajustment_end_date,j.plan_end_date) as plan_end_date, 
            j.actual_start_date, 
            j.actual_end_date, 
            COALESCE(j.actual_start_date, j.ajustment_start_date, j.plan_start_date) as start_date,
            COALESCE(j.actual_end_date, j.ajustment_end_date, j.plan_end_date) as start_date,
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
            WHERE ut.client_id = :client_id
              AND ut.user_id = :user_id
              AND 1 = CASE 
                        WHEN COALESCE(j.actual_start_date, j.ajustment_start_date, j.plan_start_date) < cast(now() as date) 
                            THEN 
                                CASE 
                                    WHEN js.internal_code_status NOT IN ('CONCLU', 'CANCEL', 'CLOSED') THEN 1
                                    ELSE 0
                                END
                            ELSE 1
                    END
              and COALESCE(j.actual_start_date, j.ajustment_start_date, j.plan_start_date) >= cast(:p_date as date)
              and COALESCE(j.actual_start_date, j.ajustment_start_date, j.plan_start_date) < cast(:p_date as date) + interval '1 day'
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
    simulation_id: int,
    team_id: int,
    session: str,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):

    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    try:
      simulation_filter = ""
      bind_params: dict = {"client_id": clientId, "user_id": userId, "team_id": team_id}
      if simulation_id is not None:
         
          simulation_filter = """
                and not exists (
                    select 1 from simulation_view sw
                    where sw.client_id = j.client_id
                      and sw.job_id = j.job_id
                      and sw.simulation_id = :simulation_id
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
              r.geocode_lat_start,
              r.geocode_long_start,
              r.geocode_lat_end,
              r.geocode_long_end,
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
                and j.team_id = :team_id
                and (js.internal_code_status not in ('CONCLU', 'CANCEL', 'CLOSED') AND  js.internal_code_status IS NOT NULL)
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

@app.post("/clearschedulejobs")
async def clearScheduleJobs(
    body: schemas.ClearScheduleJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    resourceId = body.resource_id
    simulationId = body.simulation_id
    clientId = current_user["clientId"]

    smtp = text(f"""
          DELETE FROM simulation_jobs
            WHERE client_id = :client_id
              AND simulation_id = :simulation_id
              AND resource_id = :resource_id
          """).bindparams(client_id=clientId, simulation_id = simulationId, resource_id= resourceId)
    await db.execute(smtp)
    await db.commit()
    return []

@app.post("/newsimulation")
async def createNewSimulation(
    body: schemas.NewSimulationRequest,
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

    p_date    = body.p_date
    teamId   = body.team_id
    session   = body.session

    print("session",session)
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
                models.Simulation.session   == session,
            )
            .order_by(models.Simulation.sequence.desc())
        )
        lastSimulation = rLast.scalars().first()
        nextSequence   = (lastSimulation.sequence + 1) if lastSimulation else 1

        now = datetime.now()

        newSimulation = models.Simulation(
            client_id       = clientId,
            user_id         = userId,
            team_id         = teamId,
            simulation_date = date.fromisoformat(p_date),
            session         = session,
            sequence        = nextSequence,
            created_by      = userName,
            created_date    = now,
            modified_by     = userName,
            modified_date   = now,
        )
        db.add(newSimulation)
        await db.commit()
        await db.refresh(newSimulation)
        
        logger.info(
            "[newroutes] Simulação criada | client={} user={} team_id={} sim_id={} seq={} date={} session={}",
            clientId, userId, teamId, newSimulation.simulation_id, nextSequence, p_date, session,
        )
        return {
            "simulation_id":   newSimulation.simulation_id,
            "uid":             str(newSimulation.uid),
            "team_id":       newSimulation.team_id,
            "client_id":       newSimulation.client_id,
            "user_id":         newSimulation.user_id,
            "simulation_date": str(newSimulation.simulation_date),
            "sequence":        newSimulation.sequence,
            "session": newSimulation.session,
            "created_date":    newSimulation.created_date.isoformat(),
        }    

        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[newroutes] Erro ao criar simulação: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao criar simulação.")


@app.post("/simulationcomparison")
async def getSimulationComparison(
    body: schemas.SimulationComparisonRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    p_date         = body.p_date
    simulationIds  = body.simulation_ids
    clientId       = current_user["clientId"]

    if not simulationIds:
        raise HTTPException(status_code=400, detail="simulation_ids não pode ser vazio.")

    logger.info(f"Relatório comparativo: client_id={clientId}, p_date={p_date}, simulation_ids={simulationIds}")
    
    sim_ids_literal = ','.join(str(s) for s in simulationIds)

    if len(simulationIds) > 1:
        smtp = text(f"""
          DELETE
            FROM simulation_resources a
          WHERE a.client_id = :client_id
            AND a.simulation_id IN ({sim_ids_literal})
            AND NOT EXISTS (
                SELECT 1 
                FROM simulation_jobs b 
                WHERE a.simulation_id = b.simulation_id 
                  AND a.resource_id = b.resource_id
            )
            AND a.resource_id IN (
                SELECT resource_id
                FROM simulation_resources
                WHERE client_id = :client_id
                  AND simulation_id IN ({sim_ids_literal})
                GROUP BY resource_id
                HAVING COUNT(DISTINCT simulation_id) = 1
            );
            """).bindparams(client_id=clientId)
        await db.execute(smtp)
        await db.commit()

    smtp = text(f"""
        WITH simulations_active AS (
            SELECT s.simulation_id, s.sequence
            FROM simulation s
            WHERE s.client_id     = :client_id
              AND s.simulation_id IN ({sim_ids_literal})
        ),
        actual_by_resource AS (
            SELECT
                j.resource_id,
                COUNT(j.job_id)::INTEGER                                                          AS jobs_count,
                MIN(j.actual_start_date)                                                          AS start_date,
                MAX(j.actual_end_date)                                                            AS end_date,
                EXTRACT(EPOCH FROM (MAX(j.actual_end_date) - MIN(j.actual_start_date)))::INTEGER  AS duration_seconds,
                SUM(j.distance)::INTEGER                                                          AS actual_distance,
                SUM(j.time_distance)::INTEGER                                                     AS actual_time_distance
            FROM jobs j
            JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
            WHERE j.client_id  = :client_id
              AND j.actual_start_date >= cast(:p_date AS date)
              AND j.actual_start_date  < cast(:p_date AS date) + interval '1 day'
              AND js.internal_code_status = 'CONCLU'
              AND EXISTS (
                  SELECT 1 FROM simulation_resources sr2
                  JOIN simulations_active sa2 ON sa2.simulation_id = sr2.simulation_id
                  WHERE sr2.client_id = j.client_id AND sr2.resource_id = j.resource_id
              )
            GROUP BY j.resource_id
        ),
        sim_jobs_count AS (
            SELECT
                sj.simulation_id,
                sj.resource_id,
                COUNT(sj.job_id)::INTEGER AS jobs_count
            FROM simulation_jobs sj
            JOIN simulations_active sa ON sa.simulation_id = sj.simulation_id
            WHERE sj.client_id = :client_id
            GROUP BY sj.simulation_id, sj.resource_id
        )
        SELECT
            r.resource_id,
            r.client_resource_id,
            r.description,
            json_agg(
                json_build_object(
                    'simulation_id',       sa.simulation_id,
                    'sequence',            sa.sequence,
                    'jobs_count',          COALESCE(sjc.jobs_count, 0),
                    'distance_start',      sr.distance_start,
                    'distance_end',        sr.distance_end,
                    'time_distance_start', sr.time_distance_start,
                    'time_distance_end',   sr.time_distance_end,
                    'start_date',          sr.start_date,
                    'end_date',            sr.end_date
                ) ORDER BY sa.sequence
            ) AS simulations,
            abr.jobs_count        AS actual_jobs_count,
            abr.start_date        AS actual_start_date,
            abr.end_date          AS actual_end_date,
            abr.duration_seconds  AS actual_duration_seconds,
            abr.actual_distance   AS actual_distance,
            abr.actual_time_distance AS actual_time_distance
        FROM resources r
        JOIN simulation_resources sr
            ON  sr.client_id   = r.client_id
            AND sr.resource_id = r.resource_id
        JOIN simulations_active sa
            ON  sa.simulation_id = sr.simulation_id
        LEFT JOIN sim_jobs_count sjc
            ON  sjc.simulation_id = sr.simulation_id
            AND sjc.resource_id   = r.resource_id
        LEFT JOIN actual_by_resource abr
            ON  abr.resource_id = r.resource_id
        WHERE r.client_id = :client_id
        GROUP BY
            r.resource_id,
            r.client_resource_id,
            r.description,
            abr.jobs_count,
            abr.start_date,
            abr.end_date,
            abr.duration_seconds,
            abr.actual_distance,
            abr.actual_time_distance
        ORDER BY r.description
    """).bindparams(client_id=clientId, p_date=p_date)

    result = await db.execute(smtp)
    rows = result.mappings().all()
    return [dict(row) for row in rows]

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
              r.geocode_lat_start,
              r.geocode_long_start,
              r.geocode_lat_end,
              r.geocode_long_end,
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
        geocode_long_end = None
        geocode_lat_end = None
        geo = []
        for row in rows:
          if rId is None:
            geo.append([row.geocode_long_start,row.geocode_lat_start])
            geocode_long_end = row.geocode_long_end
            geocode_lat_end = row.geocode_lat_end
            rId = 1
          geo.append([row.geocode_long,row.geocode_lat])

        geo.append([geocode_long_end,geocode_lat_end])
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
                actual_time_setup = row.time_setup,
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
                  EXTRACT(EPOCH FROM (j.actual_end_date - j.actual_start_date) )::INTEGER AS time_service,
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
                  r.geocode_lat_start::NUMERIC,
                  r.geocode_long_start::NUMERIC,
                  r.geocode_lat_end::NUMERIC,
                  r.geocode_long_end::NUMERIC, 
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.start_time else off_shift_start_time end ) ::INTEGER AS start_time
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.end_time else r.off_shift_end_time end ) ::INTEGER AS end_time
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
                          'start', json_build_array(geocode_long_start, geocode_lat_start),
                          'end', json_build_array(geocode_long_end, geocode_lat_end),
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
                      COALESCE(j.time_service,jt.time_service,t.time_service) AS time_service,
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
                  r.geocode_lat_start::NUMERIC,
                  r.geocode_long_start::NUMERIC,
                  r.geocode_lat_end::NUMERIC,
                  r.geocode_long_end::NUMERIC, 
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.start_time else off_shift_start_time end ) ::INTEGER AS start_time
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.end_time else r.off_shift_end_time end ) ::INTEGER AS end_time
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
                          'start', json_build_array(geocode_long_start, geocode_lat_start),
                          'end', json_build_array(geocode_long_end, geocode_lat_end),
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
                  r.geocode_lat_start,
                  r.geocode_long_start,
                  r.geocode_lat_end,
                  r.geocode_long_end,
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
        geocode_long_end = None
        geocode_lat_end = None
        geo = []
        for row in rows:
          if rId is None:
            geo.append([row.geocode_long_start,row.geocode_lat_start])
            geocode_long_end = row.geocode_long_end
            geocode_lat_end = row.geocode_lat_end
            rId = 1
          geo.append([row.geocode_long,row.geocode_lat])
        geo.append([geocode_long_end,geocode_lat_end])
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
              j.actual_time_setup,
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

@app.post("/simulationbestroutejobs")
async def getSimulationBestRouteJobs(
    body: schemas.SimulationBestRouteJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    teamId = body.team_id
    p_date = body.p_date
    type = body.type
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    
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

    newSimulation = models.Simulation(
              client_id = clientId,
              user_id = userId,
              sequence = nextSequence,
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

    logger.info(f"""Parâmetros: simulation_id - {simulationId},  p_date - {p_date}, Type - {type}""")

    smtp = text(f"""
      MERGE INTO simulation_jobs u
            USING (SELECT  j.client_id
                          ,j.job_id
                          ,j.client_job_id
                          ,j.team_id
                          ,j.job_status_id
                          ,j.job_type_id
                          ,j.address_id
                          ,j.place_id
                      FROM jobs j
                       JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                       JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id 
                    WHERE j.client_id = :client_id
                      and j.team_id = :team_id
                      and a.geocode_lat IS NOT NULL
                      and (a.geocode_lat::NUMERIC) < 100
                      AND a.geocode_long IS NOT NULL
                      AND (a.geocode_long::NUMERIC) < 100
                      AND j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day'
                      AND js.internal_code_status = 'CONCLU') t
                ON (    u.client_id = t.client_id
                    AND u.job_id = t.job_id
                    AND u.team_id = t.team_id
                    AND u.simulation_id = :simulation_id)
        WHEN NOT MATCHED
        THEN
          INSERT     (client_id
                    ,simulation_id
                    ,job_id
                    ,client_job_id
                    ,team_id
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
                  ,t.job_status_id
                  ,t.job_type_id
                  ,t.address_id
                  ,t.place_id
                  , :user_name
                  ,now ()
                  , :user_name
                  ,now ());
      """).bindparams(client_id=clientId, user_name=userName, p_date = p_date, simulation_id = simulationId, team_id=teamId)
    
    result = await db.execute(smtp)


    logger.info("Criando os resources da simulação ...")
    if type == 'BRAA':
        # BRAA — todos os recursos do time que têm janela para o dia da semana
        smtp = text(f"""
            MERGE INTO simulation_resources u
                USING (SELECT r.client_id, r.resource_id
                          FROM resources r
                          JOIN team_members tm ON tm.client_id = r.client_id AND tm.resource_id = r.resource_id
                          JOIN resource_windows rw ON rw.client_id = r.client_id AND rw.resource_id = r.resource_id
                            AND rw.week_day = EXTRACT(DOW FROM CAST(:p_date AS DATE)) + 1
                        WHERE r.client_id = :client_id
                          AND tm.team_id = :team_id
                        ) t
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
        """).bindparams(client_id=clientId, user_name=userName, simulation_id=simulationId, p_date=p_date, team_id=teamId)
    else:
        # BRAC — apenas recursos que tiveram jobs CONCLU naquele dia
        smtp = text(f"""
            MERGE INTO simulation_resources u
                USING (SELECT client_id, resource_id
                          FROM resources r
                        WHERE     client_id = :client_id
                          AND EXISTS (
                              SELECT 1
                                FROM jobs j
                                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                              WHERE j.actual_start_date >= cast(:p_date as date) and j.actual_start_date < cast(:p_date as date) + interval '1 day'
                                AND js.internal_code_status = 'CONCLU'
                                AND j.client_id = r.client_id AND j.resource_id = r.resource_id
                                AND j.team_id = :team_id
                              )
                        ) t
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
        """).bindparams(client_id=clientId, user_name=userName, simulation_id=simulationId, p_date=p_date, team_id=teamId)
    result = await db.execute(smtp)
    
    
    logger.info("Iniciado Calculo das rotas ...")
    smtp = text(f"""
          WITH q1 AS (
              SELECT j.client_id
                    ,j.job_id
                    ,j.address_id
                    ,a.geocode_lat::NUMERIC geocode_lat
                    ,a.geocode_long::NUMERIC geocode_long
                    ,0 AS time_overlap
                    ,COALESCE (j.time_setup, jt.time_setup, t.time_setup) AS time_setup
                    ,COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service
                    ,j.priority + (COALESCE (jt.priority, 0) / 100)::INTEGER AS priority
                    ,0 AS start_time
                    ,EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59.9999' AS TIME)))::INTEGER AS end_time
                FROM jobs  j
                    JOIN simulation_jobs sj ON sj.client_id = j.client_id AND sj.job_id = j.job_id
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                    LEFT JOIN address_windows aw
                      ON     aw.client_id = j.client_id
                          AND aw.address_id = j.address_id
                          AND aw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
              WHERE     sj.client_id = :client_id
                    AND sj.simulation_id = :simulation_id
                    AND j.team_id = :team_id
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
                address_id,
                geocode_lat,
                geocode_long,
                time_setup,
                time_service + (time_overlap * (quantidade_jobs_mesmo_endereco -1)) as time_service,
                priority,
                start_time,
                end_time
            FROM MapeamentoEnderecos
            --WHERE ordem_job = 1
                ),
              vehicles_data AS (
                select
                  r.resource_id,
                  r.description,
                  r.geocode_lat_start::NUMERIC,
                  r.geocode_long_start::NUMERIC,
                  r.geocode_lat_end::NUMERIC,
                  r.geocode_long_end::NUMERIC,
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.start_time else off_shift_start_time end ) ::INTEGER AS start_time
                  EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then rw.end_time else r.off_shift_end_time end ) ::INTEGER AS end_time
                from resources r
                  join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                  join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                  join resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM CAST(:p_date AS DATE)) + 1
                  join simulation_resources sr on sr.client_id = r.client_id and sr.resource_id = r.resource_id and sr.simulation_id = :simulation_id
                  where r.client_id = :client_id
                    and t.team_id = :team_id
              ),
              vehicles_json AS (
                  SELECT json_agg(
                  json_strip_nulls(
                      json_build_object(
                          'id', resource_id,
                          'description', description,
                          'start', json_build_array(geocode_long_start, geocode_lat_start),
                          'end', json_build_array(geocode_long_end, geocode_lat_end),
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
                          'priority', priority,
                          'time_windows', json_build_array(json_build_array(start_time, end_time)) 
                      ))
                  ) AS array_jobs
                  FROM jobs_data
              )
              SELECT json_build_object(
                  'vehicles', (SELECT array_vehicles FROM vehicles_json),
                  'jobs', (SELECT array_jobs FROM jobs_json)
              ) AS vroom_payload;
    """).bindparams(client_id=clientId, simulation_id=simulationId, p_date = p_date, team_id = teamId)

    result = await db.execute(smtp)
    rows = result.mappings().all()
    retorno = None
    if rows and len(rows) > 0:
      vroom_payload = rows[0]['vroom_payload']
      logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
      
      retorno = await optimize_routes_vroom(vroom_payload)
      listIds = [item['id'] for item in retorno.get("unassigned", [])]
      if listIds:
        logger.info(f'Removendo Jobs da rota  ... {listIds}' )
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

      routes = retorno['routes']
      for route in routes:
        resourceId = int(route['vehicle'])
        logger.info(f"Resource id ...{resourceId}")
        # print(route)
        steps = route['steps']
        # print(steps)
        # print("=======================================")
        jobs = [step for step in steps if step.get('type') == 'job']
        # print(jobs)
        for job in jobs:
            jobId = int(job['id'])
            logger.info(f"Job id ...{jobId}")
            await db.execute(
              update(models.SimulationJobs)
              .where(
                  models.SimulationJobs.client_id == clientId,
                  models.SimulationJobs.simulation_id == simulationId,
                  models.SimulationJobs.job_id == jobId
              )
              .values(
                  resource_id = resourceId,
                  modified_by=userName,
                  modified_date=datetime.now(),
              )
            )
            await db.commit()

      for route in routes:
        resourceId = int(route['vehicle'])
        logger.info(f"Resource ... {route['vehicle']}")
        # print(route)
        steps = route['steps']
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
                      a.geocode_lat,
                      a.geocode_long,
                      r.geocode_lat_start,
                      r.geocode_long_start,
                      r.geocode_lat_end,
                      r.geocode_long_end,
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
                      JOIN resources r ON r.client_id = j.client_id AND r.resource_id = sj.resource_id
                      --JOIN q1 on q1.geocode_lat = a.geocode_lat::NUMERIC and q1.geocode_long = a.geocode_long::NUMERIC
                      JOIN q1 on q1.job_id = sj.job_id
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
        geocode_long_end = None
        geocode_lat_end = None
        geo = []
        arrivalStart = None
        arrivalEnd = None
        for row in rows:
          if rId is None:
            geo.append([float(row.geocode_long_start),float(row.geocode_lat_start)])
            geocode_long_end = row.geocode_long_end
            geocode_lat_end = row.geocode_lat_end
            arrivalStart = row.arrival_start
            arrivalEnd = row.arrival_end
            rId = 1
          geo.append([float(row.geocode_long),float(row.geocode_lat)])
        geo.append([float(geocode_long_end),float(geocode_lat_end)])
        
        geoResult = await get_route_distance_block(geo)
        print(json.dumps(geoResult))
        endDate = None
        distanceEnd = 0
        timeDistanceEnd = 0
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
          # logger.info(f"Otimizando recurso {resourceId} - Job {jobId} , arrival: {vArrival} - arrival before: {vArrivalBefore} - Overlap - {row.time_overlap} total overlap {totalOverlap}")

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
                start_date=vDtStart,
                end_date=vDtEnd,
                distance=distance,
                time_distance=timeDistance,
                time_setup=vSetup,
                order = rOrder,
                modified_by=userName,
                modified_date=datetime.now(),
            )
          )
          # await db.commit()
          rOrder += 1

        logger.info(f'Calculando distância Simulada Window Final ...')

        distanceEnd = round(geoResult[(rOrder-1)]["distance"])
        timeDistanceEnd = round(geoResult[(rOrder-1)]["duration"])
        distanceStart = round(geoResult[(0)]["distance"])
        timeDistanceStart = round(geoResult[(0)]["duration"])

        endDate = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=arrivalEnd)
        startDate = datetime.strptime(p_date, '%Y-%m-%d') + timedelta(seconds=arrivalStart)
        
        logger.info(f"Valores - resource_id: {resourceId} - {distanceEnd} - {timeDistanceEnd} - {distanceStart} - {timeDistanceStart} - {endDate} - {startDate}")        

        await db.execute(
          update(models.SimulationResources)
          .where(
              models.SimulationResources.client_id == clientId,
              models.SimulationResources.simulation_id == simulationId,
              models.SimulationResources.resource_id == resourceId
          )
          .values(
              distance_start = distanceStart,
              start_date = startDate,
              time_distance_start = timeDistanceStart,
              distance_end = distanceEnd,
              end_date = endDate,
              time_distance_end = timeDistanceEnd,
              modified_by=userName,
              modified_date=datetime.now(),
          )
        )
        await db.commit()   

    smtp = text(f"""
        with q1 as(
            select 
              :type as type,
              j.job_id,
              j.simulation_id,
              j.client_job_id,
              j.team_id, 
              j.resource_id, 
              r.client_resource_id,
              sr.distance_end         as resource_distance_end,
              sr.end_date             as resource_end_date,
              sr.time_distance_end    as resource_time_distance_end,
              sr.distance_start       as resource_distance_start,
              sr.start_date           as resource_start_date,
              sr.time_distance_start  as resource_time_distance_start,
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
              j.time_setup,
              j.start_date,
              j.end_date,
              j.distance,
              j.time_distance
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
                and t.team_id = :team_id
              )
        select
            q1.type,
            q1.simulation_id,
            q1.team_id,
            q1.resource_id,
            q1.resource_distance_end,
            q1.resource_end_date,
            q1.resource_time_distance_end,
            q1.resource_distance_start,
            q1.resource_start_date,
            q1.resource_time_distance_start,
            jsonb_agg(to_jsonb(q1) ORDER BY q1.simulated_window_order) AS resources
          from q1
          group by q1.type, q1.simulation_id, q1.team_id,
            q1.resource_id,
            q1.resource_distance_end,
            q1.resource_end_date,
            q1.resource_time_distance_end,
            q1.resource_distance_start,
            q1.resource_start_date,
            q1.resource_time_distance_start
            
      """).bindparams(client_id=clientId, simulation_id = simulationId, type= type, team_id = teamId)
    logger.info('Iniciou terceira consulta...')             
    result = await db.execute(smtp)
    rows = result.mappings().all()

    res = [dict(row) for row in rows]
    logger.info('Finalizou terceira consulta...')
    return res

# Reports
@app.post("/historybestroutejobs")
async def getHistoryBestRouteJobs(
    body: schemas.HistoryBestRouteJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    teamId = body.team_id
    p_date = body.p_date
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    
    smtp = text(f"""
        select report 
          from reports
         where client_id = :client_id
           and team_id = :team_id
           and report_date = CAST(:p_date AS DATE)
      """).bindparams(client_id=clientId, team_id = teamId, p_date = p_date)

    result = await db.execute(smtp)
    rows = result.mappings().all()
    if not rows or len(rows)==0:
       return []
    res = rows[0]['report']
    logger.info('Finalizou terceira consulta...')
    return res

@app.post("/bestroutejobsbydate")
async def getBestRouteJobsByDate(
    body: schemas.BestRouteJobsByDateRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    teamId = body.team_id
    p_start_date = body.p_start_date
    p_end_date = body.p_end_date
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]
    
    smtp = text(f"""
        WITH raw_data AS (
            SELECT report AS doc, team_id, report_date 
            from reports
            where client_id = :client_id
              and team_id = :team_id
            and report_date between CAST(:p_start_date AS DATE) and CAST(:p_end_date AS DATE)
                
        ),
        dados as (
        SELECT 
            (rep->>'type')::varchar AS report_type,
            team_id,
            report_date,

            (res->>'client_id')::int AS client_id,
            (res->>'resource_id')::int AS resource_id,
            (res->>'job_day')::timestamp AS res_job_day,
            (res->>'total_jobs')::int AS total_jobs,
            (res->>'total_distance')::numeric AS total_distance,
            (res->>'total_time_distance')::numeric AS total_time_distance

        FROM raw_data,
        -- 1. Expande a lista de tipos de relatório (REAL, BRAC, etc)
        LATERAL jsonb_array_elements(doc->'reports') AS rep,
        -- 2. Expande a lista de recursos dentro de cada relatório
        LATERAL jsonb_array_elements(rep->'resources') AS res)
        select 
          team_id
        , report_type
        , report_date
        ,  sum(total_distance) as total_distance
        ,  sum(total_time_distance) as total_time_distance
        ,  sum(total_jobs) as total_jobs
        from (select * from dados)
        group by 1,3,2
        order by 1,3,2


      """).bindparams(client_id=clientId, team_id = teamId, p_start_date = p_start_date, p_end_date = p_end_date)

    result = await db.execute(smtp)
    rows = result.mappings().all()
    
    if not rows or len(rows)==0:
       return []
    
    res = [dict(row) for row in rows]
    return res


@app.get("/actualschedulejobs")
async def getActualScheduleJobs(
    simulation_id: int = Query(..., description="Data no formato YYYY-MM-DD"),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]

    smtp = text(f"""
        select json_dado from simulation
        WHERE client_id = :client_id
            AND simulation_id = :simulation_id
        """)
    parametros = {
        "client_id": clientId,
        "simulation_id": simulation_id
    }
    result = await db.execute(smtp, parametros)
    await db.commit()
    for row in result:
        logger.warning(f'Reports finalizados!')
        return row.json_dado 

@app.post("/schedulejobs")
async def scheduleJobs(
    body: schemas.ScheduleJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    resources = body.resources
    simulationId = body.simulation_id
    perResource = body.per_resource or None
    listJobs = body.jobs
    action = body.action
    userId = current_user["userId"]
    userName = current_user["userName"]
    clientId = current_user["clientId"]

    async def calc_rote():
        if action =='C':
            smtp = text(f"""
                UPDATE simulation
                    SET json_dado = '[]'::jsonb
                WHERE client_id = :client_id
                    AND simulation_id = :simulation_id
                    RETURNING json_dado
                """).bindparams(client_id=clientId, simulation_id = simulationId)
            parametros = {
                "client_id": clientId,
                "simulation_id": simulationId
            }
            result = await db.execute(smtp, parametros)
            await db.commit()
            for row in result:
                logger.warning(f'Reports finalizados!')
                return row.json_dado 
            
        if action =='D' and len(listJobs) == 0 and len(resources) == 1:
            smtp = text(f"""
                UPDATE simulation
                    SET json_dado = (
                        SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                        FROM jsonb_array_elements(json_dado) AS elem
                        WHERE (elem->>'resource_id')::INT NOT IN({','.join(str(j) for j in resources)})
                    )
                WHERE client_id = :client_id
                    AND simulation_id = :simulation_id
                    RETURNING json_dado
                """).bindparams(client_id=clientId, simulation_id = simulationId)
            parametros = {
                "client_id": clientId,
                "simulation_id": simulationId
            }
            result = await db.execute(smtp, parametros)
            await db.commit()
            for row in result:
                logger.warning(f'Reports finalizados!')
                return row.json_dado
        
        logger.info(f"Iniciado Calculo das rotas ...action - {action}, {len(resources)}, {len(listJobs)}")                    
        smtp = text(f"""
            WITH dados AS (
                SELECT j.client_id
                    ,t.team_id
                    ,COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap,0) AS time_overlap
                    ,j.job_id
                    ,j.address_id
                    ,a.geocode_lat::NUMERIC geocode_lat
                    ,a.geocode_long::NUMERIC geocode_long
                    ,COALESCE (j.time_setup, jt.time_setup, t.time_setup) AS time_setup
                    ,COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service
                    ,j.priority + (COALESCE (jt.priority, 0) / 100)::INTEGER AS priority
                    ,EXTRACT (epoch FROM COALESCE (aw.start_time, CAST ('00:00:00' AS TIME)))::INTEGER AS start_time
                    --,0 AS start_time
                    --,EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59' AS TIME)))::INTEGER AS end_time
                    ,CASE 
                      WHEN j.time_limit_end IS NOT NULL AND s.simulation_date = date_trunc('day',j.time_limit_end) 
                        THEN 
                         EXTRACT (epoch FROM (j.time_limit_end - s.simulation_date))::INTEGER
                      ELSE
                        EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59' AS TIME)))::INTEGER
                    END AS end_time
                FROM jobs  j
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                    JOIN simulation s ON s.client_id = t.client_id and s.team_id = t.team_id
                    LEFT JOIN address_windows aw
                    ON     aw.client_id = j.client_id
                        AND aw.address_id = j.address_id
                        AND aw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
            WHERE   j.client_id = :client_id
                AND s.simulation_id = :simulation_id
                AND j.job_id IN ({','.join(str(j) for j in listJobs)})
            ),
            MapeamentoEnderecos AS (
                SELECT 
                    *,
                    -- 1. Conta o total de jobs para o mesmo address_id (e client_id, por segurança)
                    COUNT(job_id) OVER (
                        PARTITION BY client_id, team_id, geocode_lat, geocode_long
                    ) AS quantidade_jobs_mesmo_endereco,
                    
                    -- 2. Cria um ranking ordenando pelo menor job_id
                    ROW_NUMBER() OVER (
                        PARTITION BY client_id, team_id, geocode_lat, geocode_long
                        ORDER BY job_id ASC
                    ) AS ordem_job
                FROM dados
            ),
            list as (
                SELECT json_agg(job_id) AS job_id_list
                    FROM dados
            ),
            q1 as (
                SELECT 
                client_id,
                team_id,
                job_id,
                address_id,
                geocode_lat,
                geocode_long,
                time_setup,
                time_overlap,
                time_service + (time_overlap * (quantidade_jobs_mesmo_endereco -1)) as time_service,
                priority,
                start_time,
                end_time
                FROM MapeamentoEnderecos
                WHERE ordem_job = 1
            ),
            vehicles_data AS (
                select
                r.resource_id,
                r.description,
                COALESCE(r.geocode_lat_start,t.geocode_lat)::NUMERIC AS geocode_lat_start,
                COALESCE(r.geocode_long_start,t.geocode_long)::NUMERIC AS geocode_long_start,
                COALESCE(r.geocode_lat_end,t.geocode_lat)::NUMERIC AS geocode_lat_end,
                COALESCE(r.geocode_long_end,t.geocode_long)::NUMERIC AS geocode_long_end,
                CASE 
                    WHEN DATE_TRUNC('day',now()) < s.simulation_date
                        THEN 
                            EXTRACT(EPOCH FROM COALESCE(rw.start_time,t.start_time)) ::INTEGER
                    ELSE
                        CASE 
                            WHEN (NOW()::TIME > COALESCE(rw.start_time,t.start_time))
                                THEN
                                    EXTRACT(EPOCH FROM NOW() - DATE_TRUNC('day',NOW()))::INTEGER
                            ELSE
                              EXTRACT(EPOCH FROM COALESCE(rw.start_time,t.start_time))
                        END
                END AS start_time,
                EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.end_time,t.end_time) else cast('23:59:59' as time) end ) ::INTEGER AS end_time
                from resources r
                join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                join simulation s ON s.client_id = t.client_id and s.team_id = t.team_id
                LEFT JOIN resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM s.simulation_date) + 1
                where r.client_id = :client_id
                and s.simulation_id = :simulation_id
                AND COALESCE(r.geocode_lat_start, t.geocode_lat) IS NOT NULL
                AND COALESCE(r.geocode_long_start, t.geocode_long) IS NOT NULL
                AND COALESCE(r.geocode_lat_end, t.geocode_lat) IS NOT NULL
                AND COALESCE(r.geocode_long_end, t.geocode_long) IS NOT NULL
                and r.resource_id IN ({','.join(str(j) for j in resources)})
            ),
            vehicles_json AS (
            SELECT json_agg(
            json_strip_nulls(
                json_build_object(
                    'id', resource_id,
                    'description', description,
                    'start', json_build_array(geocode_long_start, geocode_lat_start),
                    'end', json_build_array(geocode_long_end, geocode_lat_end),
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
                        'priority', priority,
                        'time_windows', json_build_array(json_build_array(start_time, end_time)) 
                    ))
                ) AS array_jobs
                FROM q1
            )
            SELECT json_build_object(
                'vehicles', (SELECT array_vehicles FROM vehicles_json),
                'jobs', (SELECT array_jobs FROM jobs_json),
                'list', (SELECT job_id_list FROM list)
            ) AS vroom_payload;
        """).bindparams(client_id=clientId, simulation_id = simulationId)
        result = await db.execute(smtp)
        subRows = result.mappings().all()
        if not subRows or len(subRows) == 0:
            return
        asyncio.create_task(logs(clientId=clientId,log='dados para vroom',logJson=dict(subRows[0])))
        vroom_payload = subRows[0]['vroom_payload']

        vroomJobList = vroom_payload.pop('list', 'Chave não encontrada')
        logger.info(list(vroomJobList))
        logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
        retorno = await optimize_routes_vroom(vroom_payload)

        routes = retorno['routes']
        geos = [
            [[step['location'][0], step['location'][1]] for step in rou['steps']]
            for rou in routes
        ]
        geo_results = await asyncio.gather(*[get_route_distance_block(geo) for geo in geos])

        for rou, geoResult in zip(routes, geo_results):
            asyncio.create_task(logs(clientId=clientId, log='montagem do geo', logJson=[[s['location'][0], s['location'][1]] for s in rou['steps']]))
            asyncio.create_task(logs(clientId=clientId, log='retorno do geo', logJson=geoResult))
            rou['steps'][0]['time_distance'] = 0
            rou['steps'][0]['distance'] = 0
            for ln, x in enumerate(geoResult, start=1):
                rou['steps'][ln]['time_distance'] = x['duration']
                rou['steps'][ln]['distance'] = x['distance']

        # routes = retorno['routes']
        # for rou in routes:
        #     # print(rou)
        #     # print('--------------------------------------')
        #     steps = rou['steps']
        #     geo = []
        #     for step in steps:
        #         geo.append([step['location'][0],step['location'][1]])
        #     # print(geo)
        #     # print('---------------------------------------')
        #     await logs(clientId=clientId,log='montagem do geo',logJson=geo)
        #     geoResult = await get_route_distance_block(geo)
        #     await logs(clientId=clientId,log='retorno do geo',logJson=geoResult)
        #     # print(geoResult) if r=='BRAC' else None
        #     ln = 1
        #     step = rou['steps'][0]
        #     step['time_distance'] = 0
        #     step['distance'] = 0
        #     for x in geoResult:
        #         # print(x['duration'],x['distance'])
        #         step = rou['steps'][ln]
        #         step['time_distance'] = x['duration']
        #         step['distance'] = x['distance']
        #         # print(ln,' - ',step)
        #         ln += 1
        listIds = [item['id'] for item in retorno.get("unassigned", [])]
        if listIds:
            asyncio.create_task(logs(clientId=clientId,log='lista de unassigned',logJson=listIds))
            logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
        
        asyncio.create_task(logs(clientId=clientId,log='retorno do vroom',logJson=retorno))
        
        vroomResult = json.dumps(retorno)
        smtp = text(f"""
            WITH payload_data AS (
                SELECT '{vroomResult}'::jsonb AS data
            ),
            rotas AS (
                SELECT 
                    (rota_json->>'vehicle')::int AS id_veiculo,
                    rota_json->>'description' AS nome_motorista,
                    rota_json->'steps' AS passos_array
                FROM payload_data,
                    jsonb_array_elements(data->'routes') AS rota_json
            ),
            q1 as (
                SELECT
                    r.id_veiculo::int AS resource_id,
                    ordem_passo::int AS sequence,
                    passo_json->>'type' AS stop_type,
                    (passo_json->>'id')::int AS job_id,
                    (passo_json->'location'->>0)::numeric AS geocode_long,
                    (passo_json->'location'->>1)::numeric AS geocode_lat,
                    (passo_json->>'arrival')::int as arrival,
                    (passo_json->>'service')::int AS service,
                    (passo_json->>'setup')::int AS setup,
                    (passo_json->>'distance')::numeric::int AS distance,
                    (passo_json->>'time_distance')::numeric::int AS time_distance
                FROM rotas r,
                    jsonb_array_elements(r.passos_array) WITH ORDINALITY AS passo(passo_json, ordem_passo)
            ),
            dados_jobs as(
                select  j.client_id,
                        j.team_id,
                        j.job_id,
                        s.simulation_date AS job_day,
                        COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap,0) AS time_overlap,
                        j.client_job_id,
                        js.description AS status_description,
                        jt.description as type_description,
                        CASE WHEN j.job_id <>  FIRST_VALUE(job_id) OVER (PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC)
                        THEN FIRST_VALUE(job_id) OVER (PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC)
                        ELSE j.job_id
                        END AS new_job_id,
                        j.job_type_id,
                        j.job_status_id,
                        COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service,
                        ROW_NUMBER() OVER (
                        PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long
                        ORDER BY job_id ASC
                    ) AS ordem_job,
                        j.place_id,
                        j.address_id,
                        a.geocode_lat, 
                        a.geocode_long,
                        a.address, 
                        a.city, 
                        a.state_prov, 
                        a.zippost,
                        p.trade_name, 
                        p.cnpj
                from jobs j
                join address a on a.client_id = j.client_id and a.address_id = j.address_id
                join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
                join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                join places p on p.client_id = j.client_id and p.place_id = j.place_id
                JOIN simulation s ON s.client_id = t.client_id and s.team_id = t.team_id
                where j.client_id = :client_id
                    and s.simulation_id = :simulation_id
                    and j.job_id IN ({','.join(str(j) for j in vroomJobList)})
            ),
            dados as(
                select
                    r.client_id,
                    r.resource_id,
                    r.client_resource_id,
                    j.team_id,
                    j.client_job_id,
                    r.description resource_name,
                    j.status_description,
                    j.type_description,
                    j.address, 
                    j.city, 
                    j.state_prov, 
                    j.zippost, 
                    j.trade_name, 
                    j.cnpj,
                    j.job_day,
                    q_1.geocode_long AS geocode_long_start,
                    q_1.geocode_lat AS  geocode_lat_start,
                    q_1.arrival AS arrival_from,
                    q_3.distance AS distance_from,
                    q_3.time_distance as time_distance_from,
                    
                    q_2.geocode_long AS geocode_long_end,
                    q_2.geocode_lat AS  geocode_lat_end,
                    q_2.arrival AS arrival_at,
                    q_2.distance AS distance_at,
                    q_2.time_distance as time_distance_at,
                    
                    j.job_id,
                    j.time_overlap,
                    q.geocode_long,
                    q.geocode_lat,
                    q.setup,
                    --q.arrival,
                    j.ordem_job,
                    CASE 
                        WHEN j.ordem_job > 1
                        THEN
                            q.arrival + (j.time_overlap * (j.ordem_job-1))
                        ELSE
                        q.arrival
                    end AS arrival,
                    j.time_service AS service,      
                    --q.service,
                    CASE 
                        WHEN j.ordem_job > 1
                        THEN
                            0
                        ELSE
                        q.distance
                    end AS distance,
                    --q.distance,
                    CASE 
                        WHEN j.ordem_job > 1
                        THEN
                            0
                        ELSE
                        q.time_distance
                    end AS time_distance
                    --q.time_distance
                from q1 q
                join dados_jobs j ON j.client_id = 1 and j.new_job_id = q.job_id
                join resources r ON r.client_id = 1 and r.resource_id = q.resource_id
                join q1 q_1 on q_1.resource_id = q.resource_id and q_1.stop_type = 'start'
                join q1 q_2 on q_2.resource_id = q.resource_id and q_2.stop_type = 'end'
                join q1 q_3 on q_3.resource_id = q.resource_id and q_3.stop_type = 'job' AND q_3.sequence = 2
            ),
            dados_new as (
                select 
                a.*,
                a.job_day + (arrival *  INTERVAL '1 second') start_date,
                a.job_day + ((arrival + setup + service) *  INTERVAL '1 second') end_date,
                a.job_day + ((arrival_from) *  INTERVAL '1 second') date_from,
                a.job_day + ((arrival_at) *  INTERVAL '1 second') date_at
                from dados a
            ),
            grp as (
                select 
                    q.client_id,
                    q.resource_id,
                    q.client_resource_id,
                    q.resource_name,
                    q.job_day,
                    q.geocode_long_start,
                    q.geocode_lat_start,
                    q.distance_from,
                    q.time_distance_from,
                    q.arrival_from,
                    q.date_from,
                    q.geocode_long_end,
                    q.geocode_lat_end,
                    q.distance_at,
                    q.time_distance_at,
                    q.arrival_at,
                    q.date_at,
                    json_agg(
                        json_build_object(
                            'client_id', q.client_id,
                            'team_id', q.team_id,
                            'job_id', q.job_id,
                            'time_overlap', q.time_overlap,
                            'client_job_id', q.client_job_id,
                            'status_description', q.status_description,
                            'type_description', q.type_description,
                            'address', q.address, 
                            'city', q.city, 
                            'state_prov', q.state_prov, 
                            'zippost', q.zippost, 
                            'trade_name', q.trade_name, 
                            'cnpj', q.cnpj,
                            'job_day', q.job_day,      
                            'geocode_long', q.geocode_long,
                            'geocode_lat', q.geocode_lat,
                            'arrival', q.arrival,
                            'service', q.service,
                            'distance', q.distance,
                            'time_distance', q.time_distance,
                            'start_date', q.start_date,
                            'end_date', q.end_date
                        ) ORDER BY q.start_date
                    ) AS jobs,
                    sum(q.distance) + max(q.distance_at) as total_distance,
                    sum(q.time_distance) + max(q.time_distance_at) as total_time_distance,
                    count(q.job_id) total_jobs
                from dados_new q
                GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
            )
            select
                json_agg(
                    json_build_object(
                        'client_id', client_id,
                        'resource_id', resource_id,
                        'client_resource_id', client_resource_id,
                        'resource_name', resource_name,
                        'job_day', job_day,
                        'geocode_long_start', geocode_long_start,
                        'geocode_lat_start', geocode_lat_start,
                        'distance_from', distance_from,
                        'time_distance_from', time_distance_from,
                        'arrival_from', arrival_from,
                        'date_from', date_from,
                        'geocode_long_end', geocode_long_end,
                        'geocode_lat_end', geocode_lat_end,
                        'distance_at', distance_at,
                        'time_distance_at', time_distance_at,
                        'arrival_at', arrival_at,
                        'date_at', date_at,
                        'total_distance', total_distance,
                        'total_time_distance', total_time_distance,
                        'total_jobs', total_jobs,
                        'jobs', jobs
                    )
                ) AS res
            from grp
        """).bindparams(client_id=clientId,simulation_id = simulationId)
        result = await db.execute(smtp)
        subSubRows = result.mappings().all()
        if not subSubRows or len(subSubRows) == 0:
            return
        
        asyncio.create_task(logs(clientId=clientId,log=f'Resultado da Simulacao',logJson=subSubRows[0]))
        res = subSubRows[0]['res']

        rep_json = json.dumps(res)
        if len(resources) == 1:
            logger.warning('Entrou aqui no update...')
            smtp = text(f"""
                UPDATE simulation
                SET json_dado = 
                    -- 1. Monta um array com todos os itens, EXCETO o ID 3 (remove o antigo se existir)
                    COALESCE(
                        (
                            SELECT jsonb_agg(elem)
                            FROM jsonb_array_elements(json_dado) AS elem
                            WHERE (elem->>'resource_id')::INT NOT IN ({','.join(str(j) for j in resources)})
                        ), 
                        '[]'::jsonb -- Garante que não retorne NULL caso o array fique vazio
                    ) 
                    
                    || -- Operador de concatenação de JSONB
                    
                    -- 2. Adiciona o NOVO nó inteiro (dentro de colchetes para mesclar no array principal)
                    :json_data
                    ,modified_by = 'system'
                    ,modified_date  = NOW()

                WHERE  client_id = :client_id and simulation_id = :simulation_id
                RETURNING
                    json_dado
                    """)
            
        else:
            logger.warning('Entrou aqui no MERGE...')   
            smtp = text(f"""
                MERGE INTO simulation u
                USING (SELECT CAST(:json_data AS jsonb) as rep) as t
                ON ( client_id = :client_id and simulation_id = :simulation_id)
                WHEN MATCHED THEN
                UPDATE SET 
                    json_dado = t.rep
                    ,modified_by = 'system'
                    ,modified_date  = NOW()
                RETURNING
                    json_dado
            """)
        
        parametros = {
            "client_id": clientId,
            "simulation_id": simulationId,
            "json_data": rep_json
        }
        result = await db.execute(smtp, parametros)
        await db.commit()
        for row in result:
            return row.json_dado
        logger.warning(f'Reports finalizados!')

    res = []
    if perResource:
        resources = []
        for recurso in perResource['resources']:
            resources = [recurso['resource_id']]
            print(f"\n⚙️ Processando Resource ID: {recurso['resource_id']}")
            listJobs = []
            # 2. Agora fazemos um loop na chave 'jobs' DENTRO deste recurso específico
            for job in recurso['jobs']:
                listJobs.append(job['job_id'])
                # Pegamos o ID do job atual
                job_id = job['job_id']
                print(f"  ↳ Encontrou Job ID: {job_id}")
            res = await calc_rote()
    else:
        res = await calc_rote()

    return res


# Recursos por Team

@app.get("/resources/by-team")
async def getResourcesByTeam(
    team_id: int = Query(...),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    result = await db.execute(
        select(models.Resources)
        .join(
            models.TeamMembers,
            (models.TeamMembers.client_id == models.Resources.client_id) &
            (models.TeamMembers.resource_id == models.Resources.resource_id)
        )
        .where(
            models.TeamMembers.client_id == clientId,
            models.TeamMembers.team_id == team_id,
            models.Resources.client_id == clientId
        )
        .order_by(models.Resources.description)
    )
    resources = result.scalars().all()
    return [
        {"resource_id": r.resource_id, "client_resource_id": r.client_resource_id, "description": r.description}
        for r in resources
    ]


# CRUD Schedules

@app.get("/schedules", response_model=List[schemas.ScheduleResponse])
async def getSchedules(
    team_id: Optional[int] = Query(None),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId = current_user["userId"]
    query = select(models.Schedules).where(models.Schedules.client_id == clientId and models.Schedules.user_id == userId)
    if team_id is not None:
        query = query.where(models.Schedules.team_id == team_id)
    query = query.order_by(models.Schedules.schedule_id)
    result = await db.execute(query)
    return result.scalars().all()


@app.get("/schedules/{schedule_id}", response_model=schemas.ScheduleResponse)
async def getSchedule(
    schedule_id: int,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId = current_user["userId"]
    result = await db.execute(
        select(models.Schedules).where(
            models.Schedules.client_id == clientId,
            models.Schedules.user_id == userId,
            models.Schedules.schedule_id == schedule_id
        )
    )
    schedule = result.scalar_one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="Agendamento não encontrado")
    return schedule


@app.post("/schedules", response_model=schemas.ScheduleResponse, status_code=201)
async def createSchedule(
    body: schemas.ScheduleCreateRequest,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId = current_user["userId"]
    userName = current_user["userName"]
    now = datetime.now()

    new_schedule = models.Schedules(
        client_id=clientId,
        user_id=userId,
        team_id=body.team_id,
        schedule_start_date=body.schedule_start_date,
        schedule_start_time=body.schedule_start_time,
        next_schedule_date=body.next_schedule_date,
        frequency=body.frequency,
        type_resources=body.type_resources,
        update_tasks=body.update_tasks,
        resources=body.resources,
        status="A",
        created_by=str(userName),
        created_date=now,
        modified_by=str(userName),
        modified_date=now
    )
    db.add(new_schedule)
    await db.commit()
    await db.refresh(new_schedule)
    return new_schedule


@app.patch("/schedules/{schedule_id}", response_model=schemas.ScheduleResponse)
async def updateSchedule(
    schedule_id: int,
    body: schemas.ScheduleUpdateRequest,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId = current_user["userId"]
    userName = current_user["userName"]

    result = await db.execute(
        select(models.Schedules).where(
            models.Schedules.client_id == clientId,
            models.Schedules.user_id == userId,
            models.Schedules.schedule_id == schedule_id
        )
    )
    schedule = result.scalar_one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="Agendamento não encontrado")

    updates = body.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(schedule, field, value)

    schedule.modified_by = str(userName)
    schedule.modified_date = datetime.now()

    await db.commit()
    await db.refresh(schedule)
    return schedule


@app.delete("/schedules/{schedule_id}", status_code=204)
async def deleteSchedule(
    schedule_id: int,
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    clientId = current_user["clientId"]
    userId = current_user["userId"]
    userName = current_user["userName"]

    result = await db.execute(
        select(models.Schedules).where(
            models.Schedules.client_id == clientId,
            models.Schedules.user_id == userId,
            models.Schedules.schedule_id == schedule_id
        )
    )
    schedule = result.scalar_one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="Agendamento não encontrado")

    schedule.status = "C"
    schedule.modified_by = str(userName)
    schedule.modified_date = datetime.now()

    await db.commit()
