import traceback
import sys
import asyncio
import json
import polars as pl
from sqlalchemy import text
from database import SessionLocal
from services import optimize_routes_vroom, get_route_distance_block, logs , geocode_mapbox
from loguru import logger

def log_error(e: Exception, msg: str = "Erro"):
    tb = traceback.extract_tb(sys.exc_info()[2])
    if tb:
        d = tb[-1]
        logger.error(f"{msg}: {e} — {d.filename}, linha {d.lineno}, função {d.name}")
    else:
        logger.error(f"{msg}: {e}")

def getErrorDetails(e: Exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    
    # Extrai a última linha da pilha (onde o erro ocorreu)
    detalhes = traceback.extract_tb(exc_traceback)[-1]
    
    arquivo = detalhes.filename
    linha = detalhes.lineno
    funcao = detalhes.name
    return f"Erro {e} | Detalhes: {arquivo}, {linha} , {funcao}"


# async def buildAdjustmentScheduled():
#     try:
#         smtp = """
#             WITH qjobs AS (
#                 SELECT 
#                     j.*,
#                     COALESCE(j.actual_start_date, j.plan_start_date) as actual_date,
#                     bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_distance,
#                     bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_geo
#                 FROM jobs j
#                     JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
#                     JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
#                     JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
#                     JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
#                 WHERE (js.internal_code_status <> 'CONCLU' OR js.internal_code_status IS NULL)
#                     AND j.client_id = 1
#                     AND j.team_id = 33
#                     AND COALESCE(j.actual_start_date, j.plan_start_date) >= date_trunc('day',NOW())
#                     AND COALESCE(j.actual_start_date,j.plan_start_date) < date_trunc('day',NOW()) + interval '1 day'
                   
#                 ),
#                 q1 AS (
#                     select 
#                         * 
#                     from qjobs
#                     where null_distance = true
#                         AND null_geo = false
#                 )
#                 select 
#                     client_id, 
#                     team_id, 
#                     resource_id,
#                     DATE_TRUNC('day', actual_date) as actual_date, 
#                     count(*) total 
#                 from qjobs
#                 group by client_id, team_id, resource_id, DATE_TRUNC('day', actual_date)
#                 order by 1,2,3, 4
#         """)
#         async with SessionLocal() as db:
#             result = await db.execute(smtp)
#             rows = result.mappings().all()
#             if not rows or len(rows) ==0:
#                 return
#             for row in rows:
#                 clientId = row.client_id
#                 await logs(clientId=clientId,log='jobs',logJson=row)
#                 teamId = row.team_id
#                 resourceId = row.resource_id
#                 actualDate = row.actual_date

#                 logger.info("Iniciado Calculo das rotas ...")
#                 smtp = text(f"""
#                     WITH dados AS (
#                         SELECT 
#                             j.client_id
#                             ,t.team_id
#                             ,300 time_overlap
#                             ,j.job_id
#                             ,j.address_id
#                             ,a.geocode_lat::NUMERIC geocode_lat
#                             ,a.geocode_long::NUMERIC geocode_long
#                             ,COALESCE (j.time_setup, jt.time_setup, t.time_setup) AS time_setup
#                             ,COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service
#                             ,j.priority + (COALESCE (jt.priority, 0) / 100)::INTEGER AS priority
#                             ,EXTRACT (epoch FROM COALESCE (aw.start_time, CAST ('00:00:00' AS TIME)))::INTEGER AS start_time
#                             --,0 AS start_time
#                             ,EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59' AS TIME)))::INTEGER AS end_time
#                         FROM jobs  j
#                             JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
#                             JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
#                             JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
#                             JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
#                             LEFT JOIN address_windows aw
#                             ON     aw.client_id = j.client_id
#                                 AND aw.address_id = j.address_id
#                                 AND aw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
#                             where j.client_id = :client_id
#                                 AND j.team_id = :team_id
#                                 AND a.geocode_lat is not null
#                                 AND a.geocode_long is not null
#                                 and (a.geocode_lat::NUMERIC) < 100
#                                 AND (a.geocode_long::NUMERIC) < 100
#                                 and j.resource_id = :resource_id
#                                 and (js.internal_code_status <> 'CONCLU' OR js.internal_code_status IS NULL)
#                                 and COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
#                                 AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'),
#                     MapeamentoEnderecos AS (
#                         SELECT 
#                             *,
#                             -- 1. Conta o total de jobs para o mesmo address_id (e client_id, por segurança)
#                             COUNT(job_id) OVER (
#                                 PARTITION BY client_id, team_id, geocode_lat, geocode_long
#                             ) AS quantidade_jobs_mesmo_endereco,
                            
#                             -- 2. Cria um ranking ordenando pelo menor job_id
#                             ROW_NUMBER() OVER (
#                                 PARTITION BY client_id, team_id, geocode_lat, geocode_long
#                                 ORDER BY job_id ASC
#                             ) AS ordem_job
#                         FROM dados
#                     ),
#                     list as (
#                         SELECT json_agg(job_id) AS job_id_list
#                             FROM dados
#                     ),
#                     q1 as (
#                         SELECT 
#                         client_id,
#                         team_id,
#                         job_id,
#                         address_id,
#                         geocode_lat,
#                         geocode_long,
#                         time_setup,
#                         time_service + (time_overlap * (quantidade_jobs_mesmo_endereco -1)) as time_service,
#                         priority,
#                         start_time,
#                         end_time
#                         FROM MapeamentoEnderecos
#                         WHERE ordem_job = 1
#                     ),
#                     last_task as(
#                         select 
#                             j.resource_id, 
#                             a.geocode_lat, 
#                             a.geocode_long,
#                             j.actual_end_date, 
#                             j.plan_end_date
#                         from jobs j
#                         join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
#                         join teams t on t.client_id = j.client_id and t.team_id = j.team_id
#                         join address a on a.client_id = j.client_id and a.address_id = j.address_id
#                         where j.client_id = :client_id
#                             and j.team_id = :team_id
#                             and j.resource_id = :resource_id
#                             and js.internal_code_status = 'CONCLU'
#                             and COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
#                                             AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'
#                             order by j.team_id, j.resource_id,j.actual_end_date desc, j.plan_end_date desc
#                         limit 1
#                     ),
#                     vehicles_data AS (
#                         select
#                             r.resource_id,
#                             r.description,
#                             COALESCE(l.geocode_lat,r.geocode_lat_from)::NUMERIC as geocode_lat_from,
#                             COALESCE(l.geocode_long,r.geocode_long_from)::NUMERIC as geocode_long_from,
#                             r.geocode_lat_at::NUMERIC,
#                             r.geocode_long_at::NUMERIC,
#                             CASE WHEN l.actual_end_date IS NULL THEN
#                                 EXTRACT(EPOCH FROM COALESCE(rw.start_time,t.start_time)) ::INTEGER
#                             ELSE
#                                 EXTRACT(EPOCH FROM l.actual_end_date - DATE_TRUNC('day',l.actual_end_date)) ::INTEGER
#                             END AS start_time,
#                             EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then COALESCE(rw.end_time,t.end_time) else cast('23:59:59' as time) end ) ::INTEGER AS end_time
#                         from resources r
#                             join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
#                             join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
#                             LEFT JOIN resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM :p_date) + 1
#                             left join last_task l on r.resource_id = l.resource_id
#                         where r.client_id = :client_id
#                             and t.team_id = :team_id
#                             and r.resource_id = :resource_id
#                     ),
                                
#                     vehicles_json AS (
#                             SELECT json_agg(
#                             json_strip_nulls(
#                                 json_build_object(
#                                     'id', resource_id,
#                                     'description', description,
#                                     'start', json_build_array(geocode_long_from, geocode_lat_from),
#                                     'end', json_build_array(geocode_long_at, geocode_lat_at),
#                                     'time_window', json_build_array(start_time, end_time)
#                                 ))
#                             ) AS array_vehicles
#                             FROM vehicles_data
#                     ),
#                     jobs_json AS (
#                         -- Agrupa todos os jobs em um array JSON
#                         SELECT json_agg(
#                         json_strip_nulls(
#                             json_build_object(
#                                 'id', job_id,
#                                 'location', json_build_array(geocode_long, geocode_lat),
#                                 'setup', time_setup,
#                                 'service', time_service,
#                                 'priority', priority,
#                                 'time_windows', json_build_array(json_build_array(start_time, end_time)) 
#                             ))
#                         ) AS array_jobs
#                         FROM q1
#                     )
#                     SELECT json_build_object(
#                         'vehicles', (SELECT array_vehicles FROM vehicles_json),
#                         'jobs', (SELECT array_jobs FROM jobs_json),
#                         'list', (SELECT job_id_list FROM list)
#                     ) AS vroom_payload;
#                 """).bindparams(client_id=clientId, p_date = actualDate, team_id = teamId, resource_id = resourceId)
                
#                 result = await db.execute(smtp)
#                 subRows = result.mappings().all()
#                 if not subRows or len(subRows) == 0:
#                     return
#                 # await logs(clientId=clientId,log='dados para vroom',logJson=dict(subRows[0]))
#                 vroom_payload = subRows[0]['vroom_payload']

#                 listJobs = vroom_payload.pop('list', 'Chave não encontrada')
#                 logger.info(list(listJobs))
                
#                 # logger.info(vroom_payload)

#                 logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
#                 retorno = await optimize_routes_vroom(vroom_payload)
#                 routes = retorno['routes']
#                 for rou in routes:
#                     # print(rou)
#                     # print('--------------------------------------')
#                     steps = rou['steps']
#                     geo = []
#                     for step in steps:
#                         geo.append([step['location'][0],step['location'][1]])
#                     # print(geo)
#                     # print('---------------------------------------')
#                     # await logs(clientId=clientId,log='montagem do geo',logJson=geo)
#                     geoResult = await get_route_distance_block(geo)
#                     # await logs(clientId=clientId,log='retorno do geo',logJson=geoResult)
#                     # print(geoResult) if r=='BRAC' else None
#                     ln = 1
#                     step = rou['steps'][0]
#                     step['time_distance'] = 0
#                     step['distance'] = 0
#                     for x in geoResult:
#                         # print(x['duration'],x['distance'])
#                         step = rou['steps'][ln]
#                         step['time_distance'] = x['duration']
#                         step['distance'] = x['distance']
#                         # print(ln,' - ',step)
#                         ln += 1
#                 listIds = [item['id'] for item in retorno.get("unassigned", [])]
#                 if listIds:
#                     # await logs(clientId=clientId,log='lista de unassigned',logJson=listIds)
#                     logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
                
#                 await logs(clientId=clientId,log='retorno do vroom Ajustment',logJson=retorno)
#                 vroomResult = json.dumps(retorno)
#                 smtp = text(f"""
#                     WITH payload_data AS (
#                         SELECT '{vroomResult}'::jsonb AS data
#                     ),
#                     rotas AS (
#                         SELECT 
#                             (rota_json->>'vehicle')::int AS id_veiculo,
#                             rota_json->>'description' AS nome_motorista,
#                             rota_json->'steps' AS passos_array
#                         FROM payload_data,
#                             jsonb_array_elements(data->'routes') AS rota_json
#                     ),
#                     q1 as (
#                         SELECT
#                             r.id_veiculo::int AS resource_id,
#                             ordem_passo::int AS sequence,
#                             passo_json->>'type' AS stop_type,
#                             (passo_json->>'id')::int AS job_id,
#                             (passo_json->'location'->>0)::numeric AS geocode_long,
#                             (passo_json->'location'->>1)::numeric AS geocode_lat,
#                             (passo_json->>'arrival')::int as arrival,
#                             (passo_json->>'service')::int AS service,
#                             (passo_json->>'setup')::int AS setup,
#                             (passo_json->>'distance')::numeric::int AS distance,
#                             (passo_json->>'time_distance')::numeric::int AS time_distance
#                         FROM rotas r,
#                             jsonb_array_elements(r.passos_array) WITH ORDINALITY AS passo(passo_json, ordem_passo)
#                     ),
#                     dados_jobs as(
#                         select  j.client_id,
#                                 j.team_id,
#                                 j.job_id,
#                                 300 time_overlap,
#                                 j.client_job_id,
#                                 js.description AS status_description,
#                                 jt.description as type_description,
#                                 CASE WHEN j.job_id <>  FIRST_VALUE(job_id) OVER (PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC)
#                                 THEN FIRST_VALUE(job_id) OVER (PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC)
#                                 ELSE j.job_id
#                                 END AS new_job_id,
#                                 j.job_type_id,
#                                 j.job_status_id,
#                                 COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service,
#                                 ROW_NUMBER() OVER (
#                                 PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long
#                                 ORDER BY job_id ASC
#                             ) AS ordem_job,
#                                 j.place_id,
#                                 j.address_id,
#                                 a.geocode_lat, 
#                                 a.geocode_long,
#                                 a.address, 
#                                 a.city, 
#                                 a.state_prov, 
#                                 a.zippost,
#                                 p.trade_name, 
#                                 p.cnpj
#                         from jobs j
#                         join address a on a.client_id = j.client_id and a.address_id = j.address_id
#                         join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
#                         join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
#                         JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
#                         join places p on p.client_id = j.client_id and p.place_id = j.place_id
#                         where j.client_id = :client_id
#                             and j.job_id IN ({','.join(str(j) for j in listJobs)})
#                     ),
#                     dados as(
#                         select 
#                             r.client_id,
#                             r.resource_id,
#                             r.client_resource_id,
#                             j.team_id,
#                             j.client_job_id,
#                             r.description resource_name,
#                             j.status_description,
#                             j.type_description,
#                             j.address, 
#                             j.city, 
#                             j.state_prov, 
#                             j.zippost, 
#                             j.trade_name, 
#                             j.cnpj,
#                             :p_date job_day,
#                             q_1.geocode_long AS geocode_long_from,
#                             q_1.geocode_lat AS  geocode_lat_from,
#                             q_1.arrival AS arrival_from,
#                             q_3.distance AS distance_from,
#                             q_3.time_distance as time_distance_from,
                            
#                             q_2.geocode_long AS geocode_long_at,
#                             q_2.geocode_lat AS  geocode_lat_at,
#                             q_2.arrival AS arrival_at,
#                             q_2.distance AS distance_at,
#                             q_2.time_distance as time_distance_at,
                            
#                             j.job_id,
#                             q.geocode_long,
#                             q.geocode_lat,
#                             q.setup,
#                             --q.arrival,
#                             j.ordem_job,
#                             CASE 
#                                 WHEN j.ordem_job > 1
#                                 THEN
#                                     q.arrival + (j.time_overlap * (j.ordem_job-1))
#                                 ELSE
#                                 q.arrival
#                             end AS arrival,
#                             j.time_service AS service,      
#                             --q.service,
#                             CASE 
#                                 WHEN j.ordem_job > 1
#                                 THEN
#                                     0
#                                 ELSE
#                                 q.distance
#                             end AS distance,
#                             --q.distance,
#                             CASE 
#                                 WHEN j.ordem_job > 1
#                                 THEN
#                                     0
#                                 ELSE
#                                 q.time_distance
#                             end AS time_distance
#                             --q.time_distance
#                         from q1 q
#                         join dados_jobs j ON j.client_id = 1 and j.new_job_id = q.job_id
#                         join resources r ON r.client_id = 1 and r.resource_id = q.resource_id
#                         join q1 q_1 on q_1.resource_id = q.resource_id and q_1.stop_type = 'start'
#                         join q1 q_2 on q_2.resource_id = q.resource_id and q_2.stop_type = 'end'
#                         join q1 q_3 on q_3.resource_id = q.resource_id and q_3.stop_type = 'job' AND q_3.sequence = 2
#                     ),
#                     dados_new as (
#                         select 
#                         a.*,
#                         a.job_day + (arrival *  INTERVAL '1 second') start_date,
#                         a.job_day + ((arrival + setup + service) *  INTERVAL '1 second') end_date,
#                         a.job_day + ((arrival_from) *  INTERVAL '1 second') date_from,
#                         a.job_day + ((arrival_at) *  INTERVAL '1 second') date_at
#                         from dados a
#                     )
#                     update jobs j
#                         set ajustment_start_date = d.start_date
#                             ,ajustment_end_date = d.end_date
#                         from dados_new d
#                         where j.client_id = d.client_id
#                         and j.job_id = d.job_id
#             """).bindparams(client_id=clientId,p_date = actualDate)
#                 result = await db.execute(smtp)
#                 await db.commit()
#         return
#     except Exception as e:
#         exc_type, exc_value, exc_traceback = sys.exc_info()
    
#         # Extrai a última linha da pilha (onde o erro ocorreu)
#         detalhes = traceback.extract_tb(exc_traceback)[-1]
        
#         arquivo = detalhes.filename
#         linha = detalhes.lineno
#         funcao = detalhes.name
#         logger.error(f"Erro {e}")        
#         logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")            

async def _build_report_type(r: str, clientId: int, teamId: int, actualDate, dbd) -> list:
    vBrac = """
    AND EXISTS (
        SELECT 1
        FROM jobs x
            JOIN job_status xs ON xs.client_id = x.client_id AND xs.job_status_id = x.job_status_id
        WHERE x.client_id = r.client_id
            and x.resource_id = r.resource_id
            and xs.internal_code_status = 'CONCLU'
            AND COALESCE(x.actual_start_date,x.plan_start_date) >= $p_date
            AND COALESCE(x.actual_start_date,x.plan_start_date) < $p_date + interval '1 day'
    )
    """ if r == 'BRAC' else ''

    logger.warning(f'Iniciando... {r} em {actualDate}')
    logger.info("Iniciado Calculo das rotas ...")
    stmt = f"""
        WITH dados AS (
            SELECT
                j.client_id,
                t.team_id,
                COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap, 0) AS time_overlap,
                j.job_id,
                j.address_id,
                a.geocode_lat::NUMERIC AS geocode_lat,
                a.geocode_long::NUMERIC AS geocode_long,
                COALESCE(j.time_setup, jt.time_setup, t.time_setup,0) AS time_setup,
                COALESCE(j.time_service, jt.time_service, t.time_service,0) AS time_service,
                j.priority + (COALESCE(jt.priority, 0) / 100)::INTEGER AS priority,
                EXTRACT(EPOCH FROM COALESCE(aw.start_time, CAST('00:00:00' AS TIME)))::INTEGER AS start_time,
                EXTRACT(EPOCH FROM COALESCE(aw.end_time, CAST('23:59:59' AS TIME)))::INTEGER AS end_time
            FROM jobs j
            JOIN job_status js
                ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
            JOIN job_types jt
                ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
            JOIN address a
                ON a.client_id = j.client_id AND a.address_id = j.address_id
            JOIN teams t
                ON t.client_id = j.client_id AND t.team_id = j.team_id
            LEFT JOIN address_windows aw
                ON aw.client_id = j.client_id
                AND aw.address_id = j.address_id
                AND aw.week_day = EXTRACT(DOW FROM COALESCE(j.actual_start_date, j.plan_start_date, NOW())) + 1
            WHERE COALESCE(j.actual_start_date, j.plan_start_date) >= $p_date
                AND COALESCE(j.actual_start_date, j.plan_start_date) < $p_date + INTERVAL 1 DAY
                AND j.client_id = $client_id
                AND j.team_id = $team_id
                AND js.internal_code_status = 'CONCLU'
        ),
        MapeamentoEnderecos AS (
            SELECT
                *,
                COUNT(job_id) OVER (
                    PARTITION BY client_id, team_id, geocode_lat, geocode_long
                ) AS quantidade_jobs_mesmo_endereco,
                ROW_NUMBER() OVER (
                    PARTITION BY client_id, team_id, geocode_lat, geocode_long
                    ORDER BY job_id ASC
                ) AS ordem_job
            FROM dados
        ),
        list AS (
            SELECT json_group_array(job_id) AS job_id_list
            FROM dados
        ),
        q1 AS (
            SELECT
                client_id,
                team_id,
                job_id,
                address_id,
                geocode_lat,
                geocode_long,
                time_setup,
                time_service + (time_overlap * (quantidade_jobs_mesmo_endereco - 1)) AS time_service,
                priority,
                start_time,
                end_time
            FROM MapeamentoEnderecos
            WHERE ordem_job = 1
        ),
        vehicles_data AS (
            SELECT
                r.resource_id,
                r.description,
                COALESCE(r.geocode_lat_from, t.geocode_lat)::NUMERIC AS geocode_lat_from,
                COALESCE(r.geocode_long_from, t.geocode_long)::NUMERIC AS geocode_long_from,
                COALESCE(r.geocode_lat_at, t.geocode_lat)::NUMERIC AS geocode_lat_at,
                COALESCE(r.geocode_long_at, t.geocode_long)::NUMERIC AS geocode_long_at,
                EXTRACT(EPOCH FROM COALESCE(rw.start_time, t.start_time))::INTEGER AS start_time,
                EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 THEN COALESCE(rw.end_time, t.end_time) ELSE CAST('23:59:59' AS TIME) END)::INTEGER AS end_time
            FROM resources r
            JOIN team_members tm
                ON tm.client_id = r.client_id AND tm.resource_id = r.resource_id
            JOIN teams t
                ON t.client_id = tm.client_id AND t.team_id = tm.team_id
            LEFT JOIN resource_windows rw
                ON rw.client_id = r.client_id AND rw.resource_id = r.resource_id AND rw.week_day = EXTRACT(DOW FROM $p_date) + 1
            WHERE r.client_id = $client_id
                AND t.team_id = $team_id
                {vBrac}
        ),
        vehicles_json AS (
            SELECT json_group_array(
                json_object(
                    'id', resource_id,
                    'description', description,
                    'start', json_array(geocode_long_from, geocode_lat_from),
                    'end', json_array(geocode_long_at, geocode_lat_at),
                    'time_window', json_array(start_time, end_time)
                )
            ) AS array_vehicles
            FROM vehicles_data
        ),
        jobs_json AS (
            SELECT json_group_array(
                json_object(
                    'id', job_id,
                    'location', json_array(geocode_long, geocode_lat),
                    'setup', time_setup,
                    'service', time_service,
                    'priority', priority,
                    'time_windows', json_array(json_array(start_time, end_time))
                )
            ) AS array_jobs
            FROM q1
        )
        SELECT json_object(
            'vehicles', (SELECT array_vehicles FROM vehicles_json),
            'jobs', (SELECT array_jobs FROM jobs_json),
            'list', (SELECT job_id_list FROM list)
        ) AS vroom_payload;
    """
    payload = json.loads(dbd.execute(stmt, {"client_id": clientId, "team_id": teamId, "p_date": actualDate}).fetchone()[0])
    if not payload:
        return []
    asyncio.create_task(logs(clientId=clientId, log='dados para vroom', logJson=payload))
    listJobs = payload.pop('list', 'Chave não encontrada')
    logger.info(list(listJobs))

    logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
    retorno = await optimize_routes_vroom(payload)
    logger.info('terminou vroomm....')
    routes = retorno['routes']
    geo_lists = [
        [[s['location'][0], s['location'][1]] for s in rou['steps']]
        for rou in routes
    ]
    logger.info('terminou geolist....')
    osrm_results = await asyncio.gather(
        *[get_route_distance_block(geo) for geo in geo_lists]
    )
    for rou, geoResult in zip(routes, osrm_results):
        rou['steps'][0]['time_distance'] = 0
        rou['steps'][0]['distance'] = 0
        for ln, x in enumerate(geoResult, start=1):
            rou['steps'][ln]['time_distance'] = x['duration']
            rou['steps'][ln]['distance'] = x['distance']
    listIds = [item['id'] for item in retorno.get("unassigned", [])]
    if listIds:
        asyncio.create_task(logs(clientId=clientId, log='lista de unassigned', logJson=listIds))
        logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
    asyncio.create_task(logs(clientId=clientId, log='retorno do vroom', logJson=retorno))
    logger.info('Terminou retorno ....')

    stmt = """
        WITH payload_data AS (
            SELECT CAST($vroom_data AS JSON) AS data
        ),
        rotas AS (
            SELECT
                (rota_json->>'vehicle')::INT AS id_veiculo,
                rota_json->>'description' AS nome_motorista,
                rota_json->'steps' AS passos_array
            FROM payload_data,
                UNNEST(CAST(data->'routes' AS JSON[])) AS t(rota_json)
        ),
        q1 AS (
            SELECT
                r.id_veiculo::INT AS resource_id,
                ordem_passo::INT AS sequence,
                passo_json->>'type' AS stop_type,
                (passo_json->>'id')::INT AS job_id,
                (passo_json->'location'->>0)::NUMERIC AS geocode_long,
                (passo_json->'location'->>1)::NUMERIC AS geocode_lat,
                (passo_json->>'arrival')::INT as arrival,
                (passo_json->>'service')::INT AS service,
                (passo_json->>'setup')::INT AS setup,
                (passo_json->>'distance')::NUMERIC::INT AS distance,
                (passo_json->>'time_distance')::NUMERIC::INT AS time_distance
            FROM rotas r,
                UNNEST(CAST(r.passos_array AS JSON[])) WITH ORDINALITY AS passo(passo_json, ordem_passo)
        ),
        dados_jobs AS (
            SELECT  j.client_id,
                    j.team_id,
                    j.job_id,
                    $p_date AS job_day,
                    COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap, 0) AS time_overlap,
                    j.client_job_id,
                    js.description AS status_description,
                    jt.description as type_description,
                    CASE WHEN j.job_id <> FIRST_VALUE(job_id) OVER (PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC)
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
            FROM jobs j
            JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
            JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
            JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
            JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
            JOIN places p ON p.client_id = j.client_id AND p.place_id = j.place_id
            WHERE j.client_id = $client_id
                AND list_contains($vroom_job_list, j.job_id)
        ),
        dados AS (
            SELECT
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
                q_1.geocode_long AS geocode_long_from,
                q_1.geocode_lat AS  geocode_lat_from,
                q_1.arrival AS arrival_from,
                q_3.distance AS distance_from,
                q_3.time_distance as time_distance_from,
                q_2.geocode_long AS geocode_long_at,
                q_2.geocode_lat AS  geocode_lat_at,
                q_2.arrival AS arrival_at,
                q_2.distance AS distance_at,
                q_2.time_distance as time_distance_at,
                j.job_id,
                j.time_overlap,
                q.geocode_long,
                q.geocode_lat,
                q.setup,
                j.ordem_job,
                CASE
                    WHEN j.ordem_job > 1 THEN q.arrival + (j.time_overlap * (j.ordem_job-1))
                    ELSE q.arrival
                END AS arrival,
                j.time_service AS service,
                CASE
                    WHEN j.ordem_job > 1 THEN 0
                    ELSE q.distance
                END AS distance,
                CASE
                    WHEN j.ordem_job > 1 THEN 0
                    ELSE q.time_distance
                END AS time_distance
            FROM q1 q
            JOIN dados_jobs j ON j.client_id = $client_id AND j.new_job_id = q.job_id
            JOIN resources r ON r.client_id = $client_id AND r.resource_id = q.resource_id
            JOIN q1 q_1 ON q_1.resource_id = q.resource_id AND q_1.stop_type = 'start'
            JOIN q1 q_2 ON q_2.resource_id = q.resource_id AND q_2.stop_type = 'end'
            JOIN q1 q_3 ON q_3.resource_id = q.resource_id AND q_3.stop_type = 'job' AND q_3.sequence = 2
        ),
        dados_new AS (
            SELECT
                a.*,
                a.job_day + (arrival * INTERVAL 1 SECOND) AS start_date,
                a.job_day + ((arrival + setup + service) * INTERVAL 1 SECOND) AS end_date,
                a.job_day + (arrival_from * INTERVAL 1 SECOND) AS date_from,
                a.job_day + (arrival_at * INTERVAL 1 SECOND) AS date_at
            FROM dados a
        ),
        grp AS (
            SELECT
                q.client_id,
                q.resource_id,
                q.client_resource_id,
                q.resource_name,
                q.job_day,
                q.geocode_long_from,
                q.geocode_lat_from,
                q.distance_from,
                q.time_distance_from,
                q.arrival_from,
                q.date_from,
                q.geocode_long_at,
                q.geocode_lat_at,
                q.distance_at,
                q.time_distance_at,
                q.arrival_at,
                q.date_at,
                list(
                    json_object(
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
                        'geocode_lang', q.geocode_long,
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
            FROM dados_new q
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
        )
        SELECT
            list(
                json_object(
                    'client_id', client_id,
                    'resource_id', resource_id,
                    'client_resource_id', client_resource_id,
                    'resource_name', resource_name,
                    'job_day', job_day,
                    'geocode_long_from', geocode_long_from,
                    'geocode_lat_from', geocode_lat_from,
                    'distance_from', distance_from,
                    'time_distance_from', time_distance_from,
                    'arrival_from', arrival_from,
                    'date_from', date_from,
                    'geocode_long_at', geocode_long_at,
                    'geocode_lat_at', geocode_lat_at,
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
        FROM grp
    """
    result = dbd.execute(stmt, {
        'vroom_data': json.dumps(retorno),
        'client_id': clientId,
        'p_date': actualDate,
        'vroom_job_list': listJobs
    }).fetchone()[0]
    if not result:
        return []
    asyncio.create_task(logs(clientId=clientId, log=f'Resultado do {r}', logJson=result))
    logger.warning(f'Finalizado ... {r}')
    return result if isinstance(result, list) else [result]


async def _build_real_report(clientId: int, teamId: int, actualDate, dbd) -> list:
    stmt = """
        WITH dados AS (
            SELECT
                r.client_id,
                j.team_id,
                r.resource_id,
                j.job_id,
                r.client_resource_id,
                r.description resource_name,
                j.client_job_id,
                js.description AS status_description,
                jt.description AS type_description,
                a.address,
                a.city,
                a.state_prov,
                a.zippost,
                p.trade_name,
                p.cnpj,
                DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date)) AS job_day,
                r.geocode_long_from,
                r.geocode_lat_from,
                r.geocode_long_at,
                r.geocode_lat_at,
                FIRST_VALUE(COALESCE(j.actual_start_date, j.plan_start_date)) OVER (
                    PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date))
                    ORDER BY COALESCE(j.actual_start_date, j.plan_start_date) ASC
                ) AS first_start_date,
                FIRST_VALUE(COALESCE(j.actual_end_date, j.plan_end_date)) OVER (
                    PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_end_date, j.plan_end_date))
                    ORDER BY COALESCE(j.actual_end_date, j.plan_end_date) DESC
                ) AS last_end_date,
                j.first_distance AS distance_from,
                j.first_time_distance AS time_distance_from,
                j.last_distance AS distance_at,
                j.last_time_distance AS time_distance_at,
                a.geocode_long,
                a.geocode_lat,
                EXTRACT(EPOCH FROM (COALESCE(j.actual_start_date, j.plan_start_date) - DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date))))::INTEGER AS arrival,
                EXTRACT(EPOCH FROM (COALESCE(j.actual_end_date, j.plan_end_date) - COALESCE(j.actual_start_date, j.plan_start_date)))::INTEGER AS time_service,
                j.distance,
                j.time_distance,
                COALESCE(j.actual_start_date, j.plan_start_date) AS start_date,
                COALESCE(j.actual_end_date, j.plan_end_date) AS end_date
            FROM jobs j
            JOIN resources r ON r.client_id = j.client_id AND r.resource_id = j.resource_id
            JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
            JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
            JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
            JOIN places p ON p.client_id = j.client_id AND p.place_id = j.place_id
            WHERE COALESCE(j.actual_start_date, j.plan_start_date) >= $p_date
                AND COALESCE(j.actual_start_date, j.plan_start_date) < $p_date + INTERVAL 1 DAY
                AND js.internal_code_status = 'CONCLU'
                AND a.geocode_lat IS NOT NULL
                AND a.geocode_long IS NOT NULL
                AND (a.geocode_lat::NUMERIC) < 100
                AND (a.geocode_long::NUMERIC) < 100
                AND j.client_id = $client_id
                AND j.team_id = $team_id
        ),
        dados_calc AS (
            SELECT
                c.*,
                EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from AS arrival_from,
                EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at AS arrival_at,
                c.job_day + (EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from) * INTERVAL 1 SECOND AS date_from,
                c.job_day + (EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at) * INTERVAL 1 SECOND AS date_at
            FROM dados c
        ),
        grp AS (
            SELECT
                d.client_id, d.resource_id, d.client_resource_id, d.resource_name, d.job_day,
                d.geocode_long_from, d.geocode_lat_from, d.distance_from, d.time_distance_from,
                d.arrival_from, d.date_from, d.geocode_long_at, d.geocode_lat_at, d.distance_at,
                d.time_distance_at, d.arrival_at, d.date_at,
                list(
                    json_object(
                        'client_id', d.client_id, 'job_id', d.job_id, 'team_id', d.team_id,
                        'client_job_id', d.client_job_id, 'status_description', d.status_description,
                        'type_description', d.type_description, 'address', d.address,
                        'city', d.city, 'state_prov', d.state_prov, 'zippost', d.zippost,
                        'trade_name', d.trade_name, 'cnpj', d.cnpj, 'job_day', d.job_day,
                        'geocode_lang', d.geocode_long, 'geocode_lat', d.geocode_lat,
                        'arrival', d.arrival, 'service', d.time_service,
                        'distance', d.distance, 'time_distance', d.time_distance,
                        'start_date', d.start_date, 'end_date', d.end_date
                    ) ORDER BY d.start_date
                ) AS jobs,
                sum(d.distance) + max(d.distance_at) AS total_distance,
                sum(d.time_distance) + max(d.time_distance_at) AS total_time_distance,
                count(d.job_id) AS total_jobs
            FROM dados_calc d
            GROUP BY
                d.client_id, d.resource_id, d.client_resource_id, d.resource_name,
                d.job_day, d.geocode_long_from, d.geocode_lat_from, d.distance_from,
                d.time_distance_from, d.arrival_from, d.date_from, d.geocode_long_at,
                d.geocode_lat_at, d.distance_at, d.time_distance_at, d.arrival_at, d.date_at
        )
        SELECT
            list(
                json_object(
                    'client_id', client_id, 'resource_id', resource_id,
                    'client_resource_id', client_resource_id, 'resource_name', resource_name,
                    'job_day', job_day, 'geocode_long_from', geocode_long_from,
                    'geocode_lat_from', geocode_lat_from, 'distance_from', distance_from,
                    'time_distance_from', time_distance_from, 'arrival_from', arrival_from,
                    'date_from', date_from, 'geocode_long_at', geocode_long_at,
                    'geocode_lat_at', geocode_lat_at, 'distance_at', distance_at,
                    'time_distance_at', time_distance_at, 'arrival_at', arrival_at,
                    'date_at', date_at, 'total_distance', total_distance,
                    'total_time_distance', total_time_distance, 'total_jobs', total_jobs,
                    'jobs', jobs
                )
            ) AS res
        FROM grp
    """
    row = dbd.execute(stmt, {"client_id": clientId, "p_date": actualDate, "team_id": teamId}).fetchone()[0]
    if not row:
        return []
    asyncio.create_task(logs(clientId=clientId, log='Resultado do REAL', logJson=row))
    logger.warning('Finalizado ... REAL')
    return row if isinstance(row, list) else [row]


async def buildReports(clientId: int, dbd, db):
    try:
        logger.warning(f'Iniciando geração de relatórios para o cliente {clientId}')    
        stmt = """
            WITH qjobs AS (
                SELECT 
                    j.*,
                    t.client_team_id,
                    COALESCE(j.actual_start_date, j.plan_start_date) as actual_date,
                    bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_distance,
                    bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_geo
                FROM jobs j
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                    JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
                WHERE js.internal_code_status = 'CONCLU'
                    AND j.client_id = $client_id
                    AND COALESCE(j.actual_start_date, j.plan_start_date) < date_trunc('day',NOW())
                    AND NOT EXISTS (SELECT 
                                        1
                                    FROM reports rp
                                    WHERE  rp.client_id = j.client_id
                                        AND rp.client_team_id = t.client_team_id
                                        AND COALESCE(j.actual_start_date, j.plan_start_date) >= rp.report_date 
                                        AND COALESCE(j.actual_start_date, j.plan_start_date) < rp.report_date + interval 1 day
                                        AND rp.rebuild = 0
                                    )
                )
                select
                    client_id,
                    team_id,
                    client_team_id,
                    DATE_TRUNC('day', actual_date) as actual_date, 
                    count(*) total 
                from qjobs
                group by client_id, DATE_TRUNC('day', actual_date), team_id, client_team_id
                order by client_id, DATE_TRUNC('day', actual_date), team_id, client_team_id
        """
        output = None
        result = dbd.execute(stmt, {"client_id": clientId}).pl().to_dicts()
        for row in result:
            teamId = row['team_id']
            clientTeamId = row['client_team_id']
            actualDate = row['actual_date']
            total = row['total']
            logger.warning(f'Gerando relatório para o cliente {clientId} - equipe {teamId} - data {actualDate} - total de jobs {total}')


            braa_results, brac_results, real_results = await asyncio.gather(
                _build_report_type('BRAA', clientId, teamId, actualDate, dbd),
                _build_report_type('BRAC', clientId, teamId, actualDate, dbd),
                _build_real_report(clientId, teamId, actualDate, dbd),
            )
            if not real_results:
                continue
            if False:
                _real_stmt_unused = """
                WITH dados AS (
                    SELECT
                        r.client_id,
                        j.team_id,
                        r.resource_id,
                        j.job_id,
                        r.client_resource_id,
                        r.description resource_name,
                        j.client_job_id,
                        js.description AS status_description,
                        jt.description AS type_description,
                        a.address,
                        a.city,
                        a.state_prov,
                        a.zippost,
                        p.trade_name,
                        p.cnpj,
                        DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date)) AS job_day,
                        r.geocode_long_from,
                        r.geocode_lat_from,
                        r.geocode_long_at,
                        r.geocode_lat_at,
                        FIRST_VALUE(COALESCE(j.actual_start_date, j.plan_start_date)) OVER (
                            PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date))
                            ORDER BY COALESCE(j.actual_start_date, j.plan_start_date) ASC
                        ) AS first_start_date,
                        FIRST_VALUE(COALESCE(j.actual_end_date, j.plan_end_date)) OVER (
                            PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_end_date, j.plan_end_date))
                            ORDER BY COALESCE(j.actual_end_date, j.plan_end_date) DESC
                        ) AS last_end_date,
                        j.first_distance AS distance_from,
                        j.first_time_distance AS time_distance_from,
                        j.last_distance AS distance_at,
                        j.last_time_distance AS time_distance_at,
                        a.geocode_long,
                        a.geocode_lat,
                        EXTRACT(EPOCH FROM (COALESCE(j.actual_start_date, j.plan_start_date) - DATE_TRUNC('day', COALESCE(j.actual_start_date, j.plan_start_date))))::INTEGER AS arrival,
                        EXTRACT(EPOCH FROM (COALESCE(j.actual_end_date, j.plan_end_date) - COALESCE(j.actual_start_date, j.plan_start_date)))::INTEGER AS time_service,
                        j.distance,
                        j.time_distance,
                        COALESCE(j.actual_start_date, j.plan_start_date) AS start_date,
                        COALESCE(j.actual_end_date, j.plan_end_date) AS end_date
                    FROM jobs j
                    JOIN resources r ON r.client_id = j.client_id AND r.resource_id = j.resource_id
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN places p ON p.client_id = j.client_id AND p.place_id = j.place_id
                    WHERE COALESCE(j.actual_start_date, j.plan_start_date) >= $p_date
                        AND COALESCE(j.actual_start_date, j.plan_start_date) < $p_date + INTERVAL 1 DAY
                        AND js.internal_code_status = 'CONCLU'
                        AND a.geocode_lat IS NOT NULL
                        AND a.geocode_long IS NOT NULL
                        AND (a.geocode_lat::NUMERIC) < 100
                        AND (a.geocode_long::NUMERIC) < 100
                        AND j.client_id = $client_id
                        AND j.team_id = $team_id
                ),
                dados_calc AS (
                    SELECT
                        c.*,
                        EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from AS arrival_from,
                        EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at AS arrival_at,
                        c.job_day + (EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from) * INTERVAL 1 SECOND AS date_from,
                        c.job_day + (EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at) * INTERVAL 1 SECOND AS date_at
                    FROM dados c
                ),
                grp AS (
                    SELECT
                        d.client_id,
                        d.resource_id,
                        d.client_resource_id,
                        d.resource_name,
                        d.job_day,
                        d.geocode_long_from,
                        d.geocode_lat_from,
                        d.distance_from,
                        d.time_distance_from,
                        d.arrival_from,
                        d.date_from,
                        d.geocode_long_at,
                        d.geocode_lat_at,
                        d.distance_at,
                        d.time_distance_at,
                        d.arrival_at,
                        d.date_at,
                        list(
                            json_object(
                                'client_id', d.client_id,
                                'job_id', d.job_id,
                                'team_id', d.team_id,
                                'client_job_id', d.client_job_id,
                                'status_description', d.status_description,
                                'type_description', d.type_description,
                                'address', d.address,
                                'city', d.city,
                                'state_prov', d.state_prov,
                                'zippost', d.zippost,
                                'trade_name', d.trade_name,
                                'cnpj', d.cnpj,
                                'job_day', d.job_day,
                                'geocode_lang', d.geocode_long,
                                'geocode_lat', d.geocode_lat,
                                'arrival', d.arrival,
                                'service', d.time_service,
                                'distance', d.distance,
                                'time_distance', d.time_distance,
                                'start_date', d.start_date,
                                'end_date', d.end_date
                            ) ORDER BY d.start_date
                        ) AS jobs,
                        sum(d.distance) + max(d.distance_at) AS total_distance,
                        sum(d.time_distance) + max(d.time_distance_at) AS total_time_distance,
                        count(d.job_id) AS total_jobs
                    FROM dados_calc d
                    GROUP BY
                        d.client_id, d.resource_id, d.client_resource_id, d.resource_name,
                        d.job_day, d.geocode_long_from, d.geocode_lat_from, d.distance_from,
                        d.time_distance_from, d.arrival_from, d.date_from, d.geocode_long_at,
                        d.geocode_lat_at, d.distance_at, d.time_distance_at, d.arrival_at, d.date_at
                )
                SELECT
                    list(
                        json_object(
                            'client_id', client_id,
                            'resource_id', resource_id,
                            'client_resource_id', client_resource_id,
                            'resource_name', resource_name,
                            'job_day', job_day,
                            'geocode_long_from', geocode_long_from,
                            'geocode_lat_from', geocode_lat_from,
                            'distance_from', distance_from,
                            'time_distance_from', time_distance_from,
                            'arrival_from', arrival_from,
                            'date_from', date_from,
                            'geocode_long_at', geocode_long_at,
                            'geocode_lat_at', geocode_lat_at,
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
                FROM grp
            """
            output = {
                "reports": [
                    {"type": "REAL", "resources": real_results},
                    {"type": "BRAC", "resources": brac_results},
                    {"type": "BRAA", "resources": braa_results},
                ]
            }
            rep_json = json.dumps(output)
            await db.execute(
                text("""
                    MERGE INTO reports u
                    USING (SELECT CAST(:json_data AS jsonb) AS rep) AS t
                    ON (client_id = :client_id AND client_team_id = :client_team_id AND report_date = :report_date)
                    WHEN MATCHED THEN
                        UPDATE SET
                            report = t.rep,
                            rebuild = 0,
                            modified_by = 'system',
                            modified_date = NOW()
                    WHEN NOT MATCHED THEN
                        INSERT (client_id, client_team_id, report_date, report, created_by, created_date, modified_by, modified_date)
                        VALUES (:client_id, :client_team_id, :report_date, t.rep, 'system', now(), 'system', now())
                """),
                {"client_id": clientId, "client_team_id": clientTeamId, "report_date": actualDate, "json_data": rep_json}
            )
            await db.commit()
            logger.warning('Reports finalizados!')

    except Exception as e:
        log_error(e, "[buildReports] Erro")

