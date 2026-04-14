import traceback
import sys
import asyncio
import redis.asyncio as Redis
import redis_client
import json
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.future import select
from database import engine, Base, SessionLocal
from config import settings
from services import optimize_routes_vroom, get_route_distance_block, logs 
import models
from auth import get_password_hash
from tools import (
    getJobStatusMatrix, getJobTypeMatrix, getPlaceMatrix, getPriorityMatrix, getResourceWindowMatrix,
    getStyleMetrix, getResourcesMatrix, getTeamMatrix, getJobsMatrix, getGeoPosMatrix,
    getTeamMemberMatrix, getLogInOutMatrix, getAdressMatrix
)
from loguru import logger


def oneHour(dateTime: str):
    data_obj = datetime.strptime(dateTime, '%Y-%m-%d %H:%M:%S')
    nova_data_obj = data_obj - timedelta(hours=1)
    return nova_data_obj.strftime('%Y-%m-%d %H:%M:%S')


async def deleteJobs():
    
    tempDateTime = (datetime.now() - timedelta(days=15))
    pDate = tempDateTime.strftime('%Y-%m-%d')
    #vamos verificar se existe alguma tarefa concluida sem distância calculada
    async with SessionLocal() as db:
      smtp = text("DELETE FROM jobs WHERE created_date < CAST(:p_date AS DATE);").bindparams(p_date=pDate)
      await db.execute(smtp)
      # smtp = text("""DELETE FROM address a
      #                 WHERE NOT EXISTS (SELECT 1 
      #                                     FROM jobs j 
      #                                     WHERE j.client_id = a.client_id
      #                                       AND j.address_id = a.address_id);
      #             """)
      # await db.execute(smtp)
      await db.commit()

async def buildReports():
    try:
        smtp = text("""
            WITH qjobs AS (
                SELECT 
                    j.*,
                    COALESCE(j.actual_start_date, j.plan_start_date) as actual_date,
                    bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_distance,
                    bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date)) AS null_geo
                FROM jobs j
                    JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                    JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                    JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                    JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
                WHERE js.internal_code_status = 'CONCLU'
                    AND j.client_id = 1
                    AND j.team_id = 33
                    AND COALESCE(j.actual_start_date, j.plan_start_date) < date_trunc('day',NOW())
                    AND NOT EXISTS (SELECT 
                                        1
                                    FROM reports rp
                                    WHERE  rp.client_id = j.client_id
                                        AND rp.team_id = j.team_id
                                        AND COALESCE(j.actual_start_date, j.plan_start_date) >= rp.report_date 
                                        AND COALESCE(j.actual_start_date, j.plan_start_date) < rp.report_date + interval '1 day'
                                        AND rp.rebuild = 0
                                    )
                ),
                q1 AS (
                    select 
                        * 
                    from qjobs
                    where null_distance = true
                        AND null_geo = false
                )
                select 
                    client_id, 
                    team_id, 
                    DATE_TRUNC('day', actual_date) as actual_date, 
                    count(*) total 
                from qjobs
                group by client_id, team_id, DATE_TRUNC('day', actual_date)
                order by 1,2,3
        """)
        output = None
        async with SessionLocal() as db:
            result = await db.execute(smtp)
            rows = result.mappings().all()
            if not rows or len(rows) ==0:
                return
            for row in rows:
                clientId = row.client_id
                await logs(clientId=clientId,log='jobs',logJson=row)
                teamId = row.team_id
                actualDate = row.actual_date
                braa_results = []
                brac_results = []
                for r in ['BRAA','BRAC']:
                    logger.warning(f'Iniciando... {r} em {actualDate}')
                    vBrac = text("""
                    AND EXISTS (
                        SELECT 1 
                        FROM jobs x
                            JOIN job_status xs ON xs.client_id = x.client_id AND xs.job_status_id = x.job_status_id
                        WHERE x.client_id = r.client_id
                            and x.resource_id = r.resource_id
                            and xs.internal_code_status = 'CONCLU'
                            AND COALESCE(x.actual_start_date,x.plan_start_date) >= :p_date
                            AND COALESCE(x.actual_start_date,x.plan_start_date) < :p_date + interval '1 day'
                    )    
                    """)    
                    
                    logger.info("Iniciado Calculo das rotas ...")
                    smtp = text(f"""
                        WITH dados AS (
                            SELECT j.client_id
                                ,t.team_id
                                ,300 time_overlap
                                ,j.job_id
                                ,j.address_id
                                ,a.geocode_lat::NUMERIC geocode_lat
                                ,a.geocode_long::NUMERIC geocode_long
                                ,COALESCE (j.time_setup, jt.time_setup, t.time_setup) AS time_setup
                                ,COALESCE (j.time_service, jt.time_service, t.time_service) AS time_service
                                ,j.priority + (COALESCE (jt.priority, 0) / 100)::INTEGER AS priority
                                ,EXTRACT (epoch FROM COALESCE (aw.start_time, CAST ('00:00:00' AS TIME)))::INTEGER AS start_time
                                --,0 AS start_time
                                ,EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59' AS TIME)))::INTEGER AS end_time
                            FROM jobs  j
                                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                                JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                                LEFT JOIN address_windows aw
                                ON     aw.client_id = j.client_id
                                    AND aw.address_id = j.address_id
                                    AND aw.week_day = EXTRACT (dow FROM COALESCE (j.actual_start_date, j.plan_start_date, now ())) + 1
                        WHERE  COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
                            AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'
                            AND j.client_id = :client_id
                            AND j.team_id = :team_id
                            AND a.geocode_lat is not null
                            AND a.geocode_long is not null
                            and (a.geocode_lat::NUMERIC) < 100
                            AND (a.geocode_long::NUMERIC) < 100
                            AND js.internal_code_status = 'CONCLU'
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
                            r.geocode_lat_from::NUMERIC,
                            r.geocode_long_from::NUMERIC,
                            r.geocode_lat_at::NUMERIC,
                            r.geocode_long_at::NUMERIC,
                            EXTRACT(EPOCH FROM COALESCE(rw.start_time,t.start_time)) ::INTEGER AS start_time,
                            EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then COALESCE(rw.end_time,t.end_time) else cast('23:59:59' as time) end ) ::INTEGER AS end_time
                            from resources r
                            join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                            join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                            LEFT JOIN resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM :p_date) + 1
                            where r.client_id = :client_id
                            and t.team_id = :team_id
                            {vBrac if r =='BRAC' else ''}
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
                    """).bindparams(client_id=clientId, p_date = actualDate, team_id = teamId)
                    result = await db.execute(smtp)
                    subRows = result.mappings().all()
                    if not subRows or len(subRows) == 0:
                        return
                    await logs(clientId=clientId,log='dados para vroom',logJson=dict(subRows[0]))
                    vroom_payload = subRows[0]['vroom_payload']

                    listJobs = vroom_payload.pop('list', 'Chave não encontrada')
                    logger.info(list(listJobs))
                    
                    # logger.info(vroom_payload)

                    logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
                    retorno = await optimize_routes_vroom(vroom_payload)
                    

                    routes = retorno['routes']
                    for rou in routes:
                        # print(rou)
                        # print('--------------------------------------')
                        steps = rou['steps']
                        geo = []
                        for step in steps:
                            geo.append([step['location'][0],step['location'][1]])
                        # print(geo)
                        # print('---------------------------------------')
                        await logs(clientId=clientId,log='montagem do geo',logJson=geo)
                        geoResult = await get_route_distance_block(geo)
                        await logs(clientId=clientId,log='retorno do geo',logJson=geoResult)
                        # print(geoResult) if r=='BRAC' else None
                        ln = 1
                        step = rou['steps'][0]
                        step['time_distance'] = 0
                        step['distance'] = 0
                        for x in geoResult:
                            # print(x['duration'],x['distance'])
                            step = rou['steps'][ln]
                            step['time_distance'] = x['duration']
                            step['distance'] = x['distance']
                            # print(ln,' - ',step)
                            ln += 1
                    listIds = [item['id'] for item in retorno.get("unassigned", [])]
                    if listIds:
                        await logs(clientId=clientId,log='lista de unassigned',logJson=listIds)
                        logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
                    
                    await logs(clientId=clientId,log='retorno do vroom',logJson=retorno)
                    
                    # if actualDate.strftime('%Y-%m-%d') == '2026-03-04':
                    #     return
                    # continue

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
                                  300 time_overlap,
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
                            where j.client_id = :client_id
                              and j.job_id IN ({','.join(str(j) for j in listJobs)})
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
                                :p_date job_day,
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
                                json_agg(
                                    json_build_object(
                                        'client_id', q.client_id,
                                        'job_id', q.job_id,
                                        'team_id', q.team_id,
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
                        from grp
                    """).bindparams(client_id=clientId,p_date = actualDate)
                    result = await db.execute(smtp)
                    subSubRows = result.mappings().all()
                    if not subSubRows or len(subSubRows) == 0:
                        return
                    
                    await logs(clientId=clientId,log=f'Resultado do {r}',logJson=subSubRows[0])
                    res = subSubRows[0]['res']
                    if isinstance(res, list):
                        brac_results.extend(res) if r =='BRAC' else braa_results.extend(res)
                    else:
                        brac_results.append(res) if r =='BRAC' else braa_results.append(res)

                    logger.warning(f'Finalizado ... {r}')

                logger.warning(f'Iniciando... REAL')
                smtp = text(f"""
                    with dados as(
                        select 
                            r.client_id,
                            j.team_id,
                            r.resource_id,
                            j.job_id,
                            r.client_resource_id,
                            r.description resource_name,
                            j.client_job_id,
                            js.description AS status_description,
                            jt.description as type_description,
                            a.address, 
                            a.city, 
                            a.state_prov, 
                            a.zippost, 
                            p.trade_name, 
                            p.cnpj,
                            date_trunc('day',COALESCE(j.actual_start_date,j.plan_start_date)) job_day,
                            r.geocode_long_from,
                            r.geocode_lat_from,
                            r.geocode_long_at,
                            r.geocode_lat_at,
                            FIRST_VALUE(COALESCE(j.actual_start_date,j.plan_start_date)) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, date_trunc('day',COALESCE(j.actual_start_date,j.plan_start_date)) ORDER BY COALESCE(j.actual_start_date,j.plan_start_date) ASC ) first_start_date,
                            FIRST_VALUE(COALESCE(j.actual_end_date,j.plan_end_date)) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, date_trunc('day',COALESCE(j.actual_end_date,j.plan_end_date)) ORDER BY COALESCE(j.actual_end_date,j.plan_end_date) DESC ) last_end_date,
                            j.first_distance AS distance_from,
                            j.first_time_distance as time_distance_from,
                            j.last_distance AS distance_at,
                            j.last_time_distance as time_distance_at,
                            a.geocode_long,
                            a.geocode_lat,
                            EXTRACT(EPOCH FROM (COALESCE(j.actual_start_date,j.plan_start_date) - date_trunc('day',COALESCE(j.actual_start_date,j.plan_start_date))))::INTEGER AS arrival,
                            EXTRACT(EPOCH FROM (COALESCE(j.actual_end_date,j.plan_end_date) - COALESCE(j.actual_start_date,j.plan_start_date)) )::INTEGER as time_service,
                            j.distance,
                            j.time_distance,
                            COALESCE(j.actual_start_date,j.plan_start_date) as start_date,
                            COALESCE(j.actual_end_date,j.plan_end_date) as end_date,
                            bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_distance,
                            bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_geo
                        from jobs j
                        JOIN resources r ON r.client_id = j.client_id and r.resource_id = j.resource_id
                        JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                        join job_types jt on jt.client_id = j.client_id and jt.job_type_id = j.job_type_id
                        join address a on a.client_id = j.client_id and a.address_id = j.address_id
                        join places p on p.client_id = j.client_id and p.place_id = j.place_id
                        WHERE COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
                            AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'
                            AND js.internal_code_status = 'CONCLU'
                            AND a.geocode_lat is not null
                            AND a.geocode_long is not null
                            and (a.geocode_lat::NUMERIC) < 100
                            AND (a.geocode_long::NUMERIC) < 100
                            AND j.client_id = :client_id
                            AND j.team_id = :team_id
                    ),
                    dados_calc as
                    (
                        select 
                            c.*,
                            EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from  AS arrival_from,
                            EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at AS arrival_at,
                            c.job_day  + (EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from) * INTERVAL '1 second' date_from,
                            c.job_day  + (EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at) * INTERVAL '1 second' date_at
                        from dados c
                        --where c.null_distance = false
                        --and c.null_geo = false
                    ),
                    grp as (
                        select 
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
                            json_agg(
                                json_build_object(
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
                                ) ORDER BY start_date
                            ) AS jobs,
                            sum(d.distance) + max(d.distance_at) as total_distance,
                            sum(d.time_distance) + max(d.time_distance_at) as total_time_distance,
                            count(d.job_id) total_jobs
                        from dados_calc d
                        GROUP BY     
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
                            d.date_at
                    )
                        select
                            json_agg(
                                json_build_object(
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
                        from grp
                """).bindparams(client_id=clientId, p_date = actualDate, team_id = teamId)
                result = await db.execute(smtp)
                subRows = result.mappings().all()
                if not subRows or len(subRows) == 0:
                    return
                
                await logs(clientId=clientId,log=f'Resultado do REAL',logJson=subRows[0])
                res = subRows[0]['res']
                real_results = []
                if isinstance(res, list):
                    real_results.extend(res)
                else:
                    real_results.append(res)
                output = {
                    "reports": [
                        {"type": "REAL", "resources": real_results},
                        {"type": "BRAC", "resources": brac_results},
                        {"type": "BRAA", "resources": braa_results},
                    ]
                }
                print('Chegou aqui?')
                rep_json = json.dumps(output)
                smtp = text(f"""
                    MERGE INTO reports u
                    USING (SELECT CAST(:json_data AS jsonb) as rep) as t
                    ON ( client_id = :client_id and team_id = :team_id and report_date = :report_date)
                    WHEN MATCHED THEN
                    UPDATE SET 
                        report = t.rep
                        ,rebuild = 0
                        ,modified_by = 'system'
                        ,modified_date  = NOW()
                    WHEN NOT MATCHED
                        THEN        
                    INSERT (client_id, team_id, report_date, report, created_by, created_date, modified_by, modified_date)
                    VALUES (:client_id, :team_id, :report_date, t.rep, 'system', now(), 'system', now())
                """)
                
                parametros = {
                    "client_id": clientId,
                    "team_id": teamId,
                    "report_date": actualDate,
                    "json_data": rep_json
                }
                await db.execute(smtp, parametros)
                await db.commit()
                logger.warning(f'Reports finalizados!')

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
    
        # Extrai a última linha da pilha (onde o erro ocorreu)
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")        

async def calcDistance():
    try:
    #vamos verificar se existe alguma tarefa concluida sem distância calculada
        smtp = text("""
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
                    WHEN r.geocode_lat_at IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_lat) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                    r.geocode_lat_at 
                END AS geocode_lat_at,
                CASE 
                    WHEN r.geocode_long_at IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_long) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                    r.geocode_long_at 
                END AS geocode_long_at,
                -- Verifica se o ponto de chegada do recurso é nulo, se for, não tem ponto de partida
                CASE 
                    WHEN r.geocode_lat_from IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_lat) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                    r.geocode_lat_from 
                END AS geocode_lat_from,
                CASE 
                    WHEN r.geocode_long_from IS NULL 
                    THEN 
                        FIRST_VALUE(a.geocode_long) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', j.actual_start_date) ORDER BY j.actual_start_date ASC )
                    ELSE 
                    r.geocode_long_from 
                END AS geocode_long_from,
                --verifica se existe algum distance nulo no periodo, então processa
                bool_or(j.distance IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_distance,
                --verifica se existe algum geocode está vazio e não processa
                bool_or(a.geocode_long IS NULL OR a.geocode_lat IS NULL) OVER (PARTITION BY j.client_id, j.team_id, j.resource_id, DATE_TRUNC('day', COALESCE(j.actual_start_date,j.plan_start_date))) AS null_geo
            FROM jobs j
                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                JOIN resources r on r.client_id = j.client_id AND r.resource_id = j.resource_id
                /*LEFT JOIN LATERAL (
                        SELECT a2.geocode_lat, a2.geocode_long
                            FROM jobs j2
                            JOIN address a2 ON a2.client_id = j2.client_id AND a2.address_id = j2.address_id
                            WHERE j2.actual_start_date < j.actual_start_date
                            AND j2.actual_start_date >= DATE_TRUNC('day', j.actual_start_date)
                            AND j2.job_status_id = j.job_status_id
                            AND j2.team_id = j.team_id
                            AND j2.client_id = j.client_id
                            AND j2.resource_id = j.resource_id
                            ORDER BY j2.actual_start_date DESC
                            LIMIT 1
                        ) jl ON true*/
            WHERE js.internal_code_status = 'CONCLU'
            order by j.client_id, j.team_id, j.resource_id, j.actual_start_date nulls last, j.plan_start_date
        )
        SELECT
            q1.client_id,
            q1.team_id,
            q1.resource_id,
            q1.fix_start_date,
            (
            jsonb_build_array(jsonb_build_array(MAX(q1.geocode_long_from)::NUMERIC, MAX(q1.geocode_lat_from)::NUMERIC))
            ||
            jsonb_agg(jsonb_build_array(q1.geocode_long::NUMERIC, q1.geocode_lat::NUMERIC) ORDER BY q1.actual_start_date nulls last, q1.plan_start_date) 
            || 
            jsonb_build_array(jsonb_build_array(MAX(q1.geocode_long_at)::NUMERIC, MAX(q1.geocode_lat_at)::NUMERIC))
            ) AS rota_completa
         FROM q1
        WHERE null_distance = true
          AND null_geo = false
        GROUP BY q1.client_id, q1.team_id, q1.resource_id, q1.fix_start_date
        ORDER BY q1.client_id, q1.team_id, q1.resource_id, q1.fix_start_date;
        """)
        async with SessionLocal() as db:
            result = await db.execute(smtp)
            rows = result.mappings().all()
            for row in rows:
                clientId = row.client_id

                await logs(clientId=clientId,log=f'Dados para distance',logJson=row)
                
                resourceId = row.resource_id
                teamId = row.team_id
                pDate = row.fix_start_date
                geo = row.rota_completa
                geoResult = await get_route_distance_block(geo)
                
                await logs(clientId=clientId,log=f'Resultado do distance',logJson=geoResult)
                
                geoJson = json.dumps(geoResult)
                smtp = text(f"""
                MERGE INTO jobs AS u
                    USING (
                        WITH qjson AS 
                        (
                            SELECT  
                                y.*, 
                                FIRST_VALUE(distance) OVER (ORDER BY linha ASC ) AS first_distance,
                                FIRST_VALUE(distance) OVER (ORDER BY linha DESC ) AS last_distance,
                                FIRST_VALUE(duration) OVER (ORDER BY linha ASC ) AS first_duration,
                                FIRST_VALUE(duration) OVER (ORDER BY linha DESC ) AS last_duration
                             FROM (
                                SELECT 
                                    ROW_NUMBER() over() linha, 
                                    x.*
                                  FROM 
                                    jsonb_to_recordset('{geoJson}') 
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
                """).bindparams(client_id=clientId, team_id = teamId, p_date=pDate, resource_id = resourceId)

                result = await db.execute(smtp)
                await db.commit()
                rows = result.mappings().all()
                
                await logs(clientId=clientId,log=f'Resultado do MERGE distance',logJson=([dict(row) for row in rows]))
    except Exception as e:
        logger.error(f"[calcDistance] Erro ao processar postgres: {e}")          

async def getStyle(r):
    logger.warning("[getStyle] Entrou atualização de estilos...")

    snapKey = f'snapStyleTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getStyleMetrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO styles AS u
                  USING (SELECT
                          x.item_style_id,
                          x.font_weight,
                          COALESCE(x.background, '#FFFFFF') AS background,
                          COALESCE(x.foreground, '#000000') AS foreground,
                          x.modified_dttm,
                          x.last_snap
                          FROM jsonb_to_recordset(:dados_json)
                            AS x( item_style_id text,
                                  font_weight text,
                                  background text,
                                  foreground text,
                                  modified_dttm TIMESTAMP,
                                  last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_style_id = t.item_style_id
                  WHEN MATCHED AND (
                                 u.font_weight IS DISTINCT FROM t.font_weight
                              OR u.background IS DISTINCT FROM t.background
                              OR u.foreground IS DISTINCT FROM t.foreground
                              )  THEN
                      UPDATE SET font_weight = t.font_weight
                          ,background = t.background
                          ,foreground = t.foreground
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_style_id, font_weight, background, foreground, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.item_style_id, t.font_weight, t.background, t.foreground,'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
                  RETURNING
                      u.style_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.style_id} | Ação STYLE realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)
          except Exception as e:
                    logger.error(f"Erro ao processar postgres: {e}")

    return

async def getResources(r):
    logger.warning("[getResources] Entrou atualização dos Recursos ...")

    snapKey = f'snapResourceTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)

    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'


    dateTime = oneHour(dateTime)

    result_rows = await getResourcesMatrix(dateTime, settings.client_uid)
    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO resources AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              person_id text,
                              name text,
                              geocode_lat text,
                              geocode_long text,
                              geocode_lat_from text,
                              geocode_long_from text,
                              geocode_lat_at text,
                              work_status integer,
                              geocode_long_at text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_resource_id = t.person_id
                  WHEN MATCHED AND (
                                 u.actual_geocode_lat IS DISTINCT FROM t.geocode_lat
                              OR u.actual_geocode_long IS DISTINCT FROM t.geocode_long
                              OR u.geocode_lat_from IS DISTINCT FROM t.geocode_lat_from
                              OR u.geocode_long_from IS DISTINCT FROM t.geocode_long_from
                              OR u.geocode_lat_at IS DISTINCT FROM t.geocode_lat_at
                              --OR u.fl_off_shift IS DISTINCT FROM t.work_status
                              OR u.geocode_long_at IS DISTINCT FROM t.geocode_long_at
                              OR u.description IS DISTINCT FROM t.name
                              )  THEN
                      UPDATE SET actual_geocode_lat = t.geocode_lat
                          ,actual_geocode_long = t.geocode_long
                          ,description = t.name
                          --,fl_off_shift = t.work_status --Voltar quando decidir se vai ser pelo cliente
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_resource_id, description, fl_off_shift, actual_geocode_lat, actual_geocode_long, geocode_lat_from, geocode_long_from, geocode_lat_at, geocode_long_at, modified_date_geo, modified_date_login, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.person_id, t.name, t.work_status, t.geocode_lat, t.geocode_long, t.geocode_lat_from, t.geocode_long_from, t.geocode_lat_at, t.geocode_long_at, NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days', 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
                  RETURNING
                       u.resource_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.resource_id} | Ação RESOURCE GEO realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getResource] Erro ao processar postgres: {e}")

    return

async def getResourceWindow(r):
    logger.warning("[getResourceWindow] Entrou atualização das Janelas de Recursos ...")

    snapKey = f'snapResourceWindowTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)

    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getResourceWindowMatrix(dateTime, settings.client_uid)
    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)

              stmt = text(f"""
                  MERGE INTO resource_windows AS u
                  USING (SELECT
                            x.person_id,
                            x.work_cal_time_id,
                            r.resource_id,
                            cast(x.day_code as integer) as day_code,
                            x.description,
                            cast(x.start_tm as time) as start_tm,
                            cast(x.stop_tm as time) as stop_tm,
                            x.modified_dttm,
                            x.last_snap
                          FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              person_id text,
                              work_cal_time_id text,
                              day_code text,
                              description text,
                              start_tm TIMESTAMP,
                              stop_tm TIMESTAMP,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)
                          JOIN resources r ON r.client_resource_id = x.person_id AND r.client_id = :client_id
                          ) AS t
                  ON u.client_id = :client_id
                          AND u.resource_id = t.resource_id
                          AND u.client_rw_id = t.work_cal_time_id
                  WHEN MATCHED AND (
                              u.description IS DISTINCT FROM t.description
                              OR u.start_time IS DISTINCT FROM t.start_tm
                              OR u.end_time IS DISTINCT FROM t.stop_tm
                              OR u.week_day IS DISTINCT FROM t.day_code
                              )  THEN
                      UPDATE SET description = t.description
                          ,start_time = t.start_tm
                          ,end_time = t.stop_tm
                          ,week_day = t.day_code
                          ,modified_date = t.modified_dttm
                          ,modified_by = 'INTEGRATION'
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, resource_id, client_rw_id, description, start_time, end_time, week_day, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.resource_id, t.work_cal_time_id, t.description, t.start_tm, t.stop_tm, t.day_code,'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
                  RETURNING
                       u.rw_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.rw_id} | Ação RESOURCE WINDOW realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getResourceWindow] Erro ao processar postgres: {e}")

    return

async def getAddress(r):
    logger.warning("[getAddress] Entrou atualização de Endereços ...")

    snapKey = f'snapAddressTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)

    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getAdressMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO address AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              address_id text,
                              address text,
                              geocode_lat text,
                              geocode_long text,
                              city text,
                              state_prov text,
                              zippost text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_address_id = t.address_id
                  WHEN MATCHED AND (
                                 u.geocode_lat IS DISTINCT FROM t.geocode_lat
                              OR u.address IS DISTINCT FROM t.address
                              OR u.geocode_long IS DISTINCT FROM t.geocode_long
                              OR u.city IS DISTINCT FROM t.city
                              OR u.state_prov IS DISTINCT FROM t.state_prov
                              OR u.zippost IS DISTINCT FROM t.zippost
                              )  THEN
                      UPDATE SET geocode_lat = t.geocode_lat
                          ,address = t.address
                          ,geocode_long = t.geocode_long
                          ,city = t.city
                          ,state_prov = t.state_prov
                          ,zippost = t.zippost
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  RETURNING
                      u.address_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.address_id} | Ação ADDRESS realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getAddress] Erro ao processar postgres: {e}")

    return

async def getPlaces(r):
    logger.info("[getPlaces] Entrou atualização de Locais ...")

    snapKey = f'snapPlaceTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)

    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getPlaceMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO places AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              place_id text,
                              trade_name text,
                              cnpj text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_place_id = t.place_id
                  WHEN MATCHED AND (
                                 u.trade_name IS DISTINCT FROM t.trade_name
                              OR u.cnpj IS DISTINCT FROM t.cnpj
                              )  THEN
                      UPDATE SET trade_name = t.trade_name
                          ,cnpj = t.cnpj
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  RETURNING
                      u.place_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.place_id} | Ação PLACE realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getPlaces] Erro ao processar postgres: {e}")

    return


async def getGeoPos(r):
    logger.warning("[getGeoPos] Entrou atualização Geo Resource posicionamento...")

    snapKey = f'snapGeoTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getGeoPosMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO resources AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              modified_by text,
                              geocode_lat text,
                              geocode_long text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_resource_id = t.modified_by
                  WHEN MATCHED AND (
                                 u.actual_geocode_lat IS DISTINCT FROM t.geocode_lat
                              OR u.actual_geocode_long IS DISTINCT FROM t.geocode_long
                              ) AND u.modified_date_geo < t.modified_dttm

                          THEN
                      UPDATE SET actual_geocode_lat = t.geocode_lat
                          ,actual_geocode_long = t.geocode_long
                          ,modified_date_geo = t.modified_dttm
                      RETURNING
                          to_jsonb(u) AS registro_json, merge_action(), u.resource_id
              """)
              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.resource_id} | Ação GEO realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)
          except Exception as e:
                    logger.error(f"[getGeoPos] Erro ao processar postgres: {e}")

    return


async def getLogInOut(r):
    logger.warning("[getLogInOut] Entrou atualização Login...")

    snapKey = f'snapLoginTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getLogInOutMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)

              stmt = text(f"""
                  MERGE INTO resources AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              person_id text, logged_in TIMESTAMP, logged_out TIMESTAMP, modified_dttm TIMESTAMP, last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_resource_id = t.person_id
                  WHEN MATCHED AND (
                                 u.logged_in IS DISTINCT FROM t.logged_in
                              OR u.logged_out IS DISTINCT FROM t.logged_out
                              ) AND u.modified_date_login < t.modified_dttm THEN
                      UPDATE SET logged_in = t.logged_in
                          ,logged_out = t.logged_out
                          ,modified_date_login = t.modified_dttm
                      RETURNING
                          to_jsonb(u) AS registro_json, merge_action(), u.resource_id
              """)
              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"Registro: {row.resource_id} | Ação LOGIN realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getLogInOut] Erro ao processar postgres: {e}")

    return


async def getTeam(r):
    logger.warning("[getTeam] Entrou atualização dos Times ...")

    snapKey = f'snapTeamTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)

    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getTeamMatrix(dateTime, settings.client_uid)
    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO teams AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              team_id text,
                              description text,
                              geocode_lat text,
                              geocode_long text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_team_id = t.team_id
                  WHEN MATCHED AND (
                              u.team_name IS DISTINCT FROM t.description
                              OR u.geocode_lat IS DISTINCT FROM t.geocode_lat
                              OR u.geocode_long IS DISTINCT FROM t.geocode_long
                              )  THEN
                      UPDATE SET team_name = t.description
                          ,geocode_lat = t.geocode_lat
                          ,geocode_long = t.geocode_long
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  RETURNING
                       u.team_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.team_id} | Ação TEAM realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getTeam] Erro ao processar postgres: {e}")

    return


async def getTeamMember(r):
    logger.warning("[getTeamMember] Entrou atualização Team Member...")

    snapKey = f'snapTeamMemberTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getTeamMemberMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)

              stmt = text(f"""
                  MERGE INTO team_members AS u
                  USING (
                      SELECT
                          x.modified_dttm,
                          tm.team_id,
                          r.resource_id,
                          x.last_snap,
                          x.person_id
                      FROM jsonb_to_recordset(:dados_json) AS x(
                          modified_dttm TIMESTAMP,
                          team_id text,
                          person_id text,
                          last_snap TIMESTAMP
                      )
                      JOIN teams tm ON tm.client_team_id = x.team_id and tm.client_id = :client_id
                      JOIN resources r ON r.client_resource_id = x.person_id and r.client_id = :client_id
                  ) AS t
                  ON u.client_id = :client_id AND u.resource_id = t.resource_id AND u.team_id = t.team_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, team_id, resource_id, created_by,created_date, modified_by, modified_date)
                      VALUES (:client_id, t.team_id, t.resource_id, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
                  RETURNING
                      to_jsonb(u) AS registro_json,
                      merge_action(),
                      u.uid;
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"UID: {row.uid} | Ação TEAM realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getTeamMember] Erro ao processar postgres: {e}")
    return


async def getJobType(r):
    logger.warning("[getJobType] Entrou atualização de Tipos de Trabalho ...")

    snapKey = f'snapJobTypeTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 2))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'

    dateTime = oneHour(dateTime)

    result_rows = await getJobTypeMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO job_types AS u
                  USING (SELECT * FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              code_value text,
                              description text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_job_type_id = t.code_value
                  WHEN MATCHED AND ( u.description IS DISTINCT FROM t.description )
                           THEN
                      UPDATE SET description = t.description
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  RETURNING
                      u.job_type_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.job_type_id} | Ação JOB_TYPE realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getJobType] Erro ao processar postgres: {e}")

    return


async def getJobStatus(r):
    logger.warning("[getJobStatus] Entrou atualização de Status de Trabalho ...")

    snapKey = f'snapJobStatusTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=365 * 7))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    else:
      dateTime = oneHour(dateTime)

    result_rows = await getJobStatusMatrix(dateTime, settings.client_uid)

    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)
              stmt = text(f"""
                  MERGE INTO job_status AS u
                  USING (SELECT x.*, s.style_id
                          FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              task_status text,
                              description text,
                              item_style_id text,
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)
                          JOIN styles s ON s.client_style_id = x.item_style_id AND s.client_id = :client_id
                          ) AS t
                  ON u.client_id = :client_id AND u.client_job_status_id = t.task_status
                  WHEN MATCHED AND (
                            u.description IS DISTINCT FROM t.description
                          OR u.style_id IS DISTINCT FROM t.style_id)
                           THEN
                      UPDATE SET description = t.description
                          ,style_id = t.style_id
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  RETURNING
                      u.job_status_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.job_status_id} | Ação JOB_STATUS realizada: {row.merge_action}")

              await r.set(snapKey, last_snap)

          except Exception as e:
                    logger.error(f"[getJobStatus] Erro ao processar postgres: {e}")

    return


async def getJobs(r):

    logger.warning("[getJobs] Entrou atualização das Tarefas ...")

    snapKey = f'snapJobsTime:{settings.client_uid}'
    dateTime = await r.get(snapKey)
    if dateTime is None:
        tempDateTime = (datetime.now() - timedelta(days=45))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    else:
      dateTime = oneHour(dateTime)

    result_rows = await getJobsMatrix(dateTime, settings.client_uid)

    if result_rows:
      last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
      first_snap = result_rows[0]['first_snap'][:19].replace('T', ' ')
      try:
          jsonResults = json.dumps(result_rows)
          #Criação do Resource  
          stmt = text(f"""
                  MERGE INTO resources AS u
                  USING (
                      SELECT 
                        DISTINCT x.* 
                      FROM jsonb_to_recordset(:dados_json)
                        AS x( person_id text,
                                resource_name text,
                                resource_geocode_lat text,
                                resource_geocode_long text,
                                geocode_lat_from text,
                                geocode_long_from text,
                                geocode_lat_at text,
                                geocode_long_at text,
                                work_status integer,
                                resource_modified_dttm TIMESTAMP
                            )
                        WHERE person_id IS NOT NULL
                      ) AS t
                  ON u.client_id = :client_id AND u.client_resource_id = t.person_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_resource_id, description, fl_off_shift, actual_geocode_lat, actual_geocode_long, geocode_lat_from, geocode_long_from, geocode_lat_at, geocode_long_at, modified_date_geo, modified_date_login, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.person_id, t.resource_name, t.work_status, t.resource_geocode_lat, t.resource_geocode_long, t.geocode_lat_from, t.geocode_long_from, t.geocode_lat_at, t.geocode_long_at, NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days', 'INTEGRATION', NOW(), 'INTEGRATION', t.resource_modified_dttm)
                  RETURNING
                       u.resource_id, merge_action(), to_jsonb(u) AS registro_json
                  """)
          async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  setSnap = True
                  logger.info(f"ID: {row.resource_id} | Ação RESOURCE realizada: {row.merge_action}")

          # Criação do Team
          setSnap = False
          stmt = text(f"""
                  MERGE INTO teams AS u
                  USING (
                      SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
                      AS x(   team_id text,
                              desc_team text,
                              team_modified_dttm TIMESTAMP
                      ) ) AS t
                  ON u.client_id = :client_id AND u.client_team_id = t.team_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_team_id, team_name, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.team_id, t.desc_team, 'INTEGRATION', NOW(), 'INTEGRATION', t.team_modified_dttm)
                  RETURNING
                       u.team_id, merge_action(), to_jsonb(u) AS registro_json
                  """)
          async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  setSnap = True
                  logger.info(f"ID: {row.team_id} | Ação TEAM realizada: {row.merge_action}")

          if setSnap:
            snapKey = f'snapTeamTime:{settings.client_uid}'
            await r.set(snapKey, first_snap)

          # Criação do Job Types
          setSnap = False
          stmt = text(f"""
                  MERGE INTO job_types AS u
                  USING (
                      SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
                      AS x(   task_type text,
                              desc_task_type text,
                              task_type_modified_dttm TIMESTAMP
                      ) ) AS t
                  ON u.client_id = :client_id AND u.client_job_type_id = t.task_type
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_job_type_id, description, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.task_type, t.desc_task_type, 'INTEGRATION', NOW(), 'INTEGRATION', t.task_type_modified_dttm)
                  RETURNING
                       u.job_type_id, merge_action(), to_jsonb(u) AS registro_json
                  """)
          async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  setSnap = True
                  logger.info(f"ID: {row.job_type_id} | Ação JOB TYPE realizada: {row.merge_action}")

          if setSnap:
            snapKey = f'snapJobTypeTime:{settings.client_uid}'
            await r.set(snapKey, first_snap)

          # Criação do Job Status
          setSnap = False
          stmt = text(f"""
                  MERGE INTO job_status AS u
                  USING (SELECT DISTINCT x.*, s.style_id
                          FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              task_status text,
                              desc_task_status text,
                              item_style_id text,
                              task_status_modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)
                          LEFT JOIN styles s ON x.item_style_id = s.client_style_id AND s.client_id = :client_id
                          ) AS t
                  ON u.client_id = :client_id AND u.client_job_status_id = t.task_status
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_job_status_id, style_id, description, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.task_status, t.style_id, t.desc_task_status, 'INTEGRATION', NOW(), 'INTEGRATION', t.task_status_modified_dttm)
                  RETURNING
                      u.job_status_id, merge_action(), to_jsonb(u) AS registro_json
              """)
          async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  setSnap = True
                  logger.info(f"ID: {row.job_status_id} | Ação JOB STATUS realizada: {row.merge_action}")

          if setSnap:
            snapKey = f'snapJobStatusTime:{settings.client_uid}'
            await r.set(snapKey, first_snap)

          # Criação do Places
          setSnap = False
          stmt = text(f"""
                  MERGE INTO places AS u
                  USING (
                      SELECT DISTINCT x.* FROM jsonb_to_recordset(:dados_json)
                      AS x(   place_id text,
                              trade_name text,
                              cnpj text,
                              place_modified_dttm TIMESTAMP
                      ) ) AS t
                  ON u.client_id = :client_id AND u.client_place_id = t.place_id
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_place_id, trade_name, cnpj, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.place_id, t.trade_name, t.cnpj, 'INTEGRATION', NOW(), 'INTEGRATION', t.place_modified_dttm)
                  RETURNING
                       u.place_id, merge_action(), to_jsonb(u) AS registro_json
                  """)
          async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  setSnap = True
                  logger.info(f"ID: {row.place_id} | Ação PLACE realizada: {row.merge_action}")

          if setSnap:
            snapKey = f'snapPlaceTime:{settings.client_uid}'
            await r.set(snapKey, first_snap)

          #Criação do Address
          setSnap = False
          stmt = text(f"""
              MERGE INTO address AS u
              USING (SELECT DISTINCT x.*
                      FROM jsonb_to_recordset(:dados_json)
                        AS x(
                          address_id text,
                          address text,
                          geocode_lat text,
                          geocode_long text,
                          city text,
                          state_prov text,
                          zippost text,
                          address_modified_dttm TIMESTAMP,
                          last_snap TIMESTAMP)) AS t
              ON u.client_id = :client_id AND u.client_address_id = t.address_id
              WHEN NOT MATCHED THEN
                  INSERT (client_id, client_address_id, address, geocode_lat, geocode_long, city , state_prov, zippost, created_by, created_date, modified_by, modified_date)
                  VALUES (:client_id, t.address_id, t.address, t.geocode_lat, t.geocode_long, t.city, t.state_prov, t.zippost, 'INTEGRATION', NOW(), 'INTEGRATION', t.address_modified_dttm)
              RETURNING
                  u.address_id, merge_action(), to_jsonb(u) AS registro_json
          """)

          async with SessionLocal() as db:
            result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
            await db.commit()
            for row in result:
              # setSnap = True
              logger.info(f"ID: {row.address_id} | Ação ADDRESS realizada: {row.merge_action}")

          # if setSnap:
          #   snapKey = f'snapAddressTime:{settings.client_uid}'
          #   await r.set(snapKey, first_snap)

           #Criação e atualização do Job
          stmt = text(f"""
              MERGE INTO jobs AS u
              USING (SELECT x.*,
                      CONCAT(x.request_id, '|', x.task_id) AS client_job_id,
                      t.team_id AS team_idd,
                      r.resource_id,
                      a.address_id AS address_idd,
                      p.place_id AS place_idd,
                      jt.job_type_id,
                      js.job_status_id,
                      COALESCE(py.priority,0) AS priority

                      FROM jsonb_to_recordset(:dados_json)
                        AS x( request_id text,
                              contr_type text,
                              task_id text,
                              team_id text,
                              person_id text,
                              address_id text,
                              place_id text,
                              task_type text,
                              task_status text,
                              created_dttm TIMESTAMP,
                              modified_dttm TIMESTAMP,
                              work_duration NUMERIC,
                              plan_start_dttm TIMESTAMP,
                              plan_end_dttm TIMESTAMP,
                              actual_start_dttm TIMESTAMP,
                              actual_end_dttm TIMESTAMP,
                              sla TIMESTAMP,
                              plan_task_dur_min integer,
                              pp_person_id text,
                              pp_actual_start_dttm TIMESTAMP,
                              pp_actual_end_dttm TIMESTAMP,
                              pt_task_id text,
                              pt_actual_start_dttm TIMESTAMP,
                              pt_actual_end_dttm TIMESTAMP,
                              pt_geocode_lat text,
                              pt_geocode_long text,
                              last_snap TIMESTAMP)
                      LEFT JOIN resources r ON x.person_id = r.client_resource_id AND :client_id = r.client_id
                      JOIN teams t ON t.client_team_id = x.team_id AND t.client_id = :client_id
                      join address a ON a.client_address_id = x.address_id AND a.client_id = :client_id
                      JOIN job_types jt ON jt.client_job_type_id = x.task_type AND jt.client_id = :client_id
                      JOIN job_status js ON js.client_job_status_id = x.task_status AND js.client_id = :client_id
                      JOIN places p ON p.client_place_id = x.place_id AND p.client_id = :client_id
                      LEFT JOIN priority py ON x.contr_type = py.client_priority_id AND :client_id = py.client_id
                      ) AS t
              ON u.client_id = :client_id AND u.client_job_id = t.client_job_id
              WHEN MATCHED AND (
                             u.team_id IS DISTINCT FROM t.team_idd
                          OR u.resource_id IS DISTINCT FROM t.resource_id
                          OR u.address_id IS DISTINCT FROM t.address_idd
                          OR u.place_id IS DISTINCT FROM t.place_idd
                          OR u.job_type_id IS DISTINCT FROM t.job_type_id
                          OR u.job_status_id IS DISTINCT FROM t.job_status_id
                          OR u.created_date IS DISTINCT FROM t.created_dttm
                          OR u.work_duration IS DISTINCT FROM t.work_duration::INTEGER
                          OR u.plan_start_date IS DISTINCT FROM t.plan_start_dttm
                          OR u.plan_end_date IS DISTINCT FROM t.plan_end_dttm
                          OR u.actual_start_date IS DISTINCT FROM t.actual_start_dttm
                          OR u.actual_end_date IS DISTINCT FROM t.actual_end_dttm
                          OR u.time_limit_end IS DISTINCT FROM t.sla
                          OR u.time_limit_start IS DISTINCT FROM t.plan_start_dttm
                          OR u.time_service IS DISTINCT FROM t.plan_task_dur_min
                          OR u.pp_resource_id IS DISTINCT FROM t.pp_person_id
                          OR u.pp_start_date IS DISTINCT FROM t.pp_actual_start_dttm
                          OR u.pp_end_date IS DISTINCT FROM t.pp_actual_end_dttm
                          OR u.pt_job_id IS DISTINCT FROM t.pt_task_id
                          OR u.pt_start_date IS DISTINCT FROM t.pt_actual_start_dttm
                          OR u.pt_end_date IS DISTINCT FROM t.pt_actual_end_dttm
                          OR u.pt_geocode_lat IS DISTINCT FROM t.pt_geocode_lat
                          OR u.pt_geocode_long IS DISTINCT FROM t.pt_geocode_long
                          OR u.priority IS DISTINCT FROM t.priority
                          ) THEN
                  UPDATE SET team_id = t.team_idd
                      ,resource_id = t.resource_id
                      ,address_id = t.address_idd
                      ,place_id = t.place_idd
                      ,job_type_id = t.job_type_id
                      ,job_status_id = t.job_status_id
                      ,work_duration = t.work_duration::INTEGER
                      ,plan_start_date = t.plan_start_dttm
                      ,plan_end_date = t.plan_end_dttm
                      ,actual_start_date = t.actual_start_dttm
                      ,actual_end_date = t.actual_end_dttm
                      ,time_limit_end = t.sla
                      ,time_limit_start = t.plan_start_dttm
                      ,time_service = t.plan_task_dur_min
                      ,pp_resource_id = t.pp_person_id
                      ,pp_start_date = t.pp_actual_start_dttm
                      ,pp_end_date = t.pp_actual_end_dttm
                      ,pt_job_id = t.pt_task_id
                      ,pt_start_date = t.pt_actual_start_dttm
                      ,pt_end_date = t.pt_actual_end_dttm
                      ,pt_geocode_lat = t.pt_geocode_lat
                      ,pt_geocode_long = t.pt_geocode_long
                      ,priority = t.priority
                      ,modified_date = t.modified_dttm
              WHEN NOT MATCHED THEN
                  INSERT (client_id, client_job_id, team_id, resource_id, address_id, place_id, job_type_id, job_status_id, work_duration, plan_start_date, plan_end_date, actual_start_date, actual_end_date, time_limit_end, time_limit_start, time_service, pp_resource_id, pp_start_date, pp_end_date, pt_job_id, pt_start_date, pt_end_date, pt_geocode_lat, pt_geocode_long, priority, created_by, created_date, modified_by, modified_date)
                  VALUES (:client_id, t.client_job_id, t.team_idd, t.resource_id, t.address_idd, t.place_idd, t.job_type_id, t.job_status_id, t.work_duration::INTEGER, t.plan_start_dttm, t.plan_end_dttm, t.actual_start_dttm, t.actual_end_dttm, t.sla, t.plan_start_dttm, t.plan_task_dur_min, t.pp_person_id, t.pp_actual_start_dttm, t.pp_actual_end_dttm, t.pt_task_id, t.pt_actual_start_dttm, t.pt_actual_end_dttm, t.pt_geocode_lat, t.pt_geocode_long, t.priority, 'INTEGRATION', t.created_dttm, 'INTEGRATION', t.modified_dttm)
              RETURNING
                  to_jsonb(u) AS registro_json, merge_action(), u.job_id
          """)

          async with SessionLocal() as db:
            result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
            await db.commit()
            for row in result:
                type = row.merge_action + ' ON JOB'
                logger.info(f"ID: {row.job_id} | Ação JOB realizada: {row.merge_action}")

          await r.set(snapKey, last_snap)
      except Exception as e:
                logger.error(f"[getJobs] Erro ao processar postgres: {e}")
    return

async def getPriority(r):
    logger.warning("[getPriority] Entrou atualização das Janelas de Recursos ...")


    result_rows = await getPriorityMatrix()
    if result_rows:
          try:
              jsonResults = json.dumps(result_rows)

              stmt = text(f"""
                  MERGE INTO priority AS u
                  USING (SELECT
                            x.*
                          FROM jsonb_to_recordset(:dados_json)
                            AS x(
                              contr_type text,
                              ranking integer) ) AS t
                  ON u.client_id = :client_id AND u.client_priority_id = t.contr_type
                  WHEN MATCHED AND (
                              u.priority IS DISTINCT FROM t.ranking
                              )  THEN
                      UPDATE SET 
                          priority = t.ranking
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = NOW()
                  WHEN NOT MATCHED THEN
                      INSERT (client_id, client_priority_id, priority, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.contr_type, t.ranking, 'INTEGRATION', NOW(), 'INTEGRATION', NOW())
                  RETURNING
                       u.priority_id, merge_action(), to_jsonb(u) AS registro_json
              """)

              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  logger.info(f"ID: {row.priority_id} | Ação PRIORITY realizada: {row.merge_action}")


          except Exception as e:
                    logger.error(f"[getResourceWindow] Erro ao processar postgres: {e}")

    return


async def processo_em_background(r):
    SLEEP_NORMAL = 10
    SLEEP_MAX = 300
    consecutive_failures = 0

    try:
        while True:
            try:
                await getStyle(r)
                await asyncio.sleep(0.2)
                await getResources(r)
                await asyncio.sleep(0.2)
                await getResourceWindow(r)
                await asyncio.sleep(0.2)
                await getGeoPos(r)
                await asyncio.sleep(0.2)
                await getLogInOut(r)
                await asyncio.sleep(0.2)
                await getJobs(r)
                await asyncio.sleep(0.2)
                await getTeam(r)
                await asyncio.sleep(0.2)
                await getTeamMember(r)
                await asyncio.sleep(0.2)
                await getAddress(r)
                await asyncio.sleep(0.2)
                await getPlaces(r)
                await asyncio.sleep(0.2)
                await getJobType(r)
                await asyncio.sleep(0.2)
                await getJobStatus(r)

                if consecutive_failures > 0:
                    logger.info(f"Background job recuperada após {consecutive_failures} falha(s) consecutiva(s).")
                consecutive_failures = 0
                sleep_time = SLEEP_NORMAL

            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                sleep_time = min(SLEEP_NORMAL * (2 ** consecutive_failures), SLEEP_MAX)
                logger.error(f"Erro na iteração do background job (falha #{consecutive_failures}): {e}. Próxima tentativa em {sleep_time}s.")

            logger.warning(f'Aguardando {sleep_time}s para próximo refresh ...')
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Processo contínuo foi encerrado.")


async def processo_em_background_longo(r):
    SLEEP_NORMAL = 300
    SLEEP_MAX = 600
    consecutive_failures = 0

    try:
        while True:
            try:
                # await deleteJobs()
                # await asyncio.sleep(0.5)
                await calcDistance()
                await asyncio.sleep(0.5)
                await buildReports()
                # await asyncio.sleep(0.5)
                # await asyncio.sleep(0.5)
                # await getPriority(r)

                if consecutive_failures > 0:
                    logger.info(f"Background job recuperada após {consecutive_failures} falha(s) consecutiva(s).")
                consecutive_failures = 0
                sleep_time = SLEEP_NORMAL

            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                sleep_time = min(SLEEP_NORMAL * (2 ** consecutive_failures), SLEEP_MAX)
                logger.error(f"Erro na iteração do background job (falha #{consecutive_failures}): {e}. Próxima tentativa em {sleep_time}s.")

            logger.warning(f'Aguardando {sleep_time}s para próximo refresh ...')
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Processo contínuo foi encerrado.")


async def main():
    
    r = redis_client.get_redis()
    logger.info("Sync worker iniciado.")

    async with engine.begin() as conn:
        
        # await conn.run_sync(Base.metadata.drop_all)
        # logger.info("Tabelas do banco dropadas com sucesso!")
        # await r.flushall()
        # logger.info("Cache Redis limpo com sucesso!")

        await conn.run_sync(Base.metadata.create_all)
        logger.info("Tabelas do banco verificadas/criadas com sucesso!")

    async with SessionLocal() as db:
        rClient = await db.execute(select(models.Clients).where(models.Clients.domain == "Admin"))
        clientDb = rClient.scalars().first()
        if not clientDb:
            logger.info("Criando cliente Admin...")
            db.add(models.Clients(
                name="Administration",
                domain="Admin",
                created_by="admin",
                created_date=datetime.now(),
                modified_by="admin",
                modified_date=datetime.now()
            ))
            await db.commit()

        rUser = await db.execute(select(models.Users).where(models.Users.user_name == "admin"))
        userDb = rUser.scalars().first()
        if not userDb:
            logger.info("Criando usuário 'admin'...")
            db.add(models.Users(
                client_id=1,
                user_name="admin",
                name="Administrator",
                passwd=get_password_hash("123456"),
                super_user=1,
                created_by="admin",
                created_date=datetime.now(),
                modified_by="admin",
                modified_date=datetime.now()
            ))
            await db.commit()

    try:
        await asyncio.gather(
            # processo_em_background(r),
            processo_em_background_longo(r)
            # adicione outros workers aqui, ex: outro_processo(r),
        )
    finally:
        await r.aclose()
        logger.info("Sync worker encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
