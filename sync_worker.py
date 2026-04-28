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
from services import optimize_routes_vroom, get_route_distance_block, logs , geocode_mapbox
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

async def calc_rote(clientId, teamId, userId, scheduleId, frequency='DAILY'):
    stmt = text(f"""
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
                ,CASE 
                    WHEN j.time_limit_end IS NOT NULL AND date_trunc('day',now()) = date_trunc('day',j.time_limit_end) 
                    THEN 
                        EXTRACT (epoch FROM (j.time_limit_end - date_trunc('day',now())))::INTEGER
                    ELSE
                    EXTRACT (epoch FROM COALESCE (aw.end_time, CAST ('23:59:59' AS TIME)))::INTEGER
                END AS end_time
            FROM jobs  j
                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                LEFT JOIN address_windows aw
                ON     j.client_id = aw.client_id
                    AND j.address_id = aw.address_id
                    AND EXTRACT(DOW FROM now()) + 1 = aw.week_day
        WHERE   j.client_id = :client_id
            AND t.team_id = :team_id
            AND (js.internal_code_status not in ('CONCLU', 'CANCEL', 'CLOSED') AND  js.internal_code_status IS NOT NULL)
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
        schedule_resources AS (
            SELECT 
                s.client_id, 
                s.schedule_id, 
                s.team_id,
                s.type_resources,
                (item->>'resource_id')::INT AS resource_id
            FROM schedules s
            LEFT JOIN jsonb_array_elements(
                CASE 
                    WHEN jsonb_typeof(s.resources) = 'array' THEN s.resources 
                    ELSE '[]'::jsonb 
                END
            ) AS item ON true
            WHERE s.client_id = :client_id
            AND s.status = 'A'
            AND s.schedule_id = :schedule_id
        ),
        vehicles_data AS (
            select
            r.resource_id,
            r.description,
            COALESCE(r.geocode_lat_start,t.geocode_lat)::NUMERIC AS geocode_lat_start,
            COALESCE(r.geocode_long_start,t.geocode_long)::NUMERIC AS geocode_long_start,
            COALESCE(r.geocode_lat_end,t.geocode_lat)::NUMERIC AS geocode_lat_end,
            COALESCE(r.geocode_long_end,t.geocode_long)::NUMERIC AS geocode_long_end,
            EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.start_time,t.start_time) else r.off_shift_start_time end ) ::INTEGER AS start_time,
            EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.end_time,t.end_time) else r.off_shift_end_time end ) ::INTEGER AS end_time
            from resources r
            join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
            join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
            JOIN schedule_resources s ON s.client_id = r.client_id AND s.team_id = t.team_id
            LEFT JOIN resource_windows rw on r.client_id = rw.client_id and r.resource_id = rw.resource_id and rw.week_day = EXTRACT(DOW FROM now()) + 1
            WHERE r.client_id = :client_id
            AND t.team_id = :team_id
            AND (
                s.type_resources = 'A' 
                OR (s.type_resources = 'S' AND r.resource_id = s.resource_id)
            )
            AND COALESCE(r.geocode_lat_start, t.geocode_lat) IS NOT NULL
            AND COALESCE(r.geocode_long_start, t.geocode_long) IS NOT NULL
            AND COALESCE(r.geocode_lat_end, t.geocode_lat) IS NOT NULL
            AND COALESCE(r.geocode_long_end, t.geocode_long) IS NOT NULL
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
    """).bindparams(client_id=clientId, schedule_id = scheduleId, team_id = teamId)

    
    if frequency == 'WEEKLY':
        stmt = text(f"""
        WITH dados AS (
            SELECT j.client_id
                ,t.team_id
                ,COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap,0) AS time_overlap
                ,j.job_id
                ,j.address_id
                ,a.geocode_lat::NUMERIC geocode_lat
                ,a.geocode_long::NUMERIC geocode_long
                ,COALESCE (j.time_setup, jt.time_setup, t.time_setup, 0) AS time_setup
                ,COALESCE (j.time_service, jt.time_service, t.time_service, 0) AS time_service
                ,j.priority + (COALESCE (jt.priority, 0) / 100)::INTEGER AS priority
                ,json_agg( 
                    json_build_array(
                        CASE 
                            WHEN j.time_limit_end IS NOT NULL AND date_trunc('day',now()) = date_trunc('day',j.time_limit_end) 
                            THEN EXTRACT(epoch FROM (date_trunc('day',now())))::INTEGER
                            ELSE null
                        END,
                        CASE 
                            WHEN j.time_limit_end IS NOT NULL AND j.time_limit_end >= date_trunc('day',now())
                            THEN EXTRACT(epoch FROM (j.time_limit_end::timestamptz))::INTEGER
                            ELSE null
                        END
                    )
                ) FILTER (
                    -- Invertemos a sua lógica: Só agrega se a condição abaixo for verdadeira
                    WHERE aw.start_time IS NOT NULL 
                        OR (j.time_limit_end IS NOT NULL AND date_trunc('day',now()) = date_trunc('day',j.time_limit_end))
                ) AS time_windows
            FROM jobs  j
                JOIN job_status js ON js.client_id = j.client_id AND js.job_status_id = j.job_status_id
                JOIN job_types jt ON jt.client_id = j.client_id AND jt.job_type_id = j.job_type_id
                JOIN address a ON a.client_id = j.client_id AND a.address_id = j.address_id
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
                LEFT JOIN address_windows aw
                ON     j.client_id = aw.client_id
                    AND j.address_id = aw.address_id
                    AND aw.week_day >= EXTRACT(DOW FROM now()) + 1 -- Considera as janelas de toda a semana seguinte
        WHERE   j.client_id = :client_id
            AND t.team_id = :team_id
            AND (js.internal_code_status not in ('CONCLU', 'CANCEL', 'CLOSED') AND  js.internal_code_status IS NOT NULL)
        GROUP BY 1,2,3,4,5,6,7,8,9,10
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
            time_windows
            FROM MapeamentoEnderecos
            WHERE ordem_job = 1
        ),
        schedule_resources AS (
            SELECT 
                s.client_id, 
                s.schedule_id, 
                s.team_id,
                s.type_resources,
                (item->>'resource_id')::INT AS resource_id
            FROM schedules s
            LEFT JOIN jsonb_array_elements(
                CASE 
                    WHEN jsonb_typeof(s.resources) = 'array' THEN s.resources 
                    ELSE '[]'::jsonb 
                END
            ) AS item ON true
            WHERE s.client_id = :client_id
            AND s.status = 'A'
            AND s.schedule_id = :schedule_id
        ),
        resources_data AS (
            SELECT
                CAST(rw.week_day*100 as text)||CAST(r.resource_id AS TEXT)::INTEGER AS resource_id,
                r.resource_id ||':' ||rw.week_day AS description,
                COALESCE(r.geocode_lat_start,t.geocode_lat)::NUMERIC AS geocode_lat_start,
                COALESCE(r.geocode_long_start,t.geocode_long)::NUMERIC AS geocode_long_start,
                COALESCE(r.geocode_lat_end,t.geocode_lat)::NUMERIC AS geocode_lat_end,
                COALESCE(r.geocode_long_end,t.geocode_long)::NUMERIC AS geocode_long_end,
                CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.start_time,t.start_time) else r.off_shift_start_time end AS start_time,
                CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.end_time,t.end_time) else r.off_shift_end_time end AS end_time,
                rw.week_day-1 AS week_day
            FROM resources r
                join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                JOIN schedule_resources s ON s.client_id = r.client_id AND s.team_id = t.team_id
                JOIN resource_windows rw on r.client_id = rw.client_id and r.resource_id = rw.resource_id AND rw.week_day >= EXTRACT(DOW FROM now()) + 1
            WHERE r.client_id = :client_id
            AND t.team_id = :team_id
            AND (
                s.type_resources = 'A' 
                OR (s.type_resources = 'S' AND r.resource_id = s.resource_id)
            )
            AND COALESCE(r.geocode_lat_start, t.geocode_lat) IS NOT NULL
            AND COALESCE(r.geocode_long_start, t.geocode_long) IS NOT NULL
            AND COALESCE(r.geocode_lat_end, t.geocode_lat) IS NOT NULL
            AND COALESCE(r.geocode_long_end, t.geocode_long) IS NOT NULL
        ),
        vehicles_data AS (
            SELECT 
                resource_id::INTEGER AS resource_id,
                description,
                geocode_lat_start,
                geocode_long_start,
                geocode_lat_end,
                geocode_long_end,
                (EXTRACT(EPOCH FROM (date_trunc('day', now())::date + (week_day - EXTRACT(DOW FROM date_trunc('day', now()))::int)) AT TIME ZONE 'UTC')
                    + EXTRACT(EPOCH FROM start_time) )::INTEGER AS start_time,
                (EXTRACT(EPOCH FROM (date_trunc('day', now())::date + (week_day - EXTRACT(DOW FROM date_trunc('day', now()))::int)) AT TIME ZONE 'UTC')
                    + EXTRACT(EPOCH FROM end_time) )::INTEGER AS end_time
            FROM resources_data
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
                    'time_windows', time_windows
                ))
            ) AS array_jobs
            FROM q1
        )
        SELECT json_build_object(
            'vehicles', (SELECT array_vehicles FROM vehicles_json),
            'jobs', (SELECT array_jobs FROM jobs_json),
            'list', (SELECT job_id_list FROM list)
        ) AS vroom_payload;
    """).bindparams(client_id=clientId, schedule_id = scheduleId, team_id = teamId)

    async with SessionLocal() as db:
        result = await db.execute(stmt)
        subRows = result.mappings().all()

    if not subRows or len(subRows) == 0:
        return
    asyncio.create_task(logs(clientId=clientId,log=f'dados para vroom Schedule - {scheduleId}',logJson=dict(subRows[0])))
    vroom_payload = subRows[0]['vroom_payload']
    
    vroomJobList = vroom_payload.pop('list', 'Chave não encontrada')
    logger.info(list(vroomJobList))
    logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
    print(vroom_payload)
    retorno = await optimize_routes_vroom(vroom_payload)

    routes = retorno['routes']
    geos = [
        [[step['location'][0], step['location'][1]] for step in rou['steps']]
        for rou in routes
    ]
    geo_results = await asyncio.gather(*[get_route_distance_block(geo) for geo in geos])

    for rou, geoResult in zip(routes, geo_results):
        asyncio.create_task(logs(clientId=clientId, log=f'montagem do geo - {scheduleId}', logJson=[[s['location'][0], s['location'][1]] for s in rou['steps']]))
        asyncio.create_task(logs(clientId=clientId, log=f'retorno do geo - {scheduleId}', logJson=geoResult))
        rou['steps'][0]['time_distance'] = 0
        rou['steps'][0]['distance'] = 0
        for ln, x in enumerate(geoResult, start=1):
            rou['steps'][ln]['time_distance'] = x['duration']
            rou['steps'][ln]['distance'] = x['distance']

    listIds = [item['id'] for item in retorno.get("unassigned", [])]
    if listIds:
        asyncio.create_task(logs(clientId=clientId,log=f'lista de unassigned - {scheduleId}',logJson=listIds))
        logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
    
    asyncio.create_task(logs(clientId=clientId,log=f'retorno do vroom - {scheduleId}',logJson=retorno))
    
    vroomResult = json.dumps(retorno)
    print(vroomResult)

    stmt = text(f"""
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
                    DATE_TRUNC('day', now()) AS job_day,
                    COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap, 0) AS time_overlap,
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
                    PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC) AS ordem_job,
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
                and j.team_id = :team_id
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
            join dados_jobs j ON j.client_id = :client_id and j.new_job_id = q.job_id
            join resources r ON r.client_id = :client_id and r.resource_id = q.resource_id
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
    """).bindparams(client_id=clientId,team_id = teamId)

    if frequency == 'WEEKLY':
        stmt = text(f"""
            WITH payload_data AS (
                SELECT '{vroomResult}'::jsonb AS data
            ),
            rotas AS (
                SELECT 
                    substr((rota_json->>'vehicle'),4)::int AS id_veiculo,
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
                    DATE_TRUNC('day', to_timestamp((passo_json->>'arrival')::int))::timestamp AS job_day,
                    (passo_json->>'service')::int AS service,
                    (passo_json->>'setup')::int AS setup,
                    (passo_json->>'distance')::numeric::int AS distance,
                    (passo_json->>'time_distance')::numeric::int AS time_distance
                FROM rotas r,
                    jsonb_array_elements(r.passos_array) WITH ORDINALITY AS passo(passo_json, ordem_passo)
            )
            --select * from q1
            ,
            dados_jobs as(
                select  j.client_id,
                        j.team_id,
                        j.job_id,
                        COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap, 0) AS time_overlap,
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
                        PARTITION BY j.client_id, j.team_id, a.geocode_lat, a.geocode_long ORDER BY job_id ASC) AS ordem_job,
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
                    and j.team_id = :team_id
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
                    q.job_day,
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
                join dados_jobs j ON j.client_id = :client_id and j.new_job_id = q.job_id
                join resources r ON r.client_id = :client_id and r.resource_id = q.resource_id
                join q1 q_1 on q_1.resource_id = q.resource_id and q_1.stop_type = 'start' and q_1.job_day = q.job_day
                join q1 q_2 on q_2.resource_id = q.resource_id and q_2.stop_type = 'end' and q_2.job_day = q.job_day
                join q1 q_3 on q_3.resource_id = q.resource_id and q_3.stop_type = 'job' AND q_3.sequence = 2 and q_3.job_day = q.job_day
            ),
            dados_new as (
                select 
                a.*,
                to_timestamp(arrival)::timestamp start_date,
                to_timestamp(arrival + setup + service)::timestamp end_date,
                to_timestamp(arrival_from)::timestamp date_from,
                to_timestamp(arrival_at)::timestamp date_at
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
            --select * from q1
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
        """).bindparams(client_id=clientId,team_id = teamId)
    async with SessionLocal() as db:
        result = await db.execute(stmt)

    subSubRows = result.mappings().all()

    if not subSubRows or len(subSubRows) == 0:
        return
    
    asyncio.create_task(logs(clientId=clientId,log=f'Resultado da Simulacao {scheduleId}',logJson=subSubRows[0]))
    res = subSubRows[0]['res']

    rep_json = json.dumps(res)
    
    logger.warning('Inserindo simulação agendada ...')   
    stmt = text(f"""
        INSERT INTO simulation (client_id, user_id, team_id, json_dado, simulation_date, fl_calc_plan, sequence, created_by, created_date, modified_by, modified_date )
        VALUES (:client_id, :user_id, :team_id, :json_data, DATE_TRUNC('day', NOW()), 1, 1, 'system', now(), 'system', NOw());
    """)

    parametros = {
        "client_id": clientId,
        "user_id": userId,
        "team_id": teamId,
        "json_data": rep_json
    }
    async with SessionLocal() as db:
        result = await db.execute(stmt, parametros)
        await db.commit()

    return

async def deleteJobs():
    
    tempDateTime = (datetime.now() - timedelta(days=15))
    pDate = tempDateTime.strftime('%Y-%m-%d')
    #vamos verificar se existe alguma tarefa concluida sem distância calculada
    async with SessionLocal() as db:
      stmt = text("DELETE FROM jobs WHERE created_date < CAST(:p_date AS DATE);").bindparams(p_date=pDate)
      await db.execute(stmt)
      # stmt = text("""DELETE FROM address a
      #                 WHERE NOT EXISTS (SELECT 1 
      #                                     FROM jobs j 
      #                                     WHERE j.client_id = a.client_idclient_id = :client_id
      #                                       AND j.address_id = a.address_id);
      #             """)
      # await db.execute(stmt)
      await db.commit()

async def checkPendencias(r):
    try:
        stmt = text("""
            select distinct 
                a.state_prov, a.city, a.address
            from address a
                join jobs j on j.address_id = a.address_id
            where a.geocode_lat::NUMERIC =0  
                OR a.geocode_long::NUMERIC = 0
                OR a.geocode_lat::NUMERIC < -40 
                OR a.geocode_lat::NUMERIC > 10
                OR a.geocode_long::NUMERIC < -80 
                OR a.geocode_long::NUMERIC > -30
                OR a.geocode_lat is null
                OR a.geocode_long is null
                order by a.state_prov, a.city, a.address   
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt)
            rows = result.mappings().all()

        for row in rows:
            address = f'{row.address}, {row.city}, {row.state_prov}'
            retorno = await geocode_mapbox(address)
            geocode_long = retorno["longitude"]
            geocode_lat = retorno["latitude"]
            stmt = text("""
                update address
                    set geocode_long = :geocode_long
                        ,geocode_lat = :geocode_lat
                    where state_prov = :state_prov
                    and city = :city
                    and address = :address
            """).bindparams(address=row.address, state_prov=row.state_prov, city=row.city, geocode_long=geocode_long, geocode_lat=geocode_lat)
            async with SessionLocal() as db:
                result = await db.execute(stmt)
                await db.commit()
            print(retorno)

        stmt = text("""
            UPDATE resources 
                set geocode_lat_start = NULL,
                    geocode_long_start = NULL,
                    geocode_long_end = NULL,
                    geocode_lat_end = NULL
            WHERE  COALESCE(geocode_lat_start::NUMERIC, 0) = 0  
                OR COALESCE(geocode_long_start::NUMERIC,0) = 0
                OR geocode_lat_start::NUMERIC < -40 
                OR geocode_lat_start::NUMERIC > 10
                OR geocode_long_start::NUMERIC < -80 
                OR geocode_long_start::NUMERIC > -30
                OR COALESCE(geocode_lat_end::NUMERIC, 0) = 0  
                OR COALESCE(geocode_long_end::NUMERIC, 0) = 0
                OR geocode_lat_end::NUMERIC < -40 
                OR geocode_lat_end::NUMERIC > 10
                OR geocode_long_end::NUMERIC < -80 
                OR geocode_long_end::NUMERIC > -30;
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt)
            await db.commit()

        stmt = text("""
            UPDATE teams 
                set geocode_lat = NULL,
                    geocode_long = NULL
            WHERE  COALESCE(geocode_lat::NUMERIC, 0) = 0  
                OR COALESCE(geocode_long::NUMERIC, 0) = 0
                OR geocode_lat::NUMERIC < -40 
                OR geocode_lat::NUMERIC > 10
                OR geocode_long::NUMERIC < -80 
                OR geocode_long::NUMERIC > -30;
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt)
            await db.commit()

        
        
        semaforo_key = f'semaforo:{settings.client_uid}'
        semaforo_raw = await r.get(semaforo_key)
        semaforo = json.loads(semaforo_raw) if semaforo_raw else {}
        semaforo['check_pendencias'] = 'Y'
        await r.set(semaforo_key, json.dumps(semaforo))

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
    
        # Extrai a última linha da pilha (onde o erro ocorreu)
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")            
async def buildAdjustmentScheduled():
    try:
        stmt = text("""
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
                WHERE (js.internal_code_status <> 'CONCLU' OR js.internal_code_status IS NULL)
                    AND j.client_id = 1
                    AND j.team_id = 33
                    AND COALESCE(j.actual_start_date, j.plan_start_date) >= date_trunc('day',NOW())
                    AND COALESCE(j.actual_start_date,j.plan_start_date) < date_trunc('day',NOW()) + interval '1 day'
                   
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
                    resource_id,
                    DATE_TRUNC('day', actual_date) as actual_date, 
                    count(*) total 
                from qjobs
                group by client_id, team_id, resource_id, DATE_TRUNC('day', actual_date)
                order by 1,2,3, 4
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt)
            rows = result.mappings().all()
            if not rows or len(rows) ==0:
                return
            for row in rows:
                clientId = row.client_id
                asyncio.create_task(logs(clientId=clientId,log='jobs',logJson=row))
                teamId = row.team_id
                resourceId = row.resource_id
                actualDate = row.actual_date

                logger.info("Iniciado Calculo das rotas ...")
                stmt = text(f"""
                    WITH dados AS (
                        SELECT 
                            j.client_id
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
                            where j.client_id = :client_id
                                AND j.team_id = :team_id
                                AND a.geocode_lat is not null
                                AND a.geocode_long is not null
                                and (a.geocode_lat::NUMERIC) < 100
                                AND (a.geocode_long::NUMERIC) < 100
                                and j.resource_id = :resource_id
                                and (js.internal_code_status <> 'CONCLU' OR js.internal_code_status IS NULL)
                                and COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
                                AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'),
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
                    last_task as(
                        select 
                            j.resource_id, 
                            a.geocode_lat, 
                            a.geocode_long,
                            j.actual_end_date, 
                            j.plan_end_date
                        from jobs j
                        join job_status js on js.client_id = j.client_id and js.job_status_id = j.job_status_id
                        join teams t on t.client_id = j.client_id and t.team_id = j.team_id
                        join address a on a.client_id = j.client_id and a.address_id = j.address_id
                        where j.client_id = :client_id
                            and j.team_id = :team_id
                            and j.resource_id = :resource_id
                            and js.internal_code_status = 'CONCLU'
                            and COALESCE(j.actual_start_date,j.plan_start_date) >= :p_date
                                            AND COALESCE(j.actual_start_date,j.plan_start_date) < :p_date + interval '1 day'
                            order by j.team_id, j.resource_id,j.actual_end_date desc, j.plan_end_date desc
                        limit 1
                    ),
                    vehicles_data AS (
                        select
                            r.resource_id,
                            r.description,
                            COALESCE(l.geocode_lat,r.geocode_lat_start)::NUMERIC as geocode_lat_start,
                            COALESCE(l.geocode_long,r.geocode_long_start)::NUMERIC as geocode_long_start,
                            r.geocode_lat_end::NUMERIC,
                            r.geocode_long_end::NUMERIC,
                            CASE WHEN l.actual_end_date IS NULL THEN
                                EXTRACT(EPOCH FROM COALESCE(rw.start_time,t.start_time)) ::INTEGER
                            ELSE
                                EXTRACT(EPOCH FROM l.actual_end_date - DATE_TRUNC('day',l.actual_end_date)) ::INTEGER
                            END AS start_time,
                            EXTRACT(EPOCH FROM CASE WHEN r.fl_off_shift = 0 then COALESCE(rw.end_time,t.end_time) else cast('23:59:59' as time) end ) ::INTEGER AS end_time
                        from resources r
                            join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                            join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                            LEFT JOIN resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM :p_date) + 1
                            left join last_task l on r.resource_id = l.resource_id
                        where r.client_id = :client_id
                            and t.team_id = :team_id
                            and r.resource_id = :resource_id
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
                """).bindparams(client_id=clientId, p_date = actualDate, team_id = teamId, resource_id = resourceId)
                
                result = await db.execute(stmt)
                subRows = result.mappings().all()
                if not subRows or len(subRows) == 0:
                    return
                # asyncio.create_task(logs(clientId=clientId,log='dados para vroom',logJson=dict(subRows[0])))
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
                    # await logs(clientId=clientId,log='montagem do geo',logJson=geo)
                    geoResult = await get_route_distance_block(geo)
                    # await logs(clientId=clientId,log='retorno do geo',logJson=geoResult)
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
                    # await logs(clientId=clientId,log='lista de unassigned',logJson=listIds)
                    logger.debug(f'Lista de jobs excluidas da rota... {listIds}')
                
                await logs(clientId=clientId,log='retorno do vroom Ajustment',logJson=retorno)
                vroomResult = json.dumps(retorno)
                stmt = text(f"""
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
                    )
                    update jobs j
                        set ajustment_start_date = d.start_date
                            ,ajustment_end_date = d.end_date
                        from dados_new d
                        where j.client_id = d.client_id
                        and j.job_id = d.job_id
            """).bindparams(client_id=clientId,p_date = actualDate)
                result = await db.execute(stmt)
                await db.commit()
        return
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
    
        # Extrai a última linha da pilha (onde o erro ocorreu)
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        
        arquivo = detalhes.filename
        linha = detalhes.lineno
        funcao = detalhes.name
        logger.error(f"Erro {e}")        
        logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")            

async def _calc_bra_report(clientId: int, teamId: int, actualDate, r: str):
    try:
        vBrac = """
                    AND EXISTS (
                        SELECT 1
                        FROM jobs x
                            JOIN job_status xs ON xs.client_id = x.client_id AND xs.job_status_id = x.job_status_id
                        WHERE x.client_id = r.client_id
                            and x.resource_id = r.resource_id
                            and xs.internal_code_status = 'CONCLU'
                            AND COALESCE(x.actual_start_date,x.plan_start_date) >= :p_date
                            AND COALESCE(x.actual_start_date,x.plan_start_date) < :p_date + interval '1 day'
                    )""" if r == 'BRAC' else ''

        logger.warning(f'Iniciando... {r} em {actualDate}')
        logger.info("Iniciado Calculo das rotas ...")
        stmt = text(f"""
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
                                COALESCE(r.geocode_lat_start,t.geocode_lat)::NUMERIC AS geocode_lat_start,
                                COALESCE(r.geocode_long_start,t.geocode_long)::NUMERIC AS geocode_long_start,
                                COALESCE(r.geocode_lat_end,t.geocode_lat)::NUMERIC AS geocode_lat_end,
                                COALESCE(r.geocode_long_end,t.geocode_long)::NUMERIC AS geocode_long_end,
                                EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.start_time,t.start_time) else r.off_shift_start_time end ) ::INTEGER AS start_time,
                                EXTRACT(EPOCH FROM CASE WHEN r.off_shift_flag = 0 then COALESCE(rw.end_time,t.end_time) else r.off_shift_end_time end ) ::INTEGER AS end_time
                            from resources r
                                join team_members tm on tm.client_id = r.client_id and tm.resource_id = r.resource_id
                                join teams t on t.client_id = tm.client_id and t.team_id = tm.team_id
                                LEFT JOIN resource_windows rw on rw.client_id = r.client_id and rw.resource_id = r.resource_id and rw.week_day = EXTRACT(DOW FROM :p_date) + 1
                            where r.client_id = :client_id
                                and t.team_id = :team_id
                                AND COALESCE(r.geocode_lat_start, t.geocode_lat) IS NOT NULL
                                AND COALESCE(r.geocode_long_start, t.geocode_long) IS NOT NULL
                                AND COALESCE(r.geocode_lat_end, t.geocode_lat) IS NOT NULL
                                AND COALESCE(r.geocode_long_end, t.geocode_long) IS NOT NULL
                                {vBrac if r =='BRAC' else ''}
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
        """).bindparams(client_id=clientId, p_date=actualDate, team_id=teamId)

        async with SessionLocal() as db:
            result = await db.execute(stmt)
            subRows = result.mappings().all()

        if not subRows:
            return []

        await logs(clientId=clientId, log='dados para vroom', logJson=dict(subRows[0]))
        vroom_payload = subRows[0]['vroom_payload']
        listJobs = vroom_payload.pop('list', [])
        if not vroom_payload['vehicles'] or not vroom_payload['jobs']:
            logger.debug(f'Veículos ou Jobs insuficientes para otimização. Veículos: {vroom_payload["vehicles"]}, Jobs: {vroom_payload["jobs"]}')
            return []
        
        logger.info('Iniciando Otimização das rotas Simulação Janela Default ...')
        retorno = await optimize_routes_vroom(vroom_payload)

        routes = retorno['routes']
        geos = [
            [[step['location'][0], step['location'][1]] for step in rou['steps']]
            for rou in routes
        ]
        geo_results = await asyncio.gather(*[get_route_distance_block(geo) for geo in geos])

        for rou, geoResult in zip(routes, geo_results):
            await logs(clientId=clientId, log='montagem do geo', logJson=[[s['location'][0], s['location'][1]] for s in rou['steps']])
            await logs(clientId=clientId, log='retorno do geo', logJson=geoResult)
            rou['steps'][0]['time_distance'] = 0
            rou['steps'][0]['distance'] = 0
            for ln, x in enumerate(geoResult, start=1):
                rou['steps'][ln]['time_distance'] = x['duration']
                rou['steps'][ln]['distance'] = x['distance']

        listIds = [item['id'] for item in retorno.get("unassigned", [])]
        if listIds:
            await logs(clientId=clientId, log='lista de unassigned', logJson=listIds)
            logger.debug(f'Lista de jobs excluidas da rota... {listIds}')

        await logs(clientId=clientId, log='retorno do vroom', logJson=retorno)
        vroomResult = json.dumps(retorno)

        stmt2 = text(f"""
            WITH payload_data AS (
                SELECT CAST(:vroom_data AS jsonb) AS data
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
                      COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap,0) AS time_overlap,
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
                    q_1.geocode_long AS geocode_long_start,
                    q_1.geocode_lat AS geocode_lat_start,
                    q_1.arrival AS arrival_from,
                    q_3.distance AS distance_from,
                    q_3.time_distance as time_distance_from,
                    q_2.geocode_long AS geocode_long_end,
                    q_2.geocode_lat AS geocode_lat_end,
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
                    end AS arrival,
                    j.time_service AS service,
                    CASE
                      WHEN j.ordem_job > 1 THEN 0
                      ELSE q.distance
                    end AS distance,
                    CASE
                      WHEN j.ordem_job > 1 THEN 0
                      ELSE q.time_distance
                    end AS time_distance
                from q1 q
                join dados_jobs j ON j.client_id = :client_id and j.new_job_id = q.job_id
                join resources r ON r.client_id = :client_id and r.resource_id = q.resource_id
                join q1 q_1 on q_1.resource_id = q.resource_id and q_1.stop_type = 'start'
                join q1 q_2 on q_2.resource_id = q.resource_id and q_2.stop_type = 'end'
                join q1 q_3 on q_3.resource_id = q.resource_id and q_3.stop_type = 'job' AND q_3.sequence = 2
            ),
            dados_new as (
              select
                a.*,
                a.job_day + (arrival * INTERVAL '1 second') start_date,
                a.job_day + ((arrival + setup + service) * INTERVAL '1 second') end_date,
                a.job_day + ((arrival_from) * INTERVAL '1 second') date_from,
                a.job_day + ((arrival_at) * INTERVAL '1 second') date_at
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
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
        """).bindparams(client_id=clientId, p_date=actualDate, vroom_data=vroomResult)

        async with SessionLocal() as db:
            result = await db.execute(stmt2)
            subSubRows = result.mappings().all()

        if not subSubRows:
            return []

        await logs(clientId=clientId, log=f'Resultado do {r}', logJson=subSubRows[0])
        res = subSubRows[0]['res']
        results = []
        if isinstance(res, list):
            results.extend(res)
        else:
            results.append(res)

        logger.warning(f'Finalizado ... {r}')
        return results

    except Exception as e:
        exc_traceback = sys.exc_info()[2]
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        logger.error(f"Erro _calc_bra_report({r}): {e} — {detalhes.filename}:{detalhes.lineno} ({detalhes.name})")
        return []

async def _calc_real_report(clientId: int, teamId: int, actualDate):
    try:
        logger.warning(f'Iniciando... REAL em {actualDate}')
        stmt = text("""
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
                    r.geocode_long_start,
                    r.geocode_lat_start,
                    r.geocode_long_end,
                    r.geocode_lat_end,
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
                JOIN teams t ON t.client_id = j.client_id AND t.team_id = j.team_id
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
            dados_calc as (
                select
                    c.*,
                    EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from AS arrival_from,
                    EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at AS arrival_at,
                    c.job_day + (EXTRACT(EPOCH FROM c.first_start_date::TIME)::INTEGER - c.time_distance_from) * INTERVAL '1 second' date_from,
                    c.job_day + (EXTRACT(EPOCH FROM c.last_end_date::TIME)::INTEGER + c.time_distance_at) * INTERVAL '1 second' date_at
                from dados c
            ),
            grp as (
                select
                    d.client_id,
                    d.resource_id,
                    d.client_resource_id,
                    d.resource_name,
                    d.job_day,
                    d.geocode_long_start,
                    d.geocode_lat_start,
                    d.distance_from,
                    d.time_distance_from,
                    d.arrival_from,
                    d.date_from,
                    d.geocode_long_end,
                    d.geocode_lat_end,
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
                    d.client_id, d.resource_id, d.client_resource_id, d.resource_name,
                    d.job_day, d.geocode_long_start, d.geocode_lat_start,
                    d.distance_from, d.time_distance_from, d.arrival_from, d.date_from,
                    d.geocode_long_end, d.geocode_lat_end, d.distance_at,
                    d.time_distance_at, d.arrival_at, d.date_at
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
        """)

        async with SessionLocal() as db:
            result = await db.execute(stmt,{
                "client_id": clientId, 
                "p_date": actualDate, 
                "team_id": teamId
                }
            )
            subRows = result.mappings().all()

        if not subRows:
            return []

        asyncio.create_task(logs(clientId=clientId, log='Resultado do REAL', logJson=subRows[0]))
        res = subRows[0]['res']
        results = []
        if isinstance(res, list):
            results.extend(res)
        else:
            results.append(res)
        return results

    except Exception as e:
        exc_traceback = sys.exc_info()[2]
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        logger.error(f"Erro _calc_real_report: {e} — {detalhes.filename}:{detalhes.lineno} ({detalhes.name})")
        return []

async def buildReports(clientId: int, r):
    try:
        logger.warning(f'Iniciando construção dos relatórios para o cliente {clientId}...')
        stmt = text("""
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
                    LEFT JOIN reports rp ON rp.client_id = j.client_id
                        AND rp.team_id = j.team_id
                        AND COALESCE(j.actual_start_date, j.plan_start_date) >= rp.report_date
                        AND COALESCE(j.actual_start_date, j.plan_start_date) < rp.report_date + interval '1 day'
                        AND rp.rebuild = 0
                WHERE js.internal_code_status = 'CONCLU'
                    AND j.client_id = :client_id
                    AND COALESCE(j.actual_start_date, j.plan_start_date) < date_trunc('day',NOW())
                    AND rp.report_date IS NULL
            ),
            q1 AS (
                select * from qjobs
                where null_distance = true AND null_geo = false
            )
            select
                client_id,
                team_id,
                DATE_TRUNC('day', actual_date) as actual_date,
                count(*) total
            from qjobs
            group by client_id, team_id, DATE_TRUNC('day', actual_date)
            order by client_id, DATE_TRUNC('day', actual_date), team_id
        """)
        async with SessionLocal() as db:
            result = await db.execute(stmt, {"client_id": clientId})
            rows = result.mappings().all()
            for row in rows:
                print(row)
                asyncio.create_task(logs(clientId=clientId, log='jobs', logJson=row))
                teamId = row.team_id
                actualDate = row.actual_date

                braa_results, brac_results, real_results = await asyncio.gather(
                    _calc_bra_report(clientId, teamId, actualDate, 'BRAA'),
                    _calc_bra_report(clientId, teamId, actualDate, 'BRAC'),
                    _calc_real_report(clientId, teamId, actualDate),
                )

                if not braa_results and not brac_results and not real_results:
                    continue

                output = {
                    "reports": [
                        {"type": "REAL", "resources": real_results},
                        {"type": "BRAC", "resources": brac_results},
                        {"type": "BRAA", "resources": braa_results},
                    ]
                }
                rep_json = json.dumps(output)
                stmt_merge = text("""
                    MERGE INTO reports u
                    USING (SELECT CAST(:json_data AS jsonb) as rep) as t
                    ON (client_id = :client_id and team_id = :team_id and report_date = :report_date)
                    WHEN MATCHED THEN
                        UPDATE SET
                            report = t.rep,
                            rebuild = 0,
                            modified_by = 'system',
                            modified_date = NOW()
                    WHEN NOT MATCHED THEN
                        INSERT (client_id, team_id, report_date, report, created_by, created_date, modified_by, modified_date)
                        VALUES (:client_id, :team_id, :report_date, t.rep, 'system', now(), 'system', now())
                """)
                await db.execute(stmt_merge, {
                    "client_id": clientId,
                    "team_id": teamId,
                    "report_date": actualDate,
                    "json_data": rep_json,
                })
                await db.commit()
                logger.warning('Reports finalizados!')
        await r.delete(f'buildReports:{clientId}')

    except Exception as e:
        exc_traceback = sys.exc_info()[2]
        detalhes = traceback.extract_tb(exc_traceback)[-1]
        logger.error(f"Erro {e}")
        logger.error(f"Detalhes: {detalhes.filename}, {detalhes.lineno} , {detalhes.name}")

async def processo_em_background(r):
    SLEEP_NORMAL = 10
    SLEEP_MAX = 300
    consecutive_failures = 0

    try:
        while True:
            try:
                await checkPendencias(r)
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

async def scheduled_process(r):
    SLEEP_NORMAL = 10
    SLEEP_MAX = 600
    consecutive_failures = 0
    try:
        while True:
            
            try:
                await checkPendencias(r)
                client_ids = []
                async with SessionLocal() as db:
                    clients = await db.execute(text("SELECT client_id FROM clients"))
                    client_ids = [row[0] for row in clients.fetchall()]

                for clientId in client_ids:
                    async with SessionLocal() as db:
                        stmt = text("""
                            select schedule_id, team_id, user_id, frequency
                                from schedules
                                where client_id = :client_id
                                and status = 'A'
                                and next_schedule_date = date_trunc('minutes', now())
                                    """)
                        result = await db.execute(stmt, {"client_id": clientId})
                    rows = result.mappings().all()
                    for row in rows:
                        scheduleId = row.schedule_id
                        teamId = row.team_id
                        userId = row.user_id
                        frequency = row.frequency
                        logger.info(f"Disparando cálculo de roteirização para Schedule ID: {scheduleId} | Team ID: {teamId} | Client ID: {clientId} | User ID: {userId}")
                        asyncio.create_task(calc_rote(clientId, teamId, userId, scheduleId, frequency),name=f'schedule:{scheduleId}:{teamId}')
                        async with SessionLocal() as db:
                            stmt = text("""
                              UPDATE schedules
                                SET next_schedule_date = next_schedule_date +   CASE 
                                                                                    WHEN frequency = 'DAILY' THEN interval '1 day' 
                                                                                    WHEN frequency = 'WEEKLY' THEN interval '1 week' 
                                                                                    WHEN frequency = 'MONTHLY' THEN interval '1 month' 
                                                                                    ELSE interval '1 day' 
                                                                                END
                                where client_id = :client_id
                                and schedule_id = :schedule_id
                            """)
                            await db.execute(stmt, {"client_id": clientId, "schedule_id": scheduleId})
                            await db.commit()
                        
                        logger.info(f"Schedule ID: {row['schedule_id']} | Next Schedule Date: {row['next_schedule_date']} | Client ID: {clientId}")

                for clientId in client_ids:
                    while True:
                        tasks = asyncio.all_tasks()
                        nomes_ativos = [task.get_name() for task in tasks]
                        if f'buildReports:{clientId}' in nomes_ativos:
                            logger.warning(f"buildReports já está em execução para client_id {clientId}. Aguardando para iniciar nova execução...")
                            break
                        
                        # await r.set(f'buildReports:{clientId}', 'Y')
                        logger.info(f"Iniciando buildReports para client_id {clientId}...")
                        asyncio.create_task(buildReports(clientId=clientId, r=r),name=f'buildReports:{clientId}')

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

    await r.set(f'semaforo:{settings.client_uid}', json.dumps({"calc_distance": "N", "check_pendencias": "N"}))

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
        
        stmt=text("""
            insert into user_team (client_id, user_id, team_id, created_by, created_date, modified_by, modified_date)
            select t.client_id, u.user_id,  t.team_id , u.user_name, now(), u.user_name, now() from users u
            join teams t on t.client_id = u.client_id
            where client_team_id = '48898'
            and  user_name = 'admin'
            and not exists( select 1 from user_team b
                            where b.client_id = t.client_id
                                and b.user_id = u.user_id);
        """)
        await db.execute(stmt)
        await db.commit()

    try:
        await asyncio.gather(
            # processo_em_background(r),
            scheduled_process(r)
            # adicione outros workers aqui, ex: outro_processo(r),
        )
    finally:
        await r.aclose()
        logger.info("Sync worker encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
