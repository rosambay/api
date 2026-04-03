import asyncio
import redis.asyncio as Redis
import database
import redis_client
import json
import models, schemas
from auth import get_password_hash, verify_password, create_access_token
from database import engine, Base, SessionLocal, get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Column, text 
from sqlalchemy.orm import aliased
from sqlalchemy.future import select
from fastapi import FastAPI, Depends, HTTPException, status,  Response, Request, Query, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import  List, Optional
from jose import JWTError, jwt
# from notification_service import criar_e_enviar_notificacao
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from config import settings
from tools import  getJobStatusMatrix, getJobTypeMatrix, getPlaceMatrix, getResourceWindowMatrix, getStyleMetrix, getResourcesMatrix, getTeamMatrix ,getUserSession, getJobsMatrix, getGeoPosMatrix, setUserSession, getTeamMemberMatrix,getLogInOutMatrix, getAdressMatrix
from loguru import logger

async def cleanSessionsRedis(r: Redis,session_web_id: str):
    # await r.delete(f"session:{session_web_id}")
    # await r.delete(f"filter:{session_web_id}")
    # await dropUserSession(session_web_id)
    return True

def oneHour(dateTime:str):
    data_obj = datetime.strptime(dateTime, '%Y-%m-%d %H:%M:%S')
    nova_data_obj = data_obj - timedelta(hours=1)
    return nova_data_obj.strftime('%Y-%m-%d %H:%M:%S')

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
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_resource_id = t.person_id
                  WHEN MATCHED AND (  
                                 u.res_geocode_lat IS DISTINCT FROM t.geocode_lat 
                              OR u.res_geocode_long IS DISTINCT FROM t.geocode_long 
                              OR u.description IS DISTINCT FROM t.name 
                              )  THEN
                      UPDATE SET res_geocode_lat = t.geocode_lat 
                          ,res_geocode_long = t.geocode_long 
                          ,actual_geocode_lat = t.geocode_lat
                          ,actual_geocode_long = t.geocode_long
                          ,description = t.name
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm 
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_resource_id, description, res_geocode_lat, res_geocode_long, actual_geocode_lat, actual_geocode_long, modified_date_geo, modified_date_login, created_by, created_date, modified_by, modified_date) 
                      VALUES (:client_id, t.person_id, t.name, t.geocode_lat, t.geocode_long, t.geocode_lat, t.geocode_long, NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days', 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
                  RETURNING 
                       u.resource_id, merge_action(), to_jsonb(u) AS registro_json
              """)
              
              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
                  logger.info(f"ID: {row.resource_id} | Ação GEO realizada: {row.merge_action}")
              
              await r.set(snapKey, last_snap)  # Atualiza o snap_time a cada execução
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
                  # await criar_e_enviar_notificacao(r, type='LOGIN', titulo="Login do usuário", dados_extra=row.registro_json)
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
                              modified_dttm TIMESTAMP,
                              last_snap TIMESTAMP)) AS t
                  ON u.client_id = :client_id AND u.client_team_id = t.team_id
                  WHEN MATCHED AND (  
                              u.team_name IS DISTINCT FROM t.description 
                              )  THEN
                      UPDATE SET team_name = t.description
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm 
                  RETURNING 
                       u.team_id, merge_action(), to_jsonb(u) AS registro_json
              """)
              
              async with SessionLocal() as db:
                result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
                await db.commit()
                for row in result:
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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

    result_rows = await getTeamMemberMatrix( dateTime, settings.client_uid)
    print(result_rows)
    if result_rows:
          last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
          try:
              jsonResults = json.dumps(result_rows)

              # stmt = text(f"""
              #     WITH q1 AS (
              #     SELECT * FROM jsonb_to_recordset(:dados_json) 
              #               AS x(
              #                 modified_by text, 
              #                 team_id text, 
              #                 person_id text, 
              #                 last_snap TIMESTAMP))
              #     DELETE FROM team_members u
              #       USING q1
              #       WHERE NOT EXISTS (select 1 FROM q1
              #                           join teams t on t.client_team_id = q1.team_id
              #                           join resources r on r.client_resource_id = q1.person_id
              #                          WHERE u.team_id = t.team_id 
              #                           AND  u.resource_id = r.resource_id)
              #     RETURNING 
              #         to_jsonb(u) AS registro_json, u.uid
              # """)
          
              # async with SessionLocal() as db:
              #   result = await db.execute(stmt, {"dados_json": jsonResults})
              #   await db.commit()
              #   for row in result:
              #       logger.info(f"UID: {row.uid} | Ação Team Member realizada: DELETE")

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
        tempDateTime = (datetime.now() - timedelta(days=7))
        dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    else:
      dateTime = oneHour(dateTime)
    
    result_rows = await getJobsMatrix(dateTime, settings.client_uid)

    if result_rows:
      last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
      first_snap = result_rows[0]['first_snap'][:19].replace('T', ' ')
      try:
          jsonResults = json.dumps(result_rows)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
                  # await criar_e_enviar_notificacao(r, type='GEO', titulo="Geo Localização", dados_extra=row.registro_json)
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
              setSnap = True
              logger.info(f"ID: {row.address_id} | Ação ADDRESS realizada: {row.merge_action}")

          if setSnap:
            snapKey = f'snapAddressTime:{settings.client_uid}'
            await r.set(snapKey, first_snap)

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
                      js.job_status_id
                      FROM jsonb_to_recordset(:dados_json) 
                        AS x( request_id text,
                              task_id text,
                              team_id text,
                              person_id text,
                              address_id text,
                              place_id text,
                              task_type text,
                              task_status text,
                              created_dttm TIMESTAMP,
                              modified_dttm TIMESTAMP,
                              plan_start_dttm TIMESTAMP,
                              plan_end_dttm TIMESTAMP,
                              actual_start_dttm TIMESTAMP,
                              actual_end_dttm TIMESTAMP,
                              sla TIMESTAMP,
                              plan_task_dur_min integer,
                              last_snap TIMESTAMP)
                      LEFT JOIN resources r ON x.person_id = r.client_resource_id AND :client_id = r.client_id
                      JOIN teams t ON t.client_team_id = x.team_id AND t.client_id = :client_id
                      join address a ON a.client_address_id = x.address_id AND a.client_id = :client_id 
                      JOIN job_types jt ON jt.client_job_type_id = x.task_type AND jt.client_id = :client_id
                      JOIN job_status js ON js.client_job_status_id = x.task_status AND js.client_id = :client_id
                      JOIN places p ON p.client_place_id = x.place_id AND p.client_id = :client_id
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
                          OR u.plan_start_date IS DISTINCT FROM t.plan_start_dttm 
                          OR u.plan_end_date IS DISTINCT FROM t.plan_end_dttm 
                          OR u.actual_start_date IS DISTINCT FROM t.actual_start_dttm 
                          OR u.actual_end_date IS DISTINCT FROM t.actual_end_dttm 
                          OR u.time_limit_end IS DISTINCT FROM t.sla 
                          OR u.time_limit_start IS DISTINCT FROM t.plan_start_dttm 
                          OR u.time_service IS DISTINCT FROM t.plan_task_dur_min 
                          ) THEN
                  UPDATE SET team_id = t.team_idd 
                      ,resource_id = t.resource_id 
                      ,address_id = t.address_idd 
                      ,place_id = t.place_idd
                      ,job_type_id = t.job_type_id 
                      ,job_status_id = t.job_status_id 
                      ,modified_date = t.modified_dttm 
                      ,plan_start_date = t.plan_start_dttm 
                      ,plan_end_date = t.plan_end_dttm 
                      ,actual_start_date = t.actual_start_dttm 
                      ,actual_end_date = t.actual_end_dttm 
                      ,time_limit_end = t.sla
                      ,time_limit_start = t.plan_start_dttm
                      ,time_service = t.plan_task_dur_min
              WHEN NOT MATCHED THEN
                  INSERT (client_id, client_job_id, team_id, resource_id, address_id, place_id, job_type_id, job_status_id, plan_start_date, plan_end_date, actual_start_date, actual_end_date, time_limit_end, time_limit_start, time_service, created_by, created_date, modified_by, modified_date) 
                  VALUES (:client_id, t.client_job_id, t.team_idd, t.resource_id, t.address_idd, t.place_idd, t.job_type_id, t.job_status_id, t.plan_start_dttm, t.plan_end_dttm, t.actual_start_dttm, t.actual_end_dttm, t.sla, t.plan_start_dttm, t.plan_task_dur_min, 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
              RETURNING 
                  to_jsonb(u) AS registro_json, merge_action(), u.job_id
          """)
          
          async with SessionLocal() as db:
            result = await db.execute(stmt, {"dados_json": jsonResults, "client_id": settings.client_uid})
            await db.commit()
            for row in result:
                type = row.merge_action + ' ON JOB'
                # await criar_e_enviar_notificacao(r, type=type, titulo="Chamado Alterado", dados_extra=row.registro_json)
                logger.info(f"ID: {row.job_id} | Ação JOB realizada: {row.merge_action}")
          
          await r.set(snapKey, last_snap)
      except Exception as e:
                logger.error(f"[getJobs] Erro ao processar postgres: {e}")
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
                await getTeamMember(r)
                await asyncio.sleep(0.2)
                await getJobs(r)
                await asyncio.sleep(0.2)
                await getTeam(r)
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

@asynccontextmanager
async def lifespan(app: FastAPI):

    async with engine.begin() as conn:
        
        # await conn.run_sync(Base.metadata.drop_all)
        # logger.info("Tabelas do banco dropadas com sucesso!")

        # O run_sync é necessário porque o create_all é síncrono por natureza
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Tabelas do banco verificadas/criadas com sucesso!")

    async with SessionLocal() as db:
        rClient = await db.execute(select(models.Clients).where(models.Clients.domain == "Admin"))
        clientDb = rClient.scalars().first()
        if not clientDb:
            print("Criando cliente ...")
            
            newClient = models.Clients(
                name = "Administration",
                domain = "Admin",
                created_by = "admin",
                created_date = datetime.now(),
                modified_by = "admin",
                modified_date = datetime.now()
            )
            db.add(newClient)
            await db.commit()

        rUser = await db.execute(select(models.Users).where(models.Users.user_name == "admin"))
        userDb = rUser.scalars().first()
        
        if not userDb:
            print("Criando usuário 'admin'...")
            
            newUser = models.Users(
                client_id = 1,
                user_name = "admin",
                name = "Administrator",
                passwd = get_password_hash("123456"),
                super_user = 1,
                created_by = "admin",
                created_date = datetime.now(),
                modified_by = "admin",
                modified_date = datetime.now()
            )
            db.add(newUser)
            await db.commit()

    r = redis_client.get_redis()
    # await r.flushall()
    # logger.info("Cache Redis limpo com sucesso!")
    job = asyncio.create_task(processo_em_background(r))
    yield
        
    job.cancel()
    

app = FastAPI(lifespan=lifespan)

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

# ROTAS DE AUTENTICAÇÃO

@app.post("/login", response_model=schemas.Token)
async def login(form_data: schemas.LoginRequest, request: Request, r: Redis = Depends(redis_client.get_redis), db: AsyncSession = Depends(get_db)):
    
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
    
    rDomains = await db.execute(select(models.Clients).where(models.Clients.domain == domain))
    clientDb = rDomains.scalars().first()

    if not clientDb:
        raise HTTPException(status_code=401, detail="Credenciais inválidas para o domínio especificado")
    
    clientId = clientDb.client_id
    clientUid = clientDb.uid
    clientDomain = clientDb.domain

    rUsers = await db.execute(select(models.Users).where(models.Users.user_name == user, models.Users.client_id == clientId))
    userDb = rUsers.scalars().first()

    if not userDb or not verify_password(pwd, userDb.passwd):
        raise HTTPException(status_code=401, detail="Credenciais inválidas para o domínio e usuário especificado")
    
    superUser = userDb.super_user

    dataToken = {
        "userId": userDb.user_id,
        "superUser": superUser,
        "clientId": clientId,
        "clientUid": str(clientUid),
        "clientDomain": clientDomain
    }
    access_token = create_access_token(data=dataToken)

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
            res_geocode_lat,
            res_geocode_long,
            actual_geocode_lat,
            actual_geocode_long,
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

        arvore = [dict(row) for row in rows]
        return arvore
        
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")

@app.get("/jobs")
async def getJobsResources(
    date: str = Query(..., description="Data no formato YYYY-MM-DD"),
    db: AsyncSession = Depends(database.get_db),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["userId"]
    client_id = current_user["clientId"]

    try:
        # Tenta executar uma query simples no banco
        smtp = text("""
          select 
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
                    (j.actual_start_date is null and j.plan_start_date >= cast(:date as date) and j.plan_start_date < cast(:date as date) + interval '1 day')
                   or (
                    j.actual_start_date is not null AND (j.actual_start_date >= cast(:date as date) and j.actual_start_date < cast(:date as date) + interval '1 day')
                    )
                  )
            order by j.plan_start_date desc
        """).bindparams(client_id=client_id, user_id=user_id, date=date)
                            
        result = await db.execute(smtp)
        rows = result.mappings().all()

        res = [dict(row) for row in rows]
        return res
        
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")

@app.get("/tasks/")
async def read_tasks(
    start_date: Optional[str] = None, # YYYY-MM-DD
    end_date: Optional[str] = None,
    r: Redis = Depends(redis_client.get_redis), 
    session: str = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)):
  
    startDate = datetime.now().strftime('%Y-%m-%d')
    endDate = datetime.now().strftime('%Y-%m-%d')

    session_web_id, user_id = session

    filterKey = f"filter:{user_id}"
    if not start_date and not end_date:
        filter = await r.get(filterKey)
        if filter:
            start_date, end_date = filter.split(':')
        else:
          start_date, end_date = startDate, endDate
    else:    
       await r.setex(filterKey, settings.access_token_expire_minutes * 60 , f"{start_date}:{end_date}")

    print('Filtrando...: ',f"{start_date}:{end_date}")
    try:
        # Tenta executar uma query simples no banco
        smtp = text("""
           WITH q1 AS(
              select t.*,
                      CASE
                        WHEN (t.actual_start_dttm IS NOT NULL AND t.actual_start_dttm > t.plan_start_dttm)
                          then t.actual_start_dttm
                          else
                            t.plan_start_dttm
                      end start_date,
                      CASE
                        WHEN (t.actual_end_dttm IS NOT NULL AND t.actual_end_dttm > t.plan_end_dttm)
                          then t.actual_end_dttm
                          else
                            t.plan_end_dttm
                      end end_date,
                      p.user_name,
                      a.geocode_lat,
                      a.geocode_long,
                      a.address,
                      a.city,
                      a.state_prov,
                      a.zippost,
                      COALESCE(s.background,'#FFFFFF') AS  background,
                      COALESCE(s.foreground,'#000000') AS  foreground,
                      p.geocode_lat AS gcode_lat_sta,
                      p.geocode_long AS gcode_long_sta,
                      p.modified_dttm AS geocode_dttm,
                      p.logged_in,
                      p.logged_out,
                      p.name AS person_name
                from tasks t
                 join team_member tm on tm.team_id = t.sta_id
                 join person p ON t.person_id = p.person_id
                 join address a ON t.address_id = a.address_id
                 left join styles s ON t.item_style_id = s.item_style_id
                WHERE t.c_plan_start_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                  AND tm.person_id = :user_id
                  order by t.sta_id, t.person_id
            ),
            q2 as (
            select q1.*, 
                CASE 
                  WHEN (q1.sla is not null and q1.sla > q1.start_date) 
                    then 'Dentro do SLA'
                  ELSE
                   CASE
                    when q1.sla is null
                      then 'SLA não definido'
                    else 'Janela do SLA pode ter sido perdida.' 
                   end
                end as status_sla 
              from q1),
            tarefas_agrupadas AS (
                          SELECT 
                              t.sta_id, 
                              t.desc_sta,
                              t.person_id, 
                              t.person_name,
                              t.user_name,
                              t.c_plan_start_date,
                              t.gcode_lat_sta,
                              t.gcode_long_sta,
                              t.logged_in,
                              t.logged_out,
                              t.desc_turno,
                              t.geocode_dttm,
                              SUM(CASE WHEN t.task_status NOT IN ('CLOSED', 'NAO CONCLUIR', 'COMPLETED', 'CANCELED') THEN 1 ELSE 0 END) AS tot_pendente,
                              COUNT(*) AS total,
                              jsonb_agg(to_jsonb(t) ORDER BY t.start_date ASC) AS array_tarefas
                          FROM q2 t
                          GROUP BY t.desc_sta, t.sta_id,  t.person_id, t.c_plan_start_date, t.person_name, t.user_name, t.gcode_lat_sta,t.gcode_long_sta, t.geocode_dttm, t.logged_in, t.logged_out, t.desc_turno
                      ),
          tecnicos_agrupados AS (
              -- Passo 2: Agrupa os Técnicos com suas tarefas por Time (sta_id)
              SELECT 
                  sta_id,
                  desc_sta,
                  jsonb_agg(
                      jsonb_build_object(
                          'id', person_id||c_plan_start_date,
                          'id_tecnico', person_id,
                          'geocode_lat_sta', gcode_lat_sta,
                          'geocode_long_sta', gcode_long_sta,
                          'geocode_dttm', geocode_dttm,
                          'logged_in', logged_in,
                          'logged_out', logged_out,
                          'nome_tecnico', person_name, 
                          'user_name', user_name, 
                          'start_date', c_plan_start_date,
                          'turno', desc_turno,
                          'tot_pendente', tot_pendente,
                          'total', total,
                          'children', array_tarefas
                      ) 
                  ) AS array_tecnicos
              FROM tarefas_agrupadas
              GROUP BY desc_sta, sta_id
          ),

          mapa_tecnicos AS (
              -- Passo 3: Cria o dicionário JSON em memória
              SELECT jsonb_object_agg(sta_id, array_tecnicos) AS dicionario_json
              FROM tecnicos_agrupados
          ),

          -- ====================================================================
          -- NOVO PASSO A: Identifica os times nas tarefas que NÃO estão em sub_teams
          -- ====================================================================
          times_orfaos AS (
              SELECT 
                  ta.sta_id,
                  ta.desc_sta,
                  ta.array_tecnicos
              FROM tecnicos_agrupados ta
              LEFT JOIN sub_teams st ON ta.sta_id::TEXT = st.team_id::TEXT
              WHERE st.team_id IS NULL
              ORDER BY ta.desc_sta
          ),

          -- ====================================================================
          -- NOVO PASSO B: Converte os times órfãos em um array JSONB
          -- ====================================================================
          json_orfaos AS (
              SELECT COALESCE(
                  jsonb_agg(
                      jsonb_build_object(
                          'id', sta_id,
                          -- Adiciona um rótulo para ficar claro na interface
                          'desc_team', desc_sta, 
                          'children', array_tecnicos
                      )
                  ), 
                  '[]'::jsonb
              ) AS array_orfaos
              FROM times_orfaos
          )

          -- Passo 4: Junta a Árvore Original com o Array de Órfãos na raiz
          SELECT 
              montar_arvore_dinamica(NULL, (SELECT dicionario_json FROM mapa_tecnicos)) 
              || 
              (SELECT array_orfaos FROM json_orfaos) AS arvore_completa;
                    """).bindparams(start_date=start_date, end_date=end_date, user_id=user_id)
        result = await db.execute(smtp)
        for row in result:
          arvore = [{
              "id": '0',
              "allteams": "Todas as Equipes",
              "children": row.arvore_completa
          }]
          arvore_json = json.dumps(arvore)
          # return arvore_json
          return Response(content=arvore_json, media_type="application/json")
        
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        raise HTTPException(status_code=500, detail="Erro de conexão com o banco de dados")

    
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

# # --- Rota de Disparo (Backend/Sistema) ---
# @app.post("/notify")
# async def notify_endpoint(user_id: str = Query(...),r: Redis = Depends(redis_client.get_redis), db: AsyncSession = Depends(get_db)):
#     execTask = await db.execute(select(models.Tasks).where(models.Tasks.person_id == user_id))
#     resTask = execTask.scalars().first()
#     if resTask:
#       job = {
#               'task_id': resTask.task_id,
#               'sta_id': resTask.sta_id,
#               'person_id': resTask.person_id,
#               'task_status': resTask.task_status
#           }
#       r_uid = await criar_e_enviar_notificacao(r, type='UPDATE ON JOB', titulo="Chamado Alterado", dados_extra=job)

#     execPerson = await db.execute(select(models.Person).where(models.Person.person_id == user_id))
#     resPerson = execPerson.scalars().first()
#     if resPerson:
#         person = {
#             "person_id": resPerson.person_id, 
#             "geocode_lat": resPerson.geocode_lat,
#             "geocode_long": resPerson.geocode_long,
#             "logged_in": str(resPerson.logged_in),
#             "logged_out": str(resPerson.logged_out),
    
#         }

#         r_uid = await criar_e_enviar_notificacao(r, type='GEO', titulo="Chamado Alterado", dados_extra=person)
#         r_uid = await criar_e_enviar_notificacao(r, type='LOGIN', titulo="Chamado Alterado", dados_extra=person)
           
#     return {"status": "Enviado", "target": r_uid}