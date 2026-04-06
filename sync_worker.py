import asyncio
import redis.asyncio as Redis
import redis_client
import json
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.future import select
from database import engine, Base, SessionLocal
from config import settings
import models
from auth import get_password_hash
from tools import (
    getJobStatusMatrix, getJobTypeMatrix, getPlaceMatrix, getResourceWindowMatrix,
    getStyleMetrix, getResourcesMatrix, getTeamMatrix, getJobsMatrix, getGeoPosMatrix,
    getTeamMemberMatrix, getLogInOutMatrix, getAdressMatrix
)
from loguru import logger


def oneHour(dateTime: str):
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
                              geocode_lat_from text,
                              geocode_long_from text,
                              geocode_lat_at text,
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
                              OR u.geocode_long_at IS DISTINCT FROM t.geocode_long_at
                              OR u.description IS DISTINCT FROM t.name
                              )  THEN
                      UPDATE SET actual_geocode_lat = t.geocode_lat
                          ,actual_geocode_long = t.geocode_long
                          ,description = t.name
                          ,modified_by = 'INTEGRATION'
                          ,modified_date = t.modified_dttm
                  WHEN NOT MATCHED THEN
                      INSERT (client_id,client_resource_id, description, actual_geocode_lat, actual_geocode_long, geocode_lat_from, geocode_long_from, geocode_lat_at, geocode_long_at, modified_date_geo, modified_date_login, created_by, created_date, modified_by, modified_date)
                      VALUES (:client_id, t.person_id, t.name, t.geocode_lat, t.geocode_long, t.geocode_lat_from, t.geocode_long_from, t.geocode_lat_at, t.geocode_long_at, NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days', 'INTEGRATION', NOW(), 'INTEGRATION', t.modified_dttm)
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
                          OR u.actual_work_duration IS DISTINCT FROM t.work_duration::INTEGER
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
                          ) THEN
                  UPDATE SET team_id = t.team_idd
                      ,resource_id = t.resource_id
                      ,address_id = t.address_idd
                      ,place_id = t.place_idd
                      ,job_type_id = t.job_type_id
                      ,job_status_id = t.job_status_id
                      ,actual_work_duration = t.work_duration::INTEGER
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
                      ,modified_date = t.modified_dttm
              WHEN NOT MATCHED THEN
                  INSERT (client_id, client_job_id, team_id, resource_id, address_id, place_id, job_type_id, job_status_id, actual_work_duration, plan_start_date, plan_end_date, actual_start_date, actual_end_date, time_limit_end, time_limit_start, time_service, pp_resource_id, pp_start_date, pp_end_date, pt_job_id, pt_start_date, pt_end_date, pt_geocode_lat, pt_geocode_long, created_by, created_date, modified_by, modified_date)
                  VALUES (:client_id, t.client_job_id, t.team_idd, t.resource_id, t.address_idd, t.place_idd, t.job_type_id, t.job_status_id, t.work_duration::INTEGER, t.plan_start_dttm, t.plan_end_dttm, t.actual_start_dttm, t.actual_end_dttm, t.sla, t.plan_start_dttm, t.plan_task_dur_min, t.pp_person_id, t.pp_actual_start_dttm, t.pp_actual_end_dttm, t.pt_task_id, t.pt_actual_start_dttm, t.pt_actual_end_dttm, t.pt_geocode_lat, t.pt_geocode_long, 'INTEGRATION', t.created_dttm, 'INTEGRATION', t.modified_dttm)
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
        await processo_em_background(r)
    finally:
        await r.aclose()
        logger.info("Sync worker encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
