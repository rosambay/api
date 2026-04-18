"""
/schedulejobs — versão DuckDB para comparativo de performance.

Diferenças em relação ao main.py:
  1. Fetches separados por tabela (queries simples, sem JOINs no Postgres)
  2. Toda transformação (JOINs, window functions, COALESCE) feita em DuckDB in-process
  3. Resultado do VROOM processado em DuckDB — sem JSON embutido em SQL
  4. perResource executado em paralelo com asyncio.gather()
  5. OSRM calls dentro de cada recurso também em paralelo
  6. Um único UPDATE ao final para todos os recursos
  7. client_id hardcoded removido (bug do original corrigido)
"""

import asyncio
import time
import json
import duckdb
from datetime import datetime, date
from typing import List, Optional, Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from loguru import logger

import database
import schemas
from services import optimize_routes_vroom, get_route_distance_block, logs
from deps import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _epoch_time(t) -> int:
    """Converte time/timedelta para segundos desde meia-noite."""
    if t is None:
        return None
    if hasattr(t, "hour"):
        return t.hour * 3600 + t.minute * 60 + t.second
    if hasattr(t, "seconds"):
        return int(t.total_seconds())
    return int(t)


def _rows_to_dicts(result) -> list[dict]:
    return [dict(r) for r in result.mappings().all()]


# ---------------------------------------------------------------------------
# fetch: uma query simples por tabela — sem JOINs, sem window functions
# ---------------------------------------------------------------------------

async def _fetch_raw(db: AsyncSession, client_id: int, simulation_id: int,
                     job_ids: list[int], resource_ids: list[int]) -> dict:
    """
    Executa as queries sequencialmente na mesma sessão.
    AsyncSession não suporta múltiplas queries concorrentes na mesma conexão.
    """
    jobs_ids_pg = f"ARRAY[{','.join(str(i) for i in job_ids)}]"
    res_ids_pg  = f"ARRAY[{','.join(str(i) for i in resource_ids)}]"

    jobs_r  = await db.execute(text(f"""
        SELECT job_id, address_id, team_id, job_type_id, job_status_id, place_id,
               time_overlap, time_setup, time_service, priority,
               actual_start_date, plan_start_date, time_limit_end, client_job_id
        FROM jobs
        WHERE client_id = :c AND job_id = ANY({jobs_ids_pg})
    """).bindparams(c=client_id))

    sim_r   = await db.execute(text("""
        SELECT simulation_id, team_id, simulation_date
        FROM simulation
        WHERE client_id = :c AND simulation_id = :sid
    """).bindparams(c=client_id, sid=simulation_id))

    jt_r    = await db.execute(text("""
        SELECT job_type_id, description, time_overlap, time_setup, time_service, priority
        FROM job_types WHERE client_id = :c
    """).bindparams(c=client_id))

    js_r    = await db.execute(text("""
        SELECT job_status_id, description
        FROM job_status WHERE client_id = :c
    """).bindparams(c=client_id))

    addr_r  = await db.execute(text("""
        SELECT address_id, geocode_lat::NUMERIC AS geocode_lat,
               geocode_long::NUMERIC AS geocode_long,
               address, city, state_prov, zippost
        FROM address WHERE client_id = :c
    """).bindparams(c=client_id))

    teams_r = await db.execute(text("""
        SELECT team_id, time_overlap, time_setup, time_service,
               EXTRACT(EPOCH FROM start_time)::INT AS start_time,
               EXTRACT(EPOCH FROM end_time)::INT   AS end_time
        FROM teams WHERE client_id = :c
    """).bindparams(c=client_id))

    aw_r    = await db.execute(text("""
        SELECT address_id, week_day,
               EXTRACT(EPOCH FROM start_time)::INT AS start_time,
               EXTRACT(EPOCH FROM end_time)::INT   AS end_time
        FROM address_windows WHERE client_id = :c
    """).bindparams(c=client_id))

    res_r   = await db.execute(text(f"""
        SELECT resource_id, description, client_resource_id,
               geocode_lat_from::NUMERIC, geocode_long_from::NUMERIC,
               geocode_lat_at::NUMERIC,   geocode_long_at::NUMERIC,
               fl_off_shift
        FROM resources
        WHERE client_id = :c AND resource_id = ANY({res_ids_pg})
    """).bindparams(c=client_id))

    tm_r    = await db.execute(text(f"""
        SELECT resource_id, team_id
        FROM team_members
        WHERE client_id = :c AND resource_id = ANY({res_ids_pg})
    """).bindparams(c=client_id))

    rw_r    = await db.execute(text(f"""
        SELECT resource_id, week_day,
               EXTRACT(EPOCH FROM start_time)::INT AS start_time,
               EXTRACT(EPOCH FROM end_time)::INT   AS end_time
        FROM resource_windows
        WHERE client_id = :c AND resource_id = ANY({res_ids_pg})
    """).bindparams(c=client_id))

    places_r = await db.execute(text("""
        SELECT place_id, trade_name, cnpj
        FROM places WHERE client_id = :c
    """).bindparams(c=client_id))

    return {
        "jobs":         _rows_to_dicts(jobs_r),
        "simulation":   _rows_to_dicts(sim_r),
        "job_types":    _rows_to_dicts(jt_r),
        "job_status":   _rows_to_dicts(js_r),
        "address":      _rows_to_dicts(addr_r),
        "teams":        _rows_to_dicts(teams_r),
        "aw":           _rows_to_dicts(aw_r),
        "resources":    _rows_to_dicts(res_r),
        "team_members": _rows_to_dicts(tm_r),
        "rw":           _rows_to_dicts(rw_r),
        "places":       _rows_to_dicts(places_r),
    }


# ---------------------------------------------------------------------------
# DuckDB: monta payload do VROOM (equivale à Query 1 do original)
# ---------------------------------------------------------------------------

_EMPTY_TABLE_DDL = {
    "aw":  "CREATE TABLE aw  (address_id  INTEGER, week_day INTEGER, start_time INTEGER, end_time INTEGER)",
    "rw":  "CREATE TABLE rw  (resource_id INTEGER, week_day INTEGER, start_time INTEGER, end_time INTEGER)",
    "jobs":          "CREATE TABLE jobs          (job_id INTEGER, address_id INTEGER, team_id INTEGER, job_type_id INTEGER, job_status_id INTEGER, place_id INTEGER, time_overlap INTEGER, time_setup INTEGER, time_service INTEGER, priority INTEGER, actual_start_date TIMESTAMP, plan_start_date TIMESTAMP, time_limit_end TIMESTAMP, client_job_id VARCHAR)",
    "job_types":     "CREATE TABLE job_types     (job_type_id INTEGER, description VARCHAR, time_overlap INTEGER, time_setup INTEGER, time_service INTEGER, priority INTEGER)",
    "job_status":    "CREATE TABLE job_status    (job_status_id INTEGER, description VARCHAR)",
    "address":       "CREATE TABLE address       (address_id INTEGER, geocode_lat DOUBLE, geocode_long DOUBLE, address VARCHAR, city VARCHAR, state_prov VARCHAR, zippost VARCHAR)",
    "teams":         "CREATE TABLE teams         (team_id INTEGER, time_overlap INTEGER, time_setup INTEGER, time_service INTEGER, start_time INTEGER, end_time INTEGER)",
    "resources":     "CREATE TABLE resources     (resource_id INTEGER, description VARCHAR, client_resource_id VARCHAR, geocode_lat_from DOUBLE, geocode_long_from DOUBLE, geocode_lat_at DOUBLE, geocode_long_at DOUBLE, fl_off_shift INTEGER)",
    "team_members":  "CREATE TABLE team_members  (resource_id INTEGER, team_id INTEGER)",
    "simulation":    "CREATE TABLE simulation    (simulation_id INTEGER, team_id INTEGER, simulation_date DATE)",
    "places":        "CREATE TABLE places        (place_id INTEGER, trade_name VARCHAR, cnpj VARCHAR)",
}


def _register_tables(con: duckdb.DuckDBPyConnection, raw: dict) -> None:
    import pyarrow as pa
    for name, rows in raw.items():
        if rows:
            con.register(name, pa.Table.from_pylist(rows))
        elif name in _EMPTY_TABLE_DDL:
            con.execute(_EMPTY_TABLE_DDL[name])
        else:
            con.execute(f"CREATE TABLE IF NOT EXISTS {name} (dummy INTEGER)")


def _build_vroom_payload(raw: dict, simulation_date: date) -> dict:
    con = duckdb.connect()

    sim_dow = simulation_date.isoweekday() % 7 + 1  # 1=Dom ... 7=Sab

    _register_tables(con, raw)

    # Replica exatamente a lógica das CTEs dados + MapeamentoEnderecos + q1
    jobs_payload = con.execute(f"""
        WITH dados AS (
            SELECT
                j.job_id,
                j.team_id,
                a.geocode_lat::DOUBLE  AS geocode_lat,
                a.geocode_long::DOUBLE AS geocode_long,
                COALESCE(j.time_overlap,  jt.time_overlap,  t.time_overlap,  0) AS time_overlap,
                COALESCE(j.time_setup,    jt.time_setup,    t.time_setup,    0)  AS time_setup,
                COALESCE(j.time_service,  jt.time_service,  t.time_service,  0)  AS time_service,
                COALESCE(j.priority, 0) + COALESCE(jt.priority, 0) / 100         AS priority,
                COALESCE(aw.start_time, 0)                                        AS start_time,
                CASE
                    WHEN j.time_limit_end IS NOT NULL
                         AND DATE_TRUNC('day', j.time_limit_end::TIMESTAMP) = '{simulation_date}'::DATE
                    THEN EPOCH(j.time_limit_end::TIMESTAMP - '{simulation_date}'::TIMESTAMP)::INT
                    ELSE COALESCE(aw.end_time, 86399)
                END AS end_time
            FROM jobs j
            JOIN job_types  jt ON jt.job_type_id = j.job_type_id
            JOIN address     a ON a.address_id   = j.address_id
            JOIN team_members tm ON tm.resource_id IS NOT NULL AND tm.team_id = j.team_id
            JOIN teams        t ON t.team_id      = j.team_id
            LEFT JOIN aw       ON aw.address_id   = j.address_id
                               AND aw.week_day    = {sim_dow}
        ),
        mapeamento AS (
            SELECT *,
                COUNT(job_id) OVER (PARTITION BY team_id, geocode_lat, geocode_long) AS qtd_mesmo_end,
                ROW_NUMBER()  OVER (PARTITION BY team_id, geocode_lat, geocode_long ORDER BY job_id) AS ordem
            FROM dados
        ),
        q1 AS (
            SELECT job_id, geocode_lat, geocode_long, time_setup, time_overlap, priority,
                   start_time, end_time,
                   time_service + (time_overlap * (qtd_mesmo_end - 1)) AS time_service
            FROM mapeamento WHERE ordem = 1
        )
        SELECT
            job_id,
            geocode_long,
            geocode_lat,
            time_setup,
            time_service,
            priority,
            start_time,
            end_time
        FROM q1
    """).fetchall()

    vehicles_payload = con.execute(f"""
        WITH vd AS (
            SELECT
                r.resource_id,
                r.description,
                r.geocode_lat_from::DOUBLE  AS geocode_lat_from,
                r.geocode_long_from::DOUBLE AS geocode_long_from,
                r.geocode_lat_at::DOUBLE    AS geocode_lat_at,
                r.geocode_long_at::DOUBLE   AS geocode_long_at,
                r.fl_off_shift,
                COALESCE(rw.start_time, t.start_time) AS rw_start,
                COALESCE(
                    CASE WHEN r.fl_off_shift = 0 THEN rw.end_time ELSE 86399 END,
                    t.end_time
                ) AS rw_end,
                t.start_time AS t_start
            FROM resources r
            JOIN team_members tm ON tm.resource_id = r.resource_id
            JOIN teams         t  ON t.team_id      = tm.team_id
            LEFT JOIN rw          ON rw.resource_id = r.resource_id
                                  AND rw.week_day   = {sim_dow}
        )
        SELECT resource_id, description,
               geocode_lat_from, geocode_long_from,
               geocode_lat_at,   geocode_long_at,
               rw_start, rw_end
        FROM vd
    """).fetchall()

    jobs_list = [
        {
            "id": r[0],
            "location": [float(r[1] or 0), float(r[2] or 0)],
            "setup":    int(r[3] or 0),
            "service":  int(r[4] or 0),
            "priority": int(r[5] or 0),
            "time_windows": [[int(r[6] or 0), int(r[7] or 86399)]]
        }
        for r in jobs_payload
    ]

    vehicles_list = [
        {
            "id": r[0],
            "description": r[1],
            "start": [float(r[3] or 0), float(r[2] or 0)],
            "end":   [float(r[5] or 0), float(r[4] or 0)],
            "time_window": [int(r[6] or 0), int(r[7] or 86399)]
        }
        for r in vehicles_payload
    ]

    job_ids = [r[0] for r in jobs_payload]

    con.close()
    return {"vehicles": vehicles_list, "jobs": jobs_list, "_job_ids": job_ids}


# ---------------------------------------------------------------------------
# DuckDB: processa resultado do VROOM + enriquece com dados do banco
# (equivale à Query 2 do original — sem JSON embutido no SQL)
# ---------------------------------------------------------------------------

def _process_vroom_result(vroom_retorno: dict, raw: dict, simulation_date: date) -> list[dict]:
    con = duckdb.connect()
    _register_tables(con, raw)

    routes = vroom_retorno.get("routes", [])

    # Desmonta as rotas do VROOM em linhas planas
    steps_rows = []
    for route in routes:
        resource_id = route["vehicle"]
        for seq, step in enumerate(route["steps"]):
            steps_rows.append({
                "resource_id":  resource_id,
                "sequence":     seq,
                "stop_type":    step["type"],
                "job_id":       step.get("id"),
                "geocode_long": step["location"][0] if step.get("location") else None,
                "geocode_lat":  step["location"][1] if step.get("location") else None,
                "arrival":      step.get("arrival", 0),
                "service":      step.get("service", 0),
                "setup":        step.get("setup", 0),
                "distance":     step.get("distance", 0),
                "time_distance":step.get("time_distance", 0),
            })

    if not steps_rows:
        return []

    import pyarrow as pa
    con.register("vroom_steps", pa.Table.from_pylist(steps_rows))

    result = con.execute(f"""
        WITH dados_jobs AS (
            SELECT
                j.job_id,
                j.team_id,
                j.client_job_id,
                j.place_id,
                j.address_id,
                j.job_type_id,
                j.job_status_id,
                a.geocode_lat::DOUBLE  AS geocode_lat,
                a.geocode_long::DOUBLE AS geocode_long,
                a.address, a.city, a.state_prov, a.zippost,
                p.trade_name, p.cnpj,
                js.description  AS status_description,
                jt.description  AS type_description,
                COALESCE(j.time_overlap, jt.time_overlap, t.time_overlap, 0) AS time_overlap,
                COALESCE(j.time_service, jt.time_service, t.time_service)    AS time_service,
                FIRST_VALUE(j.job_id) OVER (
                    PARTITION BY j.team_id, a.geocode_lat, a.geocode_long
                    ORDER BY j.job_id
                ) AS new_job_id,
                ROW_NUMBER() OVER (
                    PARTITION BY j.team_id, a.geocode_lat, a.geocode_long
                    ORDER BY j.job_id
                ) AS ordem_job
            FROM jobs j
            JOIN address    a  ON a.address_id    = j.address_id
            JOIN job_types  jt ON jt.job_type_id  = j.job_type_id
            JOIN job_status js ON js.job_status_id = j.job_status_id
            JOIN teams      t  ON t.team_id        = j.team_id
            JOIN places     p  ON p.place_id       = j.place_id
        ),
        q_start AS (
            SELECT resource_id, geocode_long, geocode_lat, arrival AS arrival_from
            FROM vroom_steps WHERE stop_type = 'start'
        ),
        q_end AS (
            SELECT resource_id, geocode_long, geocode_lat, arrival AS arrival_at,
                   distance AS distance_at, time_distance AS time_distance_at
            FROM vroom_steps WHERE stop_type = 'end'
        ),
        q_first_job AS (
            SELECT resource_id, distance AS distance_from, time_distance AS time_distance_from
            FROM vroom_steps WHERE stop_type = 'job' AND sequence = 2
        ),
        dados AS (
            SELECT
                res.resource_id,
                res.description            AS resource_name,
                res.client_resource_id,
                j.team_id,
                j.client_job_id,
                j.status_description,
                j.type_description,
                j.address, j.city, j.state_prov, j.zippost,
                j.trade_name, j.cnpj,
                j.job_id,
                j.time_overlap,
                j.time_service             AS service,
                j.ordem_job,
                q.geocode_long, q.geocode_lat,
                q.setup,
                qs.geocode_long            AS geocode_long_from,
                qs.geocode_lat             AS geocode_lat_from,
                qs.arrival_from,
                qf.distance_from,
                qf.time_distance_from,
                qe.geocode_long            AS geocode_long_at,
                qe.geocode_lat             AS geocode_lat_at,
                qe.arrival_at,
                qe.distance_at,
                qe.time_distance_at,
                CASE
                    WHEN j.ordem_job > 1 THEN q.arrival + (j.time_overlap * (j.ordem_job - 1))
                    ELSE q.arrival
                END AS arrival,
                CASE WHEN j.ordem_job > 1 THEN 0 ELSE q.distance     END AS distance,
                CASE WHEN j.ordem_job > 1 THEN 0 ELSE q.time_distance END AS time_distance,
                DATE '{simulation_date}' AS job_day
            FROM vroom_steps q
            JOIN dados_jobs j  ON j.new_job_id = q.job_id
            JOIN resources res ON res.resource_id = q.resource_id
            JOIN q_start qs    ON qs.resource_id  = q.resource_id
            JOIN q_end   qe    ON qe.resource_id  = q.resource_id
            JOIN q_first_job qf ON qf.resource_id = q.resource_id
            WHERE q.stop_type = 'job'
        )
        SELECT
            resource_id,
            client_resource_id,
            resource_name,
            team_id,
            job_day,
            geocode_long_from, geocode_lat_from, arrival_from, distance_from, time_distance_from,
            geocode_long_at,   geocode_lat_at,   arrival_at,   distance_at,   time_distance_at,
            job_id, client_job_id, status_description, type_description,
            address, city, state_prov, zippost, trade_name, cnpj,
            geocode_long, geocode_lat,
            arrival, service, distance, time_distance, setup, time_overlap, ordem_job
        FROM dados
        ORDER BY resource_id, arrival
    """).fetchall()

    columns = [
        "resource_id", "client_resource_id", "resource_name", "team_id", "job_day",
        "geocode_long_from", "geocode_lat_from", "arrival_from", "distance_from", "time_distance_from",
        "geocode_long_at",   "geocode_lat_at",   "arrival_at",   "distance_at",   "time_distance_at",
        "job_id", "client_job_id", "status_description", "type_description",
        "address", "city", "state_prov", "zippost", "trade_name", "cnpj",
        "geocode_long", "geocode_lat",
        "arrival", "service", "distance", "time_distance", "setup", "time_overlap", "ordem_job"
    ]

    rows = [dict(zip(columns, r)) for r in result]
    con.close()

    # Agrupa por resource_id
    grouped: dict[int, dict] = {}
    sim_date = simulation_date

    for row in rows:
        rid = row["resource_id"]
        if rid not in grouped:
            grouped[rid] = {
                "client_id":          1,
                "resource_id":        rid,
                "client_resource_id": row["client_resource_id"],
                "resource_name":      row["resource_name"],
                "job_day":            sim_date.isoformat(),
                "geocode_long_from":  row["geocode_long_from"],
                "geocode_lat_from":   row["geocode_lat_from"],
                "arrival_from":       row["arrival_from"],
                "distance_from":      row["distance_from"],
                "time_distance_from": row["time_distance_from"],
                "geocode_long_at":    row["geocode_long_at"],
                "geocode_lat_at":     row["geocode_lat_at"],
                "arrival_at":         row["arrival_at"],
                "distance_at":        row["distance_at"],
                "time_distance_at":   row["time_distance_at"],
                "total_distance":     0,
                "total_time_distance":0,
                "total_jobs":         0,
                "jobs":               [],
            }

        arrival_dt  = datetime.combine(sim_date, datetime.min.time()) + \
                      __import__("datetime").timedelta(seconds=int(row["arrival"]))
        end_dt      = datetime.combine(sim_date, datetime.min.time()) + \
                      __import__("datetime").timedelta(seconds=int(row["arrival"]) + int(row["setup"] or 0) + int(row["service"] or 0))

        grouped[rid]["jobs"].append({
            "job_id":             row["job_id"],
            "client_job_id":      row["client_job_id"],
            "team_id":            row["team_id"],
            "status_description": row["status_description"],
            "type_description":   row["type_description"],
            "address":            row["address"],
            "city":               row["city"],
            "state_prov":         row["state_prov"],
            "zippost":            row["zippost"],
            "trade_name":         row["trade_name"],
            "cnpj":               row["cnpj"],
            "job_day":            sim_date.isoformat(),
            "geocode_lang":       row["geocode_long"],
            "geocode_lat":        row["geocode_lat"],
            "arrival":            row["arrival"],
            "service":            row["service"],
            "distance":           row["distance"],
            "time_distance":      row["time_distance"],
            "time_overlap":       row["time_overlap"],
            "start_date":         arrival_dt.isoformat(),
            "end_date":           end_dt.isoformat(),
        })
        grouped[rid]["total_distance"]      += row["distance"] or 0
        grouped[rid]["total_time_distance"] += row["time_distance"] or 0
        grouped[rid]["total_jobs"]          += 1

    for g in grouped.values():
        g["total_distance"] += g["distance_at"] or 0
        g["total_time_distance"] += g["time_distance_at"] or 0

    return list(grouped.values())


# ---------------------------------------------------------------------------
# calc_rote: unidade de trabalho por conjunto de resource+jobs
# retorna (resource_ids, resultado) — sem gravar no banco
# ---------------------------------------------------------------------------

async def _calc_rote_duckdb(
    client_id: int,
    simulation_id: int,
    resource_ids: list[int],
    job_ids: list[int],
) -> tuple[list[int], list[dict] | None]:
    """
    Cria sua própria sessão de banco — seguro para uso em asyncio.gather().
    Cada task paralela usa uma conexão independente do pool.
    """
    t0 = time.perf_counter()

    # 1. Sessão própria — não compartilhada com outras tasks
    async with database.SessionLocal() as db:
        raw = await _fetch_raw(db, client_id, simulation_id, job_ids, resource_ids)

    sim_rows = raw["simulation"]
    if not sim_rows:
        return resource_ids, None
    simulation_date = sim_rows[0]["simulation_date"]
    if hasattr(simulation_date, "date"):
        simulation_date = simulation_date.date()

    logger.info(f"[duckdb] fetch raw: {time.perf_counter()-t0:.3f}s")

    # 2. Monta payload VROOM em DuckDB (thread executor para não bloquear event loop)
    loop = asyncio.get_event_loop()
    payload = await loop.run_in_executor(None, _build_vroom_payload, raw, simulation_date)

    logger.info(f"[duckdb] build payload: {time.perf_counter()-t0:.3f}s | jobs={len(payload['jobs'])} vehicles={len(payload['vehicles'])}")

    if not payload["jobs"] or not payload["vehicles"]:
        return resource_ids, None

    job_ids_for_result = payload.pop("_job_ids")

    await logs(clientId=client_id, log="duckdb: dados para vroom", logJson=payload)

    # 3. VROOM
    retorno = await optimize_routes_vroom(payload)
    logger.info(f"[duckdb] vroom: {time.perf_counter()-t0:.3f}s")

    # 4. OSRM calls em paralelo para todas as rotas
    routes = retorno.get("routes", [])
    if routes:
        geo_lists = []
        for rou in routes:
            geo = [[s["location"][0], s["location"][1]] for s in rou["steps"]]
            geo_lists.append(geo)

        osrm_results = await asyncio.gather(
            *[get_route_distance_block(geo) for geo in geo_lists]
        )
        logger.info(f"[duckdb] osrm (paralelo): {time.perf_counter()-t0:.3f}s")

        for rou, geo_result in zip(routes, osrm_results):
            steps = rou["steps"]
            steps[0]["time_distance"] = 0
            steps[0]["distance"] = 0
            for ln, leg in enumerate(geo_result, start=1):
                steps[ln]["time_distance"] = leg["duration"]
                steps[ln]["distance"]      = leg["distance"]

    listIds = [item["id"] for item in retorno.get("unassigned", [])]
    if listIds:
        logger.debug(f"[duckdb] unassigned: {listIds}")

    await logs(clientId=client_id, log="duckdb: retorno vroom", logJson=retorno)

    # 5. Processa resultado do VROOM em DuckDB
    raw["jobs"] = [j for j in raw["jobs"] if j["job_id"] in set(job_ids_for_result)]
    resultado = await loop.run_in_executor(
        None, _process_vroom_result, retorno, raw, simulation_date
    )
    logger.info(f"[duckdb] process result: {time.perf_counter()-t0:.3f}s | resources={len(resultado)}")

    return resource_ids, resultado


# ---------------------------------------------------------------------------
# persistência: único UPDATE mesclando todos os resultados
# ---------------------------------------------------------------------------

async def _save_simulation(
    db: AsyncSession,
    client_id: int,
    simulation_id: int,
    all_results: list[dict],
    all_resource_ids: list[int],
) -> Any:
    merged_json = json.dumps(all_results)
    merged_res  = f"ARRAY[{','.join(str(i) for i in all_resource_ids)}]"

    stmt = text(f"""
        UPDATE simulation
        SET json_dado =
            COALESCE((
                SELECT jsonb_agg(elem)
                FROM jsonb_array_elements(json_dado) AS elem
                WHERE (elem->>'resource_id')::INT NOT IN (
                    SELECT unnest({merged_res})
                )
            ), '[]'::jsonb)
            || CAST(:json_data AS jsonb)
            ,modified_by   = 'system'
            ,modified_date = NOW()
        WHERE client_id   = :client_id
          AND simulation_id = :simulation_id
        RETURNING json_dado
    """)

    result = await db.execute(stmt, {
        "client_id":     client_id,
        "simulation_id": simulation_id,
        "json_data":     merged_json,
    })
    await db.commit()
    for row in result:
        return row.json_dado
    return None


# ---------------------------------------------------------------------------
# endpoint
# ---------------------------------------------------------------------------

@router.post("/schedulejobs-duckdb")
async def scheduleJobs_duckdb(
    body: schemas.ScheduleJobsRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(database.get_db),
):
    t_total = time.perf_counter()

    resources    = body.resources
    simulationId = body.simulation_id
    perResource  = body.per_resource or None
    listJobs     = body.jobs
    action       = body.action
    clientId     = current_user["clientId"]

    # ---- action C: limpa tudo ----
    if action == "C":
        stmt = text("""
            UPDATE simulation
               SET json_dado = '[]'::jsonb
             WHERE client_id = :c AND simulation_id = :sid
             RETURNING json_dado
        """).bindparams(c=clientId, sid=simulationId)
        result = await db.execute(stmt)
        await db.commit()
        for row in result:
            return row.json_dado
        return None

    # ---- action D: remove recurso ----
    if action == "D" and len(listJobs) == 0 and len(resources) == 1:
        res_list = ",".join(str(r) for r in resources)
        stmt = text(f"""
            UPDATE simulation
               SET json_dado = (
                   SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                   FROM jsonb_array_elements(json_dado) AS elem
                   WHERE (elem->>'resource_id')::INT NOT IN ({res_list})
               )
             WHERE client_id = :c AND simulation_id = :sid
             RETURNING json_dado
        """).bindparams(c=clientId, sid=simulationId)
        result = await db.execute(stmt)
        await db.commit()
        for row in result:
            return row.json_dado
        return None

    # ---- lógica principal ----
    logger.info(f"[duckdb] iniciando | action={action} resources={resources} jobs={len(listJobs or [])}")

    if perResource:
        # Cada task recebe sua própria sessão internamente — seguro para gather
        tasks = [
            _calc_rote_duckdb(
                clientId, simulationId,
                [recurso["resource_id"]],
                [j["job_id"] for j in recurso["jobs"]],
            )
            for recurso in perResource["resources"]
        ]

        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        all_results      = []
        all_resource_ids = []

        for item in task_results:
            if isinstance(item, Exception):
                logger.error(f"[duckdb] erro numa task paralela: {item}")
                continue
            res_ids, resultado = item
            if resultado:
                all_results.extend(resultado)
                all_resource_ids.extend(res_ids)

    else:
        res_ids, resultado = await _calc_rote_duckdb(
            clientId, simulationId, resources, listJobs
        )
        all_results      = resultado or []
        all_resource_ids = res_ids

    if not all_results:
        logger.warning(f"[duckdb] sem resultados para salvar")
        return None

    # Um único UPDATE para todos os recursos
    final = await _save_simulation(db, clientId, simulationId, all_results, all_resource_ids)

    logger.info(f"[duckdb] concluído em {time.perf_counter()-t_total:.3f}s")
    return final
