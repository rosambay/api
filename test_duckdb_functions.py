"""
Testa _build_vroom_payload e _process_vroom_result com dados mock.
Executar: python test_duckdb_functions.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import date, datetime, timedelta
from schedulejobs_duckdb import _build_vroom_payload, _process_vroom_result

SIM_DATE = date(2025, 4, 17)

RAW = {
    "jobs": [
        # sem time_limit_end — usa aw.end_time ou 86399
        {"job_id": 1, "address_id": 10, "team_id": 1, "job_type_id": 1, "job_status_id": 1,
         "place_id": 1, "time_overlap": None, "time_setup": None, "time_service": None,
         "priority": 2, "actual_start_date": None, "plan_start_date": None,
         "time_limit_end": None, "client_job_id": "CJ001"},
        # mesmo endereço que job 1 — testa qtd_mesmo_end
        {"job_id": 2, "address_id": 11, "team_id": 1, "job_type_id": 1, "job_status_id": 1,
         "place_id": 2, "time_overlap": None, "time_setup": None, "time_service": None,
         "priority": 1, "actual_start_date": None, "plan_start_date": None,
         "time_limit_end": None, "client_job_id": "CJ002"},
        {"job_id": 3, "address_id": 11, "team_id": 1, "job_type_id": 1, "job_status_id": 1,
         "place_id": 2, "time_overlap": None, "time_setup": None, "time_service": None,
         "priority": 1, "actual_start_date": None, "plan_start_date": None,
         "time_limit_end": None, "client_job_id": "CJ003"},
        # time_limit_end NO mesmo dia — deve usar epoch (54000 = 15h)
        {"job_id": 4, "address_id": 12, "team_id": 1, "job_type_id": 1, "job_status_id": 1,
         "place_id": 1, "time_overlap": None, "time_setup": None, "time_service": None,
         "priority": 1, "actual_start_date": None, "plan_start_date": None,
         "time_limit_end": datetime(2025, 4, 17, 15, 0, 0), "client_job_id": "CJ004"},
        # time_limit_end de OUTRO dia — deve cair no fallback (86399), nunca negativo
        {"job_id": 5, "address_id": 13, "team_id": 1, "job_type_id": 1, "job_status_id": 1,
         "place_id": 1, "time_overlap": None, "time_setup": None, "time_service": None,
         "priority": 1, "actual_start_date": None, "plan_start_date": None,
         "time_limit_end": datetime(2025, 4, 16, 15, 0, 0), "client_job_id": "CJ005"},
    ],
    "simulation": [
        {"simulation_id": 100, "team_id": 1, "simulation_date": SIM_DATE},
    ],
    "job_types": [
        {"job_type_id": 1, "description": "Visita", "time_overlap": 0,
         "time_setup": 300, "time_service": 1800, "priority": 0},
    ],
    "job_status": [
        {"job_status_id": 1, "description": "Pendente"},
    ],
    "address": [
        {"address_id": 10, "geocode_lat": -23.550, "geocode_long": -46.633,
         "address": "Rua A", "city": "SP", "state_prov": "SP", "zippost": "01310-000"},
        {"address_id": 11, "geocode_lat": -23.555, "geocode_long": -46.640,
         "address": "Rua B", "city": "SP", "state_prov": "SP", "zippost": "01320-000"},
        {"address_id": 12, "geocode_lat": -23.560, "geocode_long": -46.645,
         "address": "Rua C", "city": "SP", "state_prov": "SP", "zippost": "01330-000"},
        {"address_id": 13, "geocode_lat": -23.565, "geocode_long": -46.650,
         "address": "Rua D", "city": "SP", "state_prov": "SP", "zippost": "01340-000"},
    ],
    "teams": [
        {"team_id": 1, "time_overlap": 0, "time_setup": 600, "time_service": 3600,
         "start_time": 28800, "end_time": 64800},
    ],
    "aw": [],   # sem janelas de endereço — testa tabela vazia
    "resources": [
        {"resource_id": 5, "description": "Motorista A", "client_resource_id": "R001",
         "geocode_lat_from": -23.548, "geocode_long_from": -46.630,
         "geocode_lat_at": -23.548,  "geocode_long_at": -46.630,
         "fl_off_shift": 0},
    ],
    "team_members": [
        {"resource_id": 5, "team_id": 1},
    ],
    "rw": [],   # sem janelas de recurso — testa tabela vazia
    "places": [
        {"place_id": 1, "trade_name": "Empresa A", "cnpj": "00.000.000/0001-00"},
        {"place_id": 2, "trade_name": "Empresa B", "cnpj": "11.111.111/0001-11"},
    ],
}

VROOM_MOCK = {
    "routes": [
        {
            "vehicle": 5,
            "steps": [
                {"type": "start",  "location": [-46.630, -23.548], "arrival": 28800, "distance": 0,    "time_distance": 0,   "service": 0, "setup": 0},
                {"type": "job",    "location": [-46.633, -23.550], "arrival": 29100, "distance": 1200, "time_distance": 180, "service": 1800, "setup": 300, "id": 1},
                {"type": "job",    "location": [-46.640, -23.555], "arrival": 31500, "distance": 800,  "time_distance": 120, "service": 1800, "setup": 300, "id": 2},
                {"type": "end",    "location": [-46.630, -23.548], "arrival": 34000, "distance": 1500, "time_distance": 200, "service": 0, "setup": 0},
            ]
        }
    ],
    "unassigned": []
}


def test_build_payload():
    print("\n=== TEST: _build_vroom_payload ===")
    payload = _build_vroom_payload(RAW, SIM_DATE)
    assert "_job_ids" in payload, "falta _job_ids"
    assert len(payload["jobs"]) > 0, "jobs vazio"
    assert len(payload["vehicles"]) > 0, "vehicles vazio"

    jobs_by_id = {j["id"]: j for j in payload["jobs"]}

    # sem nulls e sem negativos em nenhum campo do payload
    for j in payload["jobs"]:
        tw = j["time_windows"][0]
        assert j["setup"]    is not None,  f"setup null no job {j['id']}"
        assert j["service"]  is not None,  f"service null no job {j['id']}"
        assert j["priority"] is not None,  f"priority null no job {j['id']}"
        assert None not in tw,             f"time_window null no job {j['id']}"
        assert None not in j["location"],  f"location null no job {j['id']}"
        assert tw[0] >= 0, f"start_time negativo ({tw[0]}) no job {j['id']}"
        assert tw[1] >= 0, f"end_time negativo ({tw[1]}) no job {j['id']}"

    # job 4: time_limit_end no mesmo dia às 15h → end_time deve ser 54000
    if 4 in jobs_by_id:
        tw4 = jobs_by_id[4]["time_windows"][0]
        assert tw4[1] == 54000, f"job 4 end_time esperado 54000, got {tw4[1]}"
        print(f"  job 4 (time_limit_end mesmo dia): end_time={tw4[1]} OK")

    # job 5: time_limit_end de outro dia → deve cair no fallback 86399
    if 5 in jobs_by_id:
        tw5 = jobs_by_id[5]["time_windows"][0]
        assert tw5[1] == 86399, f"job 5 end_time esperado 86399 (fallback), got {tw5[1]}"
        print(f"  job 5 (time_limit_end outro dia): end_time={tw5[1]} OK (fallback)")

    for v in payload["vehicles"]:
        assert None not in v["time_window"], f"time_window null no vehicle {v['id']}"
        assert None not in v["start"],       f"start null no vehicle {v['id']}"
        assert None not in v["end"],         f"end null no vehicle {v['id']}"
        assert v["time_window"][0] >= 0,     f"vehicle start_time negativo"
        assert v["time_window"][1] >= 0,     f"vehicle end_time negativo"

    print(f"  jobs={len(payload['jobs'])}  vehicles={len(payload['vehicles'])}")
    print(f"  job_ids={payload['_job_ids']}")
    print(f"  primeiro job: {payload['jobs'][0]}")
    print(f"  primeiro vehicle: {payload['vehicles'][0]}")
    print("  OK")
    return payload


def test_process_result(job_ids_for_result):
    print("\n=== TEST: _process_vroom_result ===")
    raw_filtered = dict(RAW)
    raw_filtered["jobs"] = [j for j in RAW["jobs"] if j["job_id"] in set(job_ids_for_result)]
    resultado = _process_vroom_result(VROOM_MOCK, raw_filtered, SIM_DATE)
    assert len(resultado) > 0, "resultado vazio"
    r = resultado[0]
    assert "resource_id" in r
    assert "jobs" in r
    assert len(r["jobs"]) > 0
    print(f"  resources={len(resultado)}")
    print(f"  jobs no resource 5: {len(r['jobs'])}")
    print(f"  total_distance={r['total_distance']}")
    print(f"  primeiro job: {r['jobs'][0]['client_job_id']}")
    print("  OK")


if __name__ == "__main__":
    try:
        payload = test_build_payload()
        test_process_result(payload["_job_ids"])
        print("\n[OK] Todos os testes passaram.")
    except Exception as e:
        import traceback
        print(f"\n[ERRO]: {e}")
        traceback.print_exc()
        sys.exit(1)
