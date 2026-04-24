"""
Testes para a API de integração - endpoints /token e /jobs.
Execute: python test_integration_jobs.py
Requer: pip install httpx
"""

import sys
import json
import httpx
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

# ─── Credenciais de teste ─────────────────────────────────────────────────────
# Substitua pelos valores reais de uma credencial ativa no banco
CLIENT_ID     = "e226eb56-da6a-4f16-af12-cf02c8ad7fa2"
CLIENT_SECRET = "da57626b-15fa-445a-9528-d3a698579b16"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def ok(label: str):
    print(f"  [OK] {label}")

def fail(label: str, detail: str = ""):
    print(f"  [FAIL] {label}" + (f": {detail}" if detail else ""))

def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def get_token(client_id: str = CLIENT_ID, client_secret: str = CLIENT_SECRET) -> str | None:
    resp = httpx.post(
        f"{BASE_URL}/token",
        data={
            "client_id":     client_id,
            "client_secret": client_secret,
            "grant_type":    "client_credentials",
        },
    )
    if resp.status_code == 200:
        return resp.json()["access_token"]
    return None


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ─── Payload de exemplo ───────────────────────────────────────────────────────

NOW = datetime.now()

def make_job(job_id: str = "JOB-001", **overrides) -> dict:
    base = {
        "job_id":                       job_id,
        "team_id":                      "TEAM-01",
        "desc_team":                    "Equipe de Teste",
        "team_geocode_lat":             "-23.5505",
        "team_geocode_long":            "-46.6333",
        "team_modified_date":           NOW.isoformat(),
        "resource_id":                  "REC-001",
        "resource_name":                "Técnico João",
        "resource_actual_geocode_lat":  "-23.5505",
        "resource_actual_geocode_long": "-46.6333",
        "resource_geocode_lat_from":    "-23.5505",
        "resource_geocode_long_from":   "-46.6333",
        "resource_geocode_lat_at":      "-23.5600",
        "resource_geocode_long_at":     "-46.6400",
        "resource_status_id":           1,
        "resource_desc_status":         1,
        "resource_off_shift_flag":      0,
        "resource_modified_data":       NOW.isoformat(),
        "job_status_id":                "STATUS-ABERTO",
        "desc_job_status":              "Aberto",
        "style_id":                     None,
        "status_modified_date":         NOW.isoformat(),
        "job_type_id":                  "TYPE-INSTALACAO",
        "desc_job_type":                "Instalação",
        "type_modified_date":           NOW.isoformat(),
        "place_id":                     "PLACE-001",
        "trade_name":                   "Loja Teste",
        "cnpj":                         "00.000.000/0001-00",
        "place_modified_date":          NOW.isoformat(),
        "address_id":                   "ADDR-001",
        "address":                      "Rua Exemplo, 123 - Bairro Teste",
        "city":                         "São Paulo",
        "state":                        "SP",
        "zip_code":                     "01310-100",
        "geocode_lat":                  "-23.5600",
        "geocode_long":                 "-46.6400",
        "address_modified_date":        NOW.isoformat(),
        "plan_start_date":              NOW.isoformat(),
        "plan_end_date":                (NOW + timedelta(hours=2)).isoformat(),
        "actual_start_date":            None,
        "actual_end_date":              None,
        "real_time_service":            None,
        "plan_time_service":            7200,
        "limit_start_date":             None,
        "limit_end_date":               None,
        "priority":                     1,
        "time_setup":                   300,
        "time_overlap":                 0,
        "distance":                     5000,
        "time_distance":                900,
        "complements":                  {"observacao": "Levar equipamento X"},
    }
    base.update(overrides)
    return base


# ─── Testes do /token ─────────────────────────────────────────────────────────

def test_token_valido():
    section("POST /token — credenciais válidas")
    resp = httpx.post(
        f"{BASE_URL}/token",
        data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type":    "client_credentials",
        },
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  token_type : {data.get('token_type')}")
        print(f"  expires_in : {data.get('expires_in')}s")
        print(f"  access_token (primeiros 40 chars): {data.get('access_token', '')[:40]}…")
        ok("token gerado com sucesso")
        return data["access_token"]
    else:
        fail("falha ao obter token", resp.text)
        return None


def test_token_credencial_invalida():
    section("POST /token — credenciais inválidas")
    resp = httpx.post(
        f"{BASE_URL}/token",
        data={
            "client_id":     "id-invalido",
            "client_secret": "segredo-errado",
            "grant_type":    "client_credentials",
        },
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 401:
        ok("retornou 401 para credenciais inválidas")
    else:
        fail("esperado 401", resp.text)


def test_token_grant_type_errado():
    section("POST /token — grant_type inválido")
    resp = httpx.post(
        f"{BASE_URL}/token",
        data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type":    "password",
        },
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 400:
        ok("retornou 400 para grant_type inválido")
    else:
        fail("esperado 400", resp.text)


# ─── Testes do POST /jobs ─────────────────────────────────────────────────────

def test_jobs_sem_token():
    section("POST /jobs — sem Authorization")
    resp = httpx.post(f"{BASE_URL}/jobs", json=[make_job()])
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 401:
        ok("retornou 401 sem token")
    else:
        fail("esperado 401", resp.text)


def test_jobs_token_invalido():
    section("POST /jobs — token inválido")
    resp = httpx.post(
        f"{BASE_URL}/jobs",
        json=[make_job()],
        headers={"Authorization": "Bearer token.invalido.aqui"},
    )
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 401:
        ok("retornou 401 para token inválido")
    else:
        fail("esperado 401", resp.text)


def test_jobs_lista_vazia(token: str):
    section("POST /jobs — lista vazia")
    resp = httpx.post(f"{BASE_URL}/jobs", json=[], headers=auth_headers(token))
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        assert data["processed"] == 0, "processed deve ser 0"
        assert data["inserted"]  == 0, "inserted deve ser 0"
        assert data["updated"]   == 0, "updated deve ser 0"
        ok("lista vazia retornou batch zerado")
    else:
        fail("esperado 200", resp.text)


def test_jobs_um_job_valido(token: str):
    section("POST /jobs — 1 job válido")
    payload = [make_job("JOB-TEST-001")]
    resp = httpx.post(f"{BASE_URL}/jobs", json=payload, headers=auth_headers(token))
    print(f"  Status: {resp.status_code}")
    print(f"  Resposta: {json.dumps(resp.json(), indent=2, ensure_ascii=False)}")
    if resp.status_code == 200:
        data = resp.json()
        ok(f"processados={data['processed']} inserted={data['inserted']} updated={data['updated']} erros={len(data['errors'])}")
    else:
        fail("esperado 200", resp.text)


def test_jobs_multiplos(token: str):
    section("POST /jobs — múltiplos jobs em lote")
    payload = [make_job(f"JOB-BATCH-{i:03d}") for i in range(1, 4)]
    resp = httpx.post(f"{BASE_URL}/jobs", json=payload, headers=auth_headers(token))
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Resposta: {json.dumps(data, indent=2, ensure_ascii=False)}")
        ok(f"lote com {len(payload)} jobs — processados={data['processed']}")
    else:
        fail("esperado 200", resp.text)


def test_jobs_campo_obrigatorio_ausente(token: str):
    section("POST /jobs — campo obrigatório ausente (plan_start_date)")
    job = make_job("JOB-MISSING-DATE")
    del job["plan_start_date"]
    resp = httpx.post(f"{BASE_URL}/jobs", json=[job], headers=auth_headers(token))
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 422:
        ok("retornou 422 para campo obrigatório ausente")
    else:
        print(f"  Resposta: {resp.text[:300]}")
        fail("esperado 422")


def test_jobs_body_nao_lista(token: str):
    section("POST /jobs — body não é lista (objeto único)")
    resp = httpx.post(f"{BASE_URL}/jobs", json=make_job("JOB-OBJ"), headers=auth_headers(token))
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 422:
        ok("retornou 422 para body que não é lista")
    else:
        fail("esperado 422", resp.text[:200])


# ─── Runner ───────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Testes da Integration API — /token e /jobs")
    print("="*60)
    print(f"  Base URL : {BASE_URL}")
    print(f"  client_id: {CLIENT_ID}")

    # /token
    test_token_credencial_invalida()
    test_token_grant_type_errado()
    token = test_token_valido()

    if not token:
        print("\n[ABORT] Não foi possível obter token — verifique CLIENT_ID/CLIENT_SECRET e se a API está rodando.")
        sys.exit(1)

    # /jobs
    test_jobs_sem_token()
    test_jobs_token_invalido()
    test_jobs_lista_vazia(token)
    test_jobs_campo_obrigatorio_ausente(token)
    test_jobs_body_nao_lista(token)
    test_jobs_um_job_valido(token)
    test_jobs_multiplos(token)

    print("\n" + "="*60)
    print("  Testes concluídos.")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
