import asyncio
import traceback
import sys
from loguru import logger
from datetime import datetime, timedelta
from sqlalchemy import text
import json
import httpx
from client_tools import (
    getJobStatusMatrix, getJobTypeMatrix, getPlaceMatrix, getPriorityMatrix, getResourceWindowMatrix,
    getStyleMetrix, getResourcesMatrix, getTeamMatrix, getJobsMatrix, getGeoPosMatrix,
    getTeamMemberMatrix, getLogInOutMatrix, getAdressMatrix
)
CLIENT_ID     = "e226eb56-da6a-4f16-af12-cf02c8ad7fa2"
CLIENT_SECRET = "da57626b-15fa-445a-9528-d3a698579b16"
BASE_URL = "http://localhost:8000"

def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

async def get_token(client_id: str = CLIENT_ID, client_secret: str = CLIENT_SECRET) -> str | None:
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

async def getSnaps(token: str):
    resp = httpx.get(f"{BASE_URL}/snaps", timeout=None, headers=auth_headers(token))
    
    return json.loads(resp.json())

async def getStyle(token: str, snaps: dict):

    logger.warning("[getStyle] Entrou atualização de estilos...")

    
    dateTime = snaps['styles']
    result_rows = await getStyleMetrix(dateTime)

    if result_rows:
        try:
            print(result_rows)
            resp = httpx.post(f"{BASE_URL}/styles", timeout=None, json=result_rows, headers=auth_headers(token))
            print(f"  Status: {resp.status_code}")
            print(f"  Resposta: {resp.json()}")

        except Exception as e:
            logger.error(f"[getStyle] Erro ao processar : {e}")
    return

async def getJobs(token: str, snaps: dict):

    logger.warning("[getJobs] Entrou atualização das Tarefas ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['jobs']
    result_rows = await getJobsMatrix(dateTime)

    if result_rows:
    #   last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
      try:
          print(result_rows)
          resp = httpx.post(f"{BASE_URL}/jobs", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
                logger.error(f"[getJobs] Erro ao processar : {e}")
    return

async def getResources(token: str, snaps: dict):

    logger.warning("[getResources] Entrou atualização dos Recursos ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['resources']
    result_rows = await getResourcesMatrix(dateTime)

    if result_rows:
      try:
          print(result_rows)
          resp = httpx.post(f"{BASE_URL}/resources", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getResources] Erro ao processar : {e}")
    return

async def getResourceWindows(token: str, snaps: dict):

    logger.warning("[getResourceWindows] Entrou atualização das janelas dos Recursos ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['resource_windows']
    result_rows = await getResourceWindowMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/resource_windows", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getResourceWindows] Erro ao processar : {e}")
    return

async def getAddress(token: str, snaps: dict):

    logger.warning("[getAddress] Entrou atualização de Endereços ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['address']
    result_rows = await getAdressMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/address", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getAddress] Erro ao processar : {e}")
    return

async def getPlaces(token: str, snaps: dict):

    logger.warning("[getPlaces] Entrou atualização dos locais ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['places']
    result_rows = await getPlaceMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/places", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getPlaces] Erro ao processar : {e}")
    return


async def getTeams(token: str, snaps: dict):

    logger.warning("[getTeams] Entrou atualização das Equipes ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['teams']
    result_rows = await getTeamMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/teams", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getTeams] Erro ao processar : {e}")
    return

async def getTeamMembers(token: str, snaps: dict):

    logger.warning("[getTeamMembers] Entrou atualização dos membros das equipes ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['team_members']
    result_rows = await getTeamMemberMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/team_members", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getTeamMembers] Erro ao processar : {e}")
    return

async def getJobTypes(token: str, snaps: dict):

    logger.warning("[getJobTypes] Entrou atualização dos Tipos de trabalho/Tarefa ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['job_types']
    result_rows = await getJobTypeMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/job_types", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getJobTypes] Erro ao processar : {e}")
    return

async def getJobStatus(token: str, snaps: dict):

    logger.warning("[getJobStatus] Entrou atualização da situaçao/status do trabalho/Tarefa ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['job_status']
    result_rows = await getJobStatusMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/job_status", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getJobStatus] Erro ao processar : {e}")
    return

async def getLogInOut(token: str, snaps: dict):

    logger.warning("[getLogInOut] Entrou atualização Login...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['logintime']
    result_rows = await getLogInOutMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/resource_logged", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getLogInOut] Erro ao processar : {e}")
    return

async def getActualGeoPos(token: str, snaps: dict):

    logger.warning("[getActualGeoPos] Entrou atualização Geo Resource posicionamento...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['actual_geopos']
    result_rows = await getGeoPosMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/resource_actual_geopos", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getActualGeoPos] Erro ao processar : {e}")
    return

async def getPriority(token: str, snaps: dict):

    logger.warning("[getPriority] Entrou atualização das Prioridades...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['priority']
    result_rows = await getPriorityMatrix(dateTime)

    if result_rows:
      try:
        #   print(result_rows)
          resp = httpx.post(f"{BASE_URL}/priority", timeout=None, json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
        logger.error(f"[getPriority] Erro ao processar : {e}")
    return

async def background_process():
    SLEEP_NORMAL = 10
    SLEEP_MAX = 300
    consecutive_failures = 0

    try:
        while True:
            try:
                token = await get_token()
                snaps = await getSnaps(token)
                print(snaps, type(snaps))
                await getPriority(token, snaps)
                # await getStyle(token, snaps)
                # await asyncio.sleep(0.5)
                # await getResources(token, snaps)
                # await asyncio.sleep(0.5)
                # await getLogInOut(token, snaps)
                await asyncio.sleep(0.5)
                await getActualGeoPos(token, snaps)
                # await asyncio.sleep(0.5)
                # await getResourceWindows(token,snaps)
                # await asyncio.sleep(0.5)
                # await getJobs(token, snaps)
                # await asyncio.sleep(0.5)
                # await getAddress(token,snaps)
                # await asyncio.sleep(0.5)
                # await getPlaces(token,snaps)

                # await asyncio.sleep(0.5)
                # await getTeams(token,snaps)

                # await asyncio.sleep(0.5)
                # await getTeamMembers(token,snaps)

                # await asyncio.sleep(0.5)
                # await getJobTypes(token,snaps)

                # await asyncio.sleep(0.5)
                # await getJobStatus(token,snaps)
                if consecutive_failures > 0:
                    logger.info(f"Background job recuperada após {consecutive_failures} falha(s) consecutiva(s).")
                consecutive_failures = 0
                sleep_time = SLEEP_NORMAL

            except asyncio.CancelledError:
                raise
            except Exception as e:
        
                exc_type, exc_value, exc_traceback = sys.exc_info()
            
                # Extrai a última linha da pilha (onde o erro ocorreu)
                detalhes = traceback.extract_tb(exc_traceback)[0]
                
                arquivo = detalhes.filename
                linha = detalhes.lineno
                funcao = detalhes.name
                logger.error(f"Erro {e}")        
                logger.error(f"Detalhes: {arquivo}, {linha} , {funcao}")      
                consecutive_failures += 1
                sleep_time = min(SLEEP_NORMAL * (2 ** consecutive_failures), SLEEP_MAX)
                logger.error(f"Erro na iteração do background job (falha #{consecutive_failures}): {e}. Próxima tentativa em {sleep_time}s.")

            logger.warning(f'Aguardando {sleep_time}s para próximo refresh ...')
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Processo contínuo foi encerrado.")

async def main():
    
    try:
        await asyncio.gather(
            background_process(),
            # scheduled_process(r)
            # adicione outros workers aqui, ex: outro_processo(r),
        )
    finally:

        logger.info("Sync Client worker encerrado.")

if __name__ == "__main__":
    asyncio.run(main())
