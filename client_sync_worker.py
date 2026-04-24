import asyncio
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

snaps = None
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
    resp = httpx.get(f"{BASE_URL}/snaps", headers=auth_headers(token))
    return json.loads(resp.json())

async def getStyle(token: str, snaps: str):

    logger.warning("[getStyle] Entrou atualização de estilos...")

    
    dateTime = snaps['styles']
    result_rows = await getStyleMetrix(dateTime)

    if result_rows:
        try:
            print(result_rows)
            resp = httpx.post(f"{BASE_URL}/styles", json=result_rows, headers=auth_headers(token))
            print(f"  Status: {resp.status_code}")
            print(f"  Resposta: {resp.json()}")

        except Exception as e:
            logger.error(f"[getStyle] Erro ao processar : {e}")
    return

async def getJobs(token: str, snaps: str):

    logger.warning("[getJobs] Entrou atualização das Tarefas ...")

    # tempDateTime = (datetime.now() - timedelta(days=5))
    # dateTime = tempDateTime.strftime('%Y-%m-%d ') + '00:00:00'
    dateTime = snaps['jobs']
    result_rows = await getJobsMatrix(dateTime)

    if result_rows:
    #   last_snap = result_rows[0]['last_snap'][:19].replace('T', ' ')
      try:
          print(result_rows)
          resp = httpx.post(f"{BASE_URL}/jobs", json=result_rows, headers=auth_headers(token))
          print(f"  Status: {resp.status_code}")
          print(f"  Resposta: {resp.json()}")

      except Exception as e:
                logger.error(f"[getJobs] Erro ao processar : {e}")
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
                print(snaps)
                await getStyle(token, snaps)
                await asyncio.sleep(0.5)

                # await getResources(r)
                # await asyncio.sleep(0.2)
                # await getResourceWindow(r)
                # await asyncio.sleep(0.2)
                # await getGeoPos(r)
                # await asyncio.sleep(0.2)
                # await getLogInOut(r)
                # await asyncio.sleep(0.2)
                await getJobs(token, snaps)
                await asyncio.sleep(0.2)
                # await getTeam(r)
                # await asyncio.sleep(0.2)
                # await getTeamMember(r)
                # await asyncio.sleep(0.2)
                # await getAddress(r)
                # await asyncio.sleep(0.2)
                # await getPlaces(r)
                # await asyncio.sleep(0.2)
                # await getJobType(r)
                # await asyncio.sleep(0.2)
                # await getJobStatus(r)
                # await asyncio.sleep(0.2)
                # await calcDistance(r)
                # await asyncio.sleep(0.2)
                # await checkPendencias(r)
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
