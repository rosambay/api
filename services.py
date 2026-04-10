import httpx
from typing import List
from loguru import logger
from config import settings


async def optimize_routes_vroom(payload: dict) -> dict:
    logger.debug(
        "VROOM request | vehicles={} jobs={}",
        len(payload.get("vehicles", [])),
        len(payload.get("jobs", [])),
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                settings.vroom_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        if response.status_code != 200:
            logger.error(
                "VROOM HTTP {} | url={} | body={}",
                response.status_code,
                settings.vroom_url,
                response.text[:2000],
            )
            response.raise_for_status()

        result = response.json()

        unassigned = result.get("unassigned", [])
        routes     = result.get("routes", [])
        logger.info(
            "VROOM ok | routes={} unassigned={}",
            len(routes),
            len(unassigned),
        )
        if unassigned:
            logger.warning("VROOM unassigned jobs: {}", [u.get("id") for u in unassigned])

        return result

    except httpx.TimeoutException as exc:
        logger.error("VROOM timeout (60s) | url={} | {}", settings.vroom_url, exc)
        raise

    except httpx.ConnectError as exc:
        logger.error("VROOM connection error | url={} | {}", settings.vroom_url, exc)
        raise

    except httpx.HTTPStatusError as exc:
        logger.error(
            "VROOM HTTP error {} | url={} | response={}",
            exc.response.status_code,
            settings.vroom_url,
            exc.response.text[:2000],
        )
        raise

    except Exception as exc:
        logger.exception("VROOM unexpected error | url={} | {}", settings.vroom_url, exc)
        raise


async def get_route_distance(coordinates: List[List[float]]) -> dict:
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coordinates)
    url = f"https://routes.imagineit.com.br/routes/route/v1/driving/{coords_str}"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, params={"overview": "true"})
        response.raise_for_status()

    data = response.json()

    if data.get("code") != "Ok" or not data.get("routes"):
        raise ValueError(f"OSRM retornou erro: {data.get('code')}")

    distance_meters = data["routes"][0]["distance"]
    distance_time = data["routes"][0]["duration"]
    return {
        "distance_meters": distance_meters,
        "distance_km": round(distance_meters / 1000, 3),
        "distance_time": distance_time,
        "distance_time_minutes": round(distance_time / 60, 2)
    }


async def geocode_mapbox(address: str) -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json",
            params={"access_token": settings.mapbox_key, "limit": 1},
        )
        response.raise_for_status()

    data = response.json()
    features = data.get("features", [])
    if not features:
        raise ValueError(f"Nenhum resultado encontrado para o endereço: {address}")

    feature = features[0]
    lon, lat = feature["geometry"]["coordinates"]
    return {
        "address": feature.get("place_name"),
        "longitude": lon,
        "latitude": lat,
    }


async def get_route_distance_block(coordinates: List[List[float]]) -> List[dict]:
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coordinates)
    url = f"https://routes.imagineit.com.br/routes/route/v1/driving/{coords_str}?overview=false"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, params={"overview": "false"})
        response.raise_for_status()

    data = response.json()

    if data.get("code") != "Ok" or not data.get("routes"):
        raise ValueError(f"OSRM retornou erro: {data.get('code')}")

    legs = data['routes'][0]['legs']
    return legs
