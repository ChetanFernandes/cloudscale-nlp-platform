from fastapi import APIRouter
from common.config import get_settings

router = APIRouter(prefix = "/health",tags = ["Health"])

@router.get("/live")
def liveness_check():
    return {"status": "alive"}


@router.get("/ready")
def readines_check():
    settings = get_settings()

    # For now, we just confirm config loads.
    # Later we will check DB + Redis here.

    return {"Status": "ready","environment":settings.app_env}