from fastapi import Request, HTTPException
from api_service.app.core.redis_client import redis_client
from common.config import get_settings

settings = get_settings()


async def rate_limiter(request: Request):

    client_ip = request.client.host # This gets the IP address of the client making the reques

    key = f"rate_limit:{client_ip}" # This key will store the number of requests made in the current minute.

    current = redis_client.get(key) 

    if current and int(current) >= settings.rate_limit_per_minute:
        raise HTTPException(
            status_code=429,
            detail="Too many requests"
        )

    pipe = redis_client.pipeline() # A pipeline groups multiple Redis commands together. Instead of sending commands one-by-one: # INCR # EXPIRE they are sent together, which is faster.

    pipe.incr(key) # Redis increments the counter.
    pipe.expire(key, 60) # Delete this key after 60 seconds. Becuase we want request per minute

    pipe.execute() # This sends both commands to Redis: