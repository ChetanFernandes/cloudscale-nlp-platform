# Creates a Redis connection we can reuse everywhere.

import redis
from common.config import get_settings

settings = get_settings()

redis_client = redis.Redis.from_url(
    settings.redis_url,
    decode_responses=True
)