# -*- coding: utf-8 -*-
from redis_pal import RedisPal


class RedisHash(RedisPal):
    """
    RedisPal helps you store data on Redis
    without worrying about serialization
    stuff. Besides that, it also stores
    timestamps for last changes so you can
    check that whenever you want.

    You can use this as a key-value data
    structure using "get" and "set"
    methods.

    This class inherits from redis.Redis
    and the main difference is that it
    handles serialization and deserialization
    of objects so you can set anything to
    a key.
    """

    def hgetall(self, name: str) -> dict:
        """
        Retrieves a dictionary of values from Redis based on a given name.

        Args:
            name (str): The name of the Redis hash.

        Returns:
            dict: The dictionary of values associated with the Redis key.
        """
        redis_response = super(RedisPal, self).hgetall(name=name)
        decoded_redis_response = {
            chave.decode("utf-8"): valor.decode("utf-8") for chave, valor in redis_response.items()
        }

        return decoded_redis_response
