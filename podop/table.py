""" Table lookup backends for podop
"""
import os

import aiohttp
import logging
from urllib.parse import quote

try:
    import aioredis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


class UrlTable(object):
    """ Resolve an entry by querying a parametrized GET URL.
    """

    def __init__(self, url_pattern):
        """ url_pattern must contain a format ``{}`` so the key is injected in
        the url before the query, the ``§`` character will be replaced with
        ``{}`` for easier setup.
        """
        self.url_pattern = url_pattern.replace("§", "{}")

    async def get(self, key, ns=None):
        """ Get the given key in the provided namespace
        """
        logging.debug("Table get {}".format(key))
        if ns is not None:
            key += "/" + ns
        async with aiohttp.ClientSession() as session:
            quoted_key = quote(key)
            async with session.get(
                self.url_pattern.format(quoted_key)
            ) as request:
                if request.status == 200:
                    result = await request.json()
                    logging.debug("Table get {} is {}".format(key, result))
                    return result
                elif request.status == 404:
                    raise KeyError()
                else:
                    raise Exception(request.status)

    async def set(self, key, value, ns=None):
        """ Set a value for the given key in the provided namespace
        """
        logging.debug("Table set {} to {}".format(key, value))
        if ns is not None:
            key += "/" + ns
        async with aiohttp.ClientSession() as session:
            quoted_key = quote(key)
            await session.post(self.url_pattern.format(quoted_key), json=value)

    async def iter(self, cat):
        """ Iterate the given key (experimental)
        """
        logging.debug("Table iter {}".format(cat))
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url_pattern.format(cat)) as request:
                if request.status == 200:
                    result = await request.json()
                    return result

    @staticmethod
    async def teardown():
        pass


class RedisTable(object):
    """ Resolve an entry by querying a Redis database.
    """

    redis_client = None

    def __init__(self, key_pattern):
        """ key_pattern must contain a format ``{}`` so the key is injected in
        the redis key before the query, the ``§`` character will be replaced 
        with ``{}`` for easier setup.
        """
        self.key_pattern = key_pattern.replace("§", "{}")

    async def setup_redis(self):
        if not RedisTable.redis_client:
            RedisTable.redis_client = await aioredis.create_redis(
                os.environ.get("REDIS_URL", "redis://localhost")
            )

    async def get(self, key, ns=None):
        """ Get the given key in the provided namespace
        """
        await self.setup_redis()

        logging.debug("Table get {}".format(key))
        if ns is not None:
            key += "/" + ns
        result = await RedisTable.redis_client.get(
            self.key_pattern.format(key)
        )
        if not result:
            raise KeyError()
        return result

    async def set(self, key, value, ns=None):
        """ Set a value for the given key in the provided namespace
        """
        await self.setup_redis()

        logging.debug("Table set {} to {}".format(key, value))
        if ns is not None:
            key += "/" + ns
        await RedisTable.redis_client.set(self.key_pattern.format(key))

    async def iter(self, cat):
        """ Iterate the given key (experimental)
        """
        logging.debug("Table iter {}".format(cat))

        result = await RedisTable.redis_client.get(
            self.key_pattern.format(cat)
        )
        if not result:
            raise KeyError()
        return result

    @staticmethod
    async def teardown():
        if RedisTable.redis_client:
            RedisTable.redis_client.close()
            await RedisTable.redis_client.wait_closed()
