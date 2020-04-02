import asyncio
import pickle
from typing import Callable, Dict, List, Tuple
from functools import wraps

from redis.setup import create_redis

from aioredis.errors import BusyGroupError
import aioredis


class Streem():
    def __init__(self):
        self._redis = None
        self._callables: Dict[str, Callable] = {}

    def task(self, stream_name: str):
        def _decorator(func):
            if stream_name in self._callables:
                raise Exception(f"Stream: {stream_name} already registered")

            self._callables[stream_name] = func

            @wraps(func)
            async def _call(self, *args, **kwargs):
                await self._create_redis()

                arguments = pickle.dumps(args)
                kwarguments = pickle.dumps(kwargs)
                data = {
                    "arguments": arguments,
                    "kwarguments": kwarguments
                }

                return await self.redis._redis.xadd(self._stream_name, data)
            return _call
        return _decorator

    async def start(self, consumer_name: str, group_name: str = "workers"):
        await self._create_redis()
        print(self._callables)

        stream_names = list(self._callables.keys())
        id_selectors = ["0"] * len(stream_names)

        for stream_name in stream_names:
            try:
                await self._redis.xgroup_create(stream_name, group_name,
                                                mkstream=True)
            except BusyGroupError:
                # print(f"Consumer group for '{stream_name}' already exists")
                pass

        while True:
            messages = await self._redis.xread_group(group_name,
                                                     consumer_name,
                                                     stream_names,
                                                     count=10,
                                                     latest_ids=id_selectors)

            for received_stream_name, message_id, data in messages:
                received_stream_name = received_stream_name.decode("utf-8")
                arguments = pickle.loads(data[b"arguments"])
                kwarguments = pickle.loads(data[b"kwarguments"])

                await self._callables[received_stream_name](*arguments,
                                                            **kwarguments)

                await self._redis.xack(received_stream_name, group_name,
                                       message_id)

            if not messages:
                id_selectors = [">"] * len(stream_names)

    async def _create_redis(self):
        if not self._redis:
            self._redis = await aioredis.create_redis_pool('redis://localhost')
