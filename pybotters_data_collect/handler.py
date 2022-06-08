from __future__ import annotations
from typing import Any, Union, Callable

import asyncio
import loguru
import pybotters
import traceback

from motor import motor_asyncio


class _BaseHandler:
    def __init__(
            self,
            produce_fn: Callable[[dict[str, Any], Any], list[dict]],
            logger: logging.Logger = None
    ):
        self._produce_fn = produce_fn
        self._logger = logger or loguru.logger
        self._queue = asyncio.Queue()
        asyncio.create_task(self._consumer())

    def __call__(self, msg: dict[str, Any], ws):
        return self.onmessage(msg, ws)

    def onmessage(self, msg: dict[str, Any], ws):
        item = self._produce(msg, ws)
        if item is not None:
            self._queue.put_nowait(item)

    def _produce(self, msg: dict[str, Any], ws):
        return self._produce_fn(msg, ws)

    async def _consume(self, data: list[dict]):
        raise NotImplementedError

    async def _consumer(self):
        while True:
            data = await self._queue.get()
            self._logger.debug(data)
            await self._consume(data)


class MongoHandler(_BaseHandler):
    def __init__(
            self,
            produce_fn: Callable[[dict[str, Any], Any], list[dict]] = None,
            *,
            collection: 'motor.motor_asyncio.AsyncIOMotorCollection' = None,
            db_name: str = None,
            collection_name: str = None,
            logger: logging.Logger = None
    ):
        super(MongoHandler, self).__init__(produce_fn, logger)

        if collection is not None:
            self._collection = collection
        else:
            self._collection = self.create_collection(db_name=db_name, collection_name=collection_name)

    async def _consume(self, data: list[dict]):
        try:
            await self._collection.insert_many(data)
        except Exception as e:
            self._logger.error(f"Failed to insert: {data}\n{traceback.format_exc()}")

    @property
    def collection(self) -> motor_asyncio.AsyncIOMotorCollection:
        return self._collection

    @classmethod
    def create_collection(
            cls,
            *,
            host='localhost',
            port=27017,
            db_name=None,
            collection_name=None,
            indexes=None,
            uniq_index=False,
            background_indexing=True,
    ):
        assert db_name is not None
        assert collection_name is not None
        client = motor_asyncio.AsyncIOMotorClient(host, port)
        db = client[db_name]
        collection = db[collection_name]

        if indexes is not None:
            # https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html?highlight=index#motor.motor_asyncio.AsyncIOMotorCollection.create_index
            collection.create_index(indexes, unique=uniq_index, background=background_indexing)

        return collection
