"""Base implementation of the async API."""

import json
import aiohttp
import asyncio
from logging import getLogger
import ssl
import certifi
from abc import ABC
from typing import *

from .taxonomies import *
from .utils import TokenBucket


MESSAGE_LIST = {
    "sent": (
        "Sent request to: '{}'.",
        20,
    ),
    "error": (
        "Error in request to: '{}'. Error: {}",
        30,
    ),
    "shutdown": (
        "Persistent error in request to: '{}'. Tried {} times.",
        40,
    ),
}

logger = getLogger(__name__)


class ApiError(Exception):
    pass


class AsyncApi(ABC):

    def __init__(
            self,
            base_url: str,
            *,
            headers: Optional[dict] = None,
            timeout: int = 10,
            rate_max_bucket_size: int = 5,
            rate_limit: Optional[int] = None,
            concurrent_limit: Optional[int] = None,
            block_size: Optional[int] = None,
            retries: int = 0,
            tcp_limit: int = 100,
            tcp_limit_per_host: int = 0,
            cafile: str = certifi.where()):
        self.base_url = base_url
        self.headers = headers

        if timeout <= 0:
            raise ApiError("Timeout must be a positive number.")
        self.timeout = timeout

        if (
            rate_max_bucket_size <= 0
            or not isinstance(rate_max_bucket_size, int)
        ):
            raise ApiError("Rate max bucket size must be a positive integer.")
        self.rate_max_bucket_size = rate_max_bucket_size

        if rate_limit is not None and rate_limit <= 0:
            raise ApiError("Rate limit must be a positive number.")
        self.rate_limit = rate_limit

        if (
            concurrent_limit is not None
            and (concurrent_limit <= 0
            or not isinstance(concurrent_limit, int))
        ):
            raise ApiError("Concurrent limit must be a positive integer.")
        self.concurrent_limit = concurrent_limit

        if (
            block_size is not None
            and (block_size <= 0
            or not isinstance(rate_limit, int))
        ):
            raise ApiError("Block size must be a positive integer.")
        self.block_size = block_size
        
        if retries < 0 or not isinstance(retries, int):
            raise ApiError("Retries must be a non-negative integer.")
        self.retries = retries
        
        if tcp_limit < 0 or not isinstance(tcp_limit, int):
            raise ApiError("TCP limit must be a non-negative integer.")
        self.tcp_limit = tcp_limit

        if tcp_limit_per_host < 0 or not isinstance(tcp_limit_per_host, int):
            raise ApiError(
                "TCP limit per host must be a non-negative integer."
            )
        self.tcp_limit_per_host = tcp_limit_per_host
        
        self.cafile = cafile

    async def restart(self):
        await self.shutdown()
        self.start()
    
    def start(self):
        certificates = ssl.create_default_context(
            cafile=self.cafile)
        tcp = aiohttp.TCPConnector(
            limit=self.tcp_limit, limit_per_host=self.tcp_limit_per_host,
            ssl=certificates)
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        self.session = aiohttp.ClientSession(
            self.base_url, connector=tcp,
            headers=self.headers, timeout=timeout
        )

    async def shutdown(self):
        if getattr(self, "session", None):
            await self.session.close()
            del self.session

    async def __aenter__(self):
        # set the request session
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # close and delete the request session
        await self.shutdown()

    def log(self, message, *args, **kwargs):
        _message = message[0].format(*args, **kwargs)
        log_level = message[1]
        logger.log(log_level, _message)

    def retry(coro: Coroutine):
        async def wrapper(self, *args, **kwargs):
            retries = -1
            while retries < self.retries:
                self.log(MESSAGE_LIST["sent"], args[1])

                try:
                    return await coro(self, *args, **kwargs)
                except aiohttp.ClientError as e:
                    retries += 1
                    self.log(MESSAGE_LIST["error"], args[1], str(e))

            self.log(MESSAGE_LIST["shutdown"], args[1], retries + 1)

        return wrapper

    @retry
    async def request(
            self, coro: Coroutine, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        async with coro(
            path, raise_for_status=True, **kwargs
        ) as res:
            if body_type == ContentType.BYTE:
                return await res.read()
            elif body_type == ContentType.TEXT:
                return await res.text()
            else:
                if res.content_type == "text/html":
                    return json.loads(await res.text())
                else:
                    return await res.json()

    async def _bridge(
            self, method: str, path: str,
            body_type: ContentType = ContentType.JSON, **kwargs):
        started = False
        if not getattr(self, "session", None):
            self.start()
            started = True

        # url = urljoin(self.base_url, path)

        res = await self.request(
            getattr(self.session, method), path,
            body_type=body_type, **kwargs
        )

        if started:
            await self.shutdown()
        
        return res

    async def get(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.GET, path, body_type, **kwargs)
    
    async def post(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.POST, path, body_type, **kwargs)

    async def put(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.PUT, path, body_type, **kwargs)

    async def delete(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.DELETE, path, body_type, **kwargs)

    async def head(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.HEAD, path, body_type, **kwargs)

    async def options(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.OPTIONS, path, body_type, **kwargs)

    async def patch(
            self, path: str, *,
            body_type: ContentType = ContentType.JSON, **kwargs):
        return await self._bridge(HTTPMethod.PATCH, path, body_type, **kwargs)
    
    def _wrap_awaitable(self, awaitable, semaphore, rate_limit):

        async def wrapper():
            if semaphore and rate_limit:
                async with semaphore:
                    async with rate_limit:
                        return await awaitable
            if semaphore:
                async with semaphore:
                    return await awaitable
            if rate_limit:
                async with rate_limit:
                    return await awaitable
            
            return await awaitable
        
        return wrapper

    def _wrap_block(self, block):
        if self.rate_limit:
            rate_limit = TokenBucket(
                self.rate_max_bucket_size, self.rate_limit
            )
        else:
            rate_limit = None
        
        if self.concurrent_limit:
            semaphore = asyncio.Semaphore(self.concurrent_limit)
        else:
            semaphore = None
        
        wrapped = []
        for awaitable in block:
            wrapped_awaitable = self._wrap_awaitable(
                awaitable, semaphore, rate_limit
            )
            wrapped.append(wrapped_awaitable)
        
        async def wrapper():
            return await asyncio.gather(*block)

        return wrapper

    def _process_block(self, block, run=False):
        block_future = asyncio.ensure_future(block())

        if run:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(block_future)

        return block_future

    def awaitable(func):

        def wrapper(self, *args, run=False, **kwargs):
            block = []
            for awaitable in func(self, *args, **kwargs):
                block.append(awaitable)

                if self.block_size and len(block) >= self.block_size:
                    wrapped_block = self._wrap_block(block)
                    block_future = self._process_block(wrapped_block, run=run)
                    yield block_future
                    block = []

            if block:
                wrapped_block = self._wrap_block(block)
                block_future = self._process_block(wrapped_block, run=run)
                yield block_future
                block = []

        return wrapper
