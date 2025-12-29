import asyncio
import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from typing import  Any, Callable, Dict, TypeVar, Awaitable, Deque, Optional


T = TypeVar("T")

@dataclass(frozen=True)
class _LimiterKey:
    qpm: float
    fifo: bool
    scope: str  # 用于共享/隔离 limiter


class _ExclusiveRateLimiter:
    """
    严格节流：每 interval 仅放行 1 次。无 burst。
    可选 FIFO：启用后严格按等待顺序放行。
    """
    def __init__(self, qpm: float, fifo: bool):
        if qpm <= 0:
            raise ValueError("qpm must be > 0")

        self.interval = 60.0 / float(qpm)
        self.fifo = fifo

        # 统一使用 monotonic，避免系统时间回拨导致逻辑错误
        self._next_time = time.monotonic()

        # 非 FIFO：用锁串行化即可
        self._lock = asyncio.Lock()

        # FIFO：维护等待队列
        self._queue: Deque[asyncio.Future[None]] = deque()
        self._queue_lock = asyncio.Lock()
        self._pump_task: Optional[asyncio.Task[None]] = None

    async def acquire(self) -> None:
        if not self.fifo:
            await self._acquire_non_fifo()
        else:
            await self._acquire_fifo()

    async def _acquire_non_fifo(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
                now = time.monotonic()
            # “严格节流”：每次放行都推进 next_time
            self._next_time = max(now, self._next_time) + self.interval

    async def _acquire_fifo(self) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()

        async with self._queue_lock:
            self._queue.append(fut)
            if self._pump_task is None or self._pump_task.done():
                self._pump_task = asyncio.create_task(self._pump())

        await fut  # 等待轮到自己

    async def _pump(self) -> None:
        """
        FIFO 泵：按队列顺序，每 interval 唤醒一个等待者。
        """
        while True:
            async with self._queue_lock:
                if not self._queue:
                    return
                fut = self._queue[0]

            # 严格按 next_time 节流
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
                now = time.monotonic()

            # 唤醒队首（若它已取消/完成则弹掉继续）
            async with self._queue_lock:
                if not self._queue:
                    return
                fut = self._queue[0]
                self._queue.popleft()

            if not fut.done():
                fut.set_result(None)

            self._next_time = max(now, self._next_time) + self.interval


# 全局 registry：支持“同 key 共享同 limiter”
_LIMITERS: Dict[_LimiterKey, _ExclusiveRateLimiter] = {}


def rate_limited(
    *,
    qpm: float,
    fifo: bool = False,
    scope: str = "global",
    shared: bool = False,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """
    装饰器：限制被装饰 async 函数的调用速率（严格 QPM，无 burst）。

    参数：
    - qpm: 每分钟允许次数
    - fifo: True = 严格 FIFO；False = 非 FIFO（更轻量）
    - scope: 用于共享/隔离 limiter 的命名空间（例如 "search-api" / "llm"）
    - shared: True = 相同 (qpm,fifo,scope) 的函数共享同一个 limiter
              False = 每个被装饰函数独立 limiter
    """
    if qpm <= 0:
        raise ValueError("qpm must be > 0")

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        if shared:
            key = _LimiterKey(qpm=qpm, fifo=fifo, scope=scope)
            limiter = _LIMITERS.get(key)
            if limiter is None:
                limiter = _ExclusiveRateLimiter(qpm=qpm, fifo=fifo)
                _LIMITERS[key] = limiter
        else:
            limiter = _ExclusiveRateLimiter(qpm=qpm, fifo=fifo)

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # 这里调用 get_running_loop，确保不会在 import 阶段触发事件循环错误
            _ = asyncio.get_running_loop()
            await limiter.acquire()
            return await func(*args, **kwargs)

        return wrapper

    return decorator