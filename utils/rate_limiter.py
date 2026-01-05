import asyncio
import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from typing import  Any, Callable, Dict, TypeVar, Awaitable, Deque, Optional


T = TypeVar("T")#泛型，保证函数内提示正确

class _ExclusiveRateLimiter:
    """
    严格节流：每 interval 仅放行 1 次。无 burst。
    可选 FIFO：启用后严格按等待顺序放行。
    """
    def __init__(self, qpm: float):
        if qpm <= 0:
            raise ValueError("qpm must be > 0")

        self.interval = 60.0 / float(qpm)

        # 统一使用 monotonic，避免系统时间回拨导致逻辑错误
        self._next_time = time.monotonic()

        # 非 FIFO：用锁串行化即可
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)  # 锁里面睡
                now = time.monotonic()
            # “严格节流”：每次放行都推进 next_time
            self._next_time = max(now, self._next_time) + self.interval


# 全局 registry：支持“同 key 共享同 limiter”
_LIMITERS: Dict[str, _ExclusiveRateLimiter] = {}


def rate_limited(
    *,
    qpm: float,
    scope: str = "global",
    shared: bool = False,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """
    装饰器：限制被装饰 async 函数的调用速率（严格 QPM，无 burst）。

    参数：
    - qpm: 每分钟允许次数
    - scope: 用于共享/隔离 limiter 的命名空间（例如 "search-api" / "llm"）
    - shared: True = 相同 (qpm,fifo,scope) 的函数共享同一个 limiter
              False = 每个被装饰函数独立 limiter
    """
    if qpm <= 0:
        raise ValueError("qpm must be > 0")

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        if shared:
            key = scope
            limiter = _LIMITERS.get(key)
            if limiter is None:
                limiter = _ExclusiveRateLimiter(qpm=qpm)
                _LIMITERS[key] = limiter
            assert format(limiter.interval,".2f") == format(60.0 / float(qpm),".2f"), f"[Rate Limiter] Conflict QPM '{qpm}' and '{limiter.interval}' in scope '{scope}'."
        else:
            limiter = _ExclusiveRateLimiter(qpm=qpm)

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # 这里调用 get_running_loop，确保不会在 import 阶段触发事件循环错误
            _ = asyncio.get_running_loop()
            await limiter.acquire()
            return await func(*args, **kwargs)

        return wrapper

    return decorator