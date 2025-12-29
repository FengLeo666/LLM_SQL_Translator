import asyncio
import functools
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, TypeVar

T = TypeVar("T")

_INFLIGHT: Dict[str, asyncio.Future] = {}
_LOCK = asyncio.Lock()


def _deepcopy_pydantic_or_value(obj: Any) -> Any:
    if hasattr(obj, "model_copy"):
        return obj.model_copy(deep=True)   # pydantic v2
    if hasattr(obj, "copy"):
        try:
            return obj.copy(deep=True)     # pydantic v1
        except TypeError:
            pass
    return obj


def _to_canonical(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {"__bytes__": bytes(obj).hex()}

    if isinstance(obj, (datetime, date)):
        return {"__datetime__": obj.isoformat()}

    if isinstance(obj, Decimal):
        return {"__decimal__": str(obj)}

    if isinstance(obj, (list, tuple, set)):
        if isinstance(obj, set):
            return {"__set__": sorted((_to_canonical(x) for x in obj), key=lambda x: repr(x))}
        return [_to_canonical(x) for x in obj]

    if isinstance(obj, dict):
        return {str(k): _to_canonical(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}

    # Pydantic v2
    if hasattr(obj, "model_dump"):
        return {
            "__pydantic__": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
            "data": _to_canonical(obj.model_dump(mode="json")),
        }

    # Pydantic v1
    if hasattr(obj, "dict"):
        try:
            return {
                "__pydantic__": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
                "data": _to_canonical(obj.dict()),
            }
        except TypeError:
            pass

    # dataclass
    if hasattr(obj, "__dataclass_fields__"):
        from dataclasses import asdict
        return {
            "__dataclass__": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
            "data": _to_canonical(asdict(obj)),
        }

    # 兜底（尽量避免传入此类对象）
    return {
        "__repr__": repr(obj),
        "__type__": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
    }


def _hash_call(fn: Callable[..., Any], args: tuple, kwargs: dict) -> str:
    payload = {
        "fn": f"{fn.__module__}.{fn.__qualname__}",
        "args": _to_canonical(args),
        "kwargs": _to_canonical(kwargs),
    }
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def singleflight(_fn: Callable[..., Awaitable[T]] | None = None):
    """
    既可用作：
        @singleflight_auto
    也可用作：
        @singleflight_auto()
    """

    def decorator(fn: Callable[..., Awaitable[T]]):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs) -> T:
            key = _hash_call(fn, args, kwargs)

            async with _LOCK:
                fut = _INFLIGHT.get(key)
                if fut is None:
                    loop = asyncio.get_running_loop()
                    fut = loop.create_future()
                    _INFLIGHT[key] = fut
                    owner = True
                else:
                    owner = False

            if not owner:
                res = await asyncio.shield(fut)
                return _deepcopy_pydantic_or_value(res)

            async def run_and_set():
                try:
                    result = await fn(*args, **kwargs)
                    if not fut.done():
                        fut.set_result(result)
                except Exception as e:
                    if not fut.done():
                        fut.set_exception(e)
                finally:
                    async with _LOCK:
                        if _INFLIGHT.get(key) is fut:
                            _INFLIGHT.pop(key, None)

            asyncio.create_task(run_and_set())
            res = await asyncio.shield(fut)
            return _deepcopy_pydantic_or_value(res)

        return wrapper

    # 不带括号：@singleflight_auto
    if callable(_fn):
        return decorator(_fn)

    # 带括号：@singleflight_auto()
    return decorator
