import asyncio
import hashlib
import json
import os
import re
from datetime import datetime
from functools import wraps
from typing import List, Any

import sqlglot

from .rate_limiter import rate_limited
from .singleflight import singleflight
# from .checkpointer_pool import lifespan,get_checkpointer


def split_sql(sql: str) -> List[str]:
    """
    切分规则：
    - 大小写不敏感查找 CREATE TABLE
    - 对每个 CREATE TABLE，找到其左侧最近的 ';'，取 boundary = semicolon_idx + 1 作为切分点
    - 除最后一个 chunk 外，所有 chunk 都会包含该分号，因此以 ';' 结尾（忽略尾部空白）
    """
    # 1) 找到所有 CREATE TABLE 的起始位置（大小写不敏感）
    ct_iter = list(re.finditer(r"create\s+table", sql, flags=re.IGNORECASE))
    if not ct_iter:
        return [sql.strip()] if sql.strip() else []

    # 2) 计算所有切分边界：最近分号后的 idx
    boundaries = set()
    for m in ct_iter:
        ct_pos = m.start()
        semi = sql.rfind(";", 0, ct_pos)  # 最近分号
        if semi != -1:
            boundaries.add(semi + 1)

    # 没有任何分号（或首个 CREATE TABLE 前无分号）时，不强行切 preamble
    boundaries = sorted(b for b in boundaries if 0 < b < len(sql))

    # 3) 切片
    chunks: List[str] = []
    start = 0
    for b in boundaries:
        if b <= start:
            continue
        part = sql[start:b].strip()
        if part:
            chunks.append(part)
        start = b

    last = sql[start:].strip()
    if last:
        chunks.append(last)

    return chunks

def task_id(*args)->str:
    return stable_cache_key("_".join(args))

def stable_cache_key(sentence: str) -> str:
    return hashlib.sha256(sentence.encode("utf-8")).hexdigest()



def stable_model_hash(
    model: Any,
    *,
    exclude: set[str] | None = None,
) -> str:
    """
    对 Pydantic Model / dict 做稳定 hash
    """
    if hasattr(model, "model_dump"):  # pydantic v2
        data = model.model_dump(exclude=exclude or set())
    elif hasattr(model, "dict"):      # pydantic v1
        data = model.dict(exclude=exclude or set())
    else:
        raise TypeError("stable_model_hash only supports pydantic models")

    raw = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return stable_cache_key(raw)



# def semaphore(thread_allowed:int):
#     semaphore_obj = asyncio.Semaphore(thread_allowed)
#
#     def synchronized(func):
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             async with semaphore_obj:  # 限制并发请求数
#                 return await func(*args, **kwargs)
#
#         return wrapper
#
#     return synchronized

def validate_sql(
    sql_text: str,
    sql_format: str,
    # *,
    # pretty: bool = True,
):
    """
    使用 sqlglot 对 SQL 文本进行语法校对，并按目标方言重新生成 SQL。

    参数：
        sql_text:      待校对的 SQL（可包含多条语句）
        sql_format:    目标 SQL 方言，例如: 'hive', 'mysql', 'postgres', 'spark'
        pretty:        是否格式化输出（缩进 / 换行）

    返回：
        规范化后的 SQL 文本（字符串）

    异常：
        ValueError: 当 SQL 不符合指定方言语法时抛出
    """
    try:
        # 一次性解析多条 SQL
        expressions = sqlglot.parse(sql_text, read=sql_format)
        return None
    except Exception as e:
        return e


# def count_tokens_auto(text, model=None):
#     if not model:
#         model=CONFIG.LLM_TYPE
#     if model.startswith("qwen") or "qwen" in model.lower():
#         tokenizer = get_tokenizer(model)
#         return len(tokenizer.encode(text))
#
#         # OpenAI / DeepSeek / LLaMA 等：继续尝试 tiktoken
#     try:
#         import tiktoken
#         enc = tiktoken.encoding_for_model(model)
#         return len(enc.encode(text))
#     except KeyError:
#         pass
#
#         # 其他模型 fallback → transformers（不会处理 qwen3-max）
#     try:
#         from transformers import AutoTokenizer
#         tokenizer = AutoTokenizer.from_pretrained(model)
#         return len(tokenizer.encode(text))
#     except:
#         raise ValueError(f"模型 {model} 无法找到对应的 tokenizer，请手动指定。")



def str2time(t:str)->datetime:
    return datetime.strptime(t, "%Y-%m-%d")

def semaphore(thread_allowed:int):
    semaphore_obj = asyncio.Semaphore(thread_allowed)

    def synchronized(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with semaphore_obj:  # 限制并发请求数
                return await func(*args, **kwargs)

        return wrapper

    return synchronized



def stable_cache_key(sentence: str) -> str:
    """基于单个句子的内容生成 Redis 缓存键"""
    return hashlib.sha256(sentence.encode("utf-8")).hexdigest()

def list_and_disconnect_connections(db_path, psutil=None):
    # 确保数据库文件存在
    if not os.path.isfile(db_path):
        print(f"数据库文件 {db_path} 不存在！")
        return

    # 获取所有 SQLite 相关的进程
    pid_list = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            # 获取进程的所有连接
            for conn in proc.connections(kind='inet'):
                if db_path in str(conn.laddr) or db_path in str(conn.raddr):
                    pid_list.append(proc.info['pid'])
                    print(f"发现进程 {proc.info['pid']} 使用数据库 {db_path}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    # 如果没有找到相关进程，返回提示
    if not pid_list:
        print(f"没有发现任何与 {db_path} 相关的连接。")
        return

    # 停止所有相关进程
    for pid in pid_list:
        try:
            proc = psutil.Process(pid)
            proc.terminate()  # 终止进程
            print(f"已终止进程 {pid}。")
        except psutil.NoSuchProcess:
            print(f"进程 {pid} 已不存在。")
        except psutil.AccessDenied:
            print(f"无法终止进程 {pid}，权限不足。")
        except Exception as e:
            print(f"无法终止进程 {pid}，错误: {e}")