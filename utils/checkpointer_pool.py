from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from contextlib import asynccontextmanager



_checkpointer=None

@asynccontextmanager
async def lifespan(*args,path="resources/checkpoints.db",**kwargs):
    global _checkpointer
    async with AsyncSqliteSaver.from_conn_string(
        path
    ) as saver:
        _checkpointer = saver
        yield


# @utils.semaphore(1)
async def get_checkpointer():
    global _checkpointer

    return _checkpointer