from datetime import datetime

from langchain_core.runnables import RunnableConfig
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from utils import checkpointer_pool

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from method import chunk_method
from states.main_state import ChunkState


async def get_graph(checkpointer: AsyncSqliteSaver=None):
    # 初始化 RedisSaver
    # checkpointer = AsyncRedisSaver(redis_url=CONFIG.REDIS_HOST, ttl={"default_ttl": CONFIG.REDIS_EXPIRE, "refresh_on_read": True})
    if checkpointer is None:
        checkpointer = await checkpointer_pool.get_checkpointer()

    _builder = StateGraph(ChunkState)

    _builder.add_node("process_chunk", chunk_method.process_chunk)
    _builder.add_node("validate_sql", chunk_method.validate_sql)


    _builder.add_edge(START, "process_chunk")
    _builder.add_conditional_edges("process_chunk", lambda x:"validate_sql" if x.destination_sql_language and x.limiter>0 else END)
    _builder.add_conditional_edges("validate_sql",lambda x:"process_chunk" if x.exception else END)
    # _builder.add_edge("process_chunk",END)

    return _builder.compile(name="chunk-transfer-agent", checkpointer=checkpointer)



async def start_or_resume(input_state: ChunkState, checkpointer: AsyncSqliteSaver = None)->str:
    input_state.task_id=input_state.task_id+":"+input_state.sql

    thread_id = input_state.task_id
    config: RunnableConfig = {
        'configurable': {
            'thread_id': thread_id,  # 这里设置线程 ID，确保每次调用的线程有唯一标识
            # 'checkpoint_ns': 'my_namespace',  # 设置命名空间
            # 'checkpoint_id': 'checkpoint-001'  # 设置检查点 ID
        }
    }

    # 确保传递的是 DirectorState 类，而不是模块
    graph = await get_graph(checkpointer)

    checkpoint_list = graph.checkpointer.alist(config=config)

    # 获取最晚的检查点继续
    latest_checkpoint = None

    async for i in checkpoint_list:
        i_topic = i.checkpoint.get("channel_values").get('task_id')
        # 获取每个对象的时间戳
        ts_str = i.checkpoint.get('ts')
        # 将时间戳字符串转换为 datetime 对象
        ts = datetime.fromisoformat(ts_str)
        # 如果是第一次遍历或当前时间戳更大，更新最大时间戳
        if ('latest_ts' not in locals() or ts > latest_ts) and i_topic == input_state.task_id:
            latest_ts = ts
            latest_checkpoint = i

    if latest_checkpoint:
        print(f"[LangGraph] 检测到未完成的chunk, idx:{input_state.sql[:50]}")

        config['configurable']["checkpoint"] = latest_checkpoint
        # 自动恢复 + 继续执行
        rs = await graph.ainvoke(
            None,  # resume 时必须传 None，表示从 checkpoint 恢复
            config=config
        )
    else:
        rs = await graph.ainvoke(input_state, config=config)
    return rs["sql"]


