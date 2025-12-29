from datetime import datetime

from langchain_core.runnables import RunnableConfig
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from utils import checkpointer_pool

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from method import main_method
from states.main_state import MainState


async def get_graph(checkpointer: AsyncSqliteSaver=None):
    # 初始化 RedisSaver
    # checkpointer = AsyncRedisSaver(redis_url=CONFIG.REDIS_HOST, ttl={"default_ttl": CONFIG.REDIS_EXPIRE, "refresh_on_read": True})
    if checkpointer is None:
        checkpointer = await checkpointer_pool.get_checkpointer()

    _builder = StateGraph(MainState)

    _builder.add_node("prompt_normalize",main_method.prompt_normalize)
    _builder.add_node("chunk_sql", main_method.chunk_sql)
    _builder.add_node("send_tasks", main_method.send_tasks)
    # _builder.add_node("final_join",method.final_join)

    _builder.add_edge(START,"prompt_normalize")
    _builder.add_edge("prompt_normalize","chunk_sql")
    # _builder.add_edge("prompt_normalize", "chunk_sql")
    _builder.add_edge("chunk_sql","send_tasks")
    _builder.add_edge( "send_tasks",END)

    return _builder.compile(name="sql-transfer-agent", checkpointer=checkpointer)



async def start_or_resume(input_state: MainState, checkpointer: AsyncSqliteSaver = None)->str:

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
        i_task_id = i.checkpoint.get("channel_values").get('task_id')
        # 获取每个对象的时间戳
        ts_str = i.checkpoint.get('ts')
        # 将时间戳字符串转换为 datetime 对象
        ts = datetime.fromisoformat(ts_str)
        # 如果是第一次遍历或当前时间戳更大，更新最大时间戳
        if ('latest_ts' not in locals() or ts > latest_ts) and i_task_id == input_state.task_id:
            latest_ts = ts
            latest_checkpoint = i

    if latest_checkpoint:
        print(
            f"[LangGraph] 检测到未完成的转化 from {input_state.source_format} to {input_state.destination_format} for {input_state.source_sql[:50]}...\n"
            f"将从检查点继续执行director_graph: {list(latest_checkpoint.checkpoint.get('channel_values').keys())[-1]}")

        config['configurable']["checkpoint"] = latest_checkpoint
        # 自动恢复 + 继续执行
        rs = await graph.ainvoke(
            None,  # resume 时必须传 None，表示从 checkpoint 恢复
            config=config
        )
    else:
        print(f"[LangGraph] 未检测到 checkpoint，开始新的转化流程: from {input_state.source_format} to {input_state.destination_format} for {input_state.source_sql[:50]}...\n")
        rs = await graph.ainvoke(input_state, config=config)
    return rs["result"]


