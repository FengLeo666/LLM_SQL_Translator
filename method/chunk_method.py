import llm_client
from states.main_state import ChunkResult, ChunkState
import utils


async def process_chunk(state:ChunkState):

    llm = llm_client.get_llm()
    llm = llm.with_structured_output(ChunkResult)

    prompt = (
        "你是一名专业的 SQL 迁移与语法转换专家。\n"
        "当前任务是将一个大型数据库中的 SQL 建表语句，从一种数据库格式转换为另一种数据库格式。\n\n"

        f"【源数据库类型】{state.source_format}\n"
        f"【目标数据库类型】{state.destination_format}\n\n"

        "【任务要求】\n"
        f"{state.general_prompt}\n\n"

        "【转换规则与约束】\n"
        "1. 仅对输入的 SQL 语句进行格式和语法层面的转换，不要引入输入中不存在的表或字段。\n"
        "2. 必须保证输出 SQL 在目标数据库中语法合法、可执行。\n"
        "3. 不要省略任何字段定义、表级属性或注释信息。\n"
        "4. 严格保持输入 SQL 中各建表语句及字段的原始顺序。\n"
        "5. 如果源数据库中的某些语法或特性在目标数据库中不支持，请使用目标数据库中语义最接近的实现方式。\n"
        # "6. 你不用对输入进行检查。\n"
        # "6. 不要输出任何解释性文字、说明或 Markdown，只输出转换后的 SQL 语句本身。\n\n"

        "\n\n"
        "【待转换的 SQL 语句（当前分片）】\n"
        f"{state.sql}\n"

        +(f"上次运行的错误：{state.exception}" if state.exception else "")
    )

    cr:ChunkResult=await llm.ainvoke(prompt)

    return {"sql":cr.sql+"\n\n" if cr else "","limiter":state.limiter-1}


async def validate_sql(state:ChunkState):
    if not state.limiter:
        raise RuntimeError(f"重试耗尽，报错:{state.exception}，语句：{state.sql}")
    if not state.sql:
        return {"exception","[warnning]上次调用没有返回sql语句"}
    else:
        e= utils.validate_sql(state.sql, state.destination_sql_language)
        if e:
            return {"exception":str(e)}