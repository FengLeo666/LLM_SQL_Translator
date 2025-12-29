from pydantic import BaseModel, Field

import CONFIG
import llm_client
from graph import chunk_graph
from states.main_state import MainState, ChunkState
from tqdm.asyncio import tqdm
import utils

# async def requirement_alignment(state:MainState):
#     llm=llm_client.get_llm()
#     prompt=""


async def prompt_normalize(state: MainState):
    llm = llm_client.get_llm()

    class Prompt(BaseModel):
        prompt: str = Field(description="用于遍历执行数据库每个表的提示词。")

    llm = llm.with_structured_output(Prompt)

    prompt = (
            "你是一名资深的数据仓库与 SQL 迁移专家，长期从事关系型数据库到数据仓库体系（ODS / STG / HD）的建模与迁移工作，"
            "对数仓分层规范、审计字段（技术字段）、批次字段以及派生表（清洗表 / 错误表 / 临时表）的设计原则非常熟悉。\n\n"

            "⚠️ 注意：当前你【不是】在执行具体的 SQL 转换任务，而是在为后续【逐表遍历调用大模型】设计一个【通用子任务提示词模板】。\n"
            "该提示词将被系统用于：每次输入若干表的建表 SQL，调用大模型完成一次标准化、可重复的转换。\n\n"

            "【整体任务背景】\n"
            "系统正在将一个大型数据库或数仓中的建表 SQL，从源数据库格式迁移到目标数据库 / 数仓规范下。\n"
            f"整个 SQL 文件已经按“{state.merge_n}个表的粒度”拆分为多个chunk，后续流程会对每一个分片独立调用大模型进行处理。\n\n"

            "【你的任务目标】\n"
            f"请生成一个【通用提示词】，用于指导大模型在“{state.merge_n}个表的粒度”下完成 SQL 建表语句的规范化转换。\n"
            "该提示词必须能够让模型在没有额外上下文的情况下，仅根据输入 SQL 与规则要求，生成符合数仓规范的建表语句。\n\n"

            "【该通用提示词必须明确指导模型遵循以下原则】\n"
            f"1. 输入内容始终是【{state.merge_n}个表】的源数据库建表 SQL；\n"
            "2. 只处理当前chunk，不假设存在其他跨chunk上下文；\n"
            "3. 严格遵循用户给定的转换规则与目标数仓模板规范，其优先级高于任何通用 SQL 经验；\n"
            "4. 正确处理字段名称、数据类型、字段顺序、字段注释与表注释；\n"
            "5. 明确这是【数据仓库建表场景】，而不是 OLTP 业务表迁移，"
            "需要特别关注数仓中的【审计字段 / 技术字段 / 批次字段】；\n"
            "6. 审计字段通常不是源表业务字段，而是由目标数仓规范统一定义，"
            "是否需要新增此类字段，必须以【用户规则或目标模板】为准，禁止仅因示例中存在而擅自添加；\n"
            "7. 源表中的业务字段应尽量保持语义与顺序一致，"
            "审计字段通常位于表结构的末尾；\n"
            "8. 忽略源数据库中不适用于目标数仓的对象，"
            "包括但不限于：索引、主键 / 唯一键 / 外键、WITH 存储参数、TABLESPACE 等；\n"
            "9. 除非用户规则或模板明确要求，不得凭空新增任何源表中不存在的字段，包括审计字段；\n"
            "10. 最终只输出目标数据库 / 数仓规范下的建表 SQL，不输出任何解释、分析或说明性文字。\n\n"

            f"【源数据库类型】{state.source_format}\n"
            f"【目标数据库类型 / 数仓规范】{state.destination_format}\n"
            +(f"【所有schema名称修改为】{state.target_schema}\n" if state.target_schema else "")+

            "【用户定义的转换规则（最高优先级，必须逐条遵循）】\n"
            f"{state.general_prompt}\n\n"

            "【示例参考说明（用于找不同与抽象规则）】\n"
            "以下示例用于帮助你通过“对比源表与目标表”的方式，总结字段、类型与结构上的变化规律。\n"
            "示例并非本次转换的输入数据，而是用于你理解以下问题：\n"
            "- 哪些字段是源表的业务字段；\n"
            "- 哪些字段是目标数仓规范中统一引入的审计 / 技术字段；\n"
            "- 哪些源端特性在数仓建表中被明确移除；\n"
            "- 目标数仓建表 SQL 在整体结构与风格上的规范。\n\n"
            "在对比示例时请注意：\n"
            "- 示例中出现的审计字段，属于数仓规范驱动的变化，而非源表字段推导；\n"
            "- 是否需要在真实任务中新增此类字段，必须以【用户规则或目标模板】为依据；\n"
            "- 示例仅用于总结模式，不得被直接复用到最终输出中。\n\n"

            + (f"【源数据库建表 SQL 示例】\n'''\n{state.source_sql[:10000]}\n'''\n\n"
               if state.source_sql else "")

            + (f"【目标数据库 / 数仓建表 SQL 示例】\n'''\n{state.destination_example[:10000]}\n'''\n\n"
               if state.destination_example else "")

            + "【最终输出要求】\n"
              "请只输出【一个完整、可直接用于单表 SQL 转换的通用子任务提示词】文本。\n"
              "最好能够给出一个典型示例。\n"
              "不要输出任何背景说明、分析过程或与提示词无关的内容。\n"
    )

    result: Prompt = await llm.ainvoke(prompt)

    return {"general_prompt": result.prompt}


async def chunk_sql(state: MainState):
    chunked_sql = utils.split_sql(state.source_sql)
    return {"chunked_sql": chunked_sql}


async def send_tasks(state: MainState):
    chunk_states = [
        ChunkState(
            **state.model_dump(),
            sql=sql,
            limiter=CONFIG.MAX_TRY
        )
        for sql in state.chunked_sql]

    tasks = [chunk_graph.start_or_resume(i) for i in chunk_states]

    result = await tqdm.gather(*tasks, desc="Transferring sqls: ", total=len(tasks))

    return {"result": "".join(result)}

# async def final_join(state: MainState):
#     state.result_chunks.sort(key=lambda x: x.id)
#     rs="".join(i.sql for i in state.result_chunks)
#     return {"sqls":rs}
