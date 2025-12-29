import operator
from typing import Dict, List, Annotated

from langchain_core.messages import ToolCall
from pydantic import BaseModel, Field


class ChunkResult(BaseModel):
    sql: str = Field(default_factory=str, description="转换后的结果")



class MainState(BaseModel):
    task_id: str = Field(description="用于恢复任务")
    general_prompt: str = Field(description="每次请求LLM会带的语句")
    source_format: str = Field(description="源sql数据类型")
    destination_format: str = Field(description="目标sql数据类型(数仓规范)")
    destination_sql_language:str=Field(default_factory=str,description="目标数据库语言。")
    source_sql: str = Field(default_factory=str,description="原始sql语句")
    target_schema:str = Field(default_factory=str,description="修改库名为...，如果为空不修改")
    destination_example: str = Field(default_factory=str, description="目标格式示例")
    chunked_sql: List[str] = Field(default_factory=list, description="分片后的sql，按表定义分")
    result_chunks: List[ChunkResult] = Field(default_factory=list, description="结果sql，按表定义分片")
    result: str = Field(default_factory=str, description="最后输出的sql语句")
    merge_n:int = Field(default=1,description="几个分片合并为一个分片")



class ChunkState(MainState, ChunkResult):
    exception:str=Field(default_factory=str, description="解析的错误")
    limiter: int = Field(default=1,description="剩余尝试次数")
