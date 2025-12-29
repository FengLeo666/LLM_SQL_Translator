import asyncio
import os
import warnings

import CONFIG
from graph import main_graph
import utils
from states.main_state import MainState

if __name__=="__main__":
    sql_file_path = r"resources/sqls/其他备份建表-gbase 8C/gbase 8C备份建表语句-STG/gbase8c建表（财务税务）.txt"
    destination_sql_example_path= r"resources/sqls/example-gbase hd/STG层建表HD--产业协同供需（错误表、结果表、error表）.sql"
    source_format = "gbase8c"
    destination_format = "gbasehd"
    target_schema="stg_cwsw"
    general_prompt = f"""
    请将输入中的 gbase8c 建表 SQL 转换为符合 gbasehd（Hive / STG）数仓模板规范的建表语句，规则如下：

    【一、处理范围与顺序】
    1. 输入通常包含 3 类表：清洗表（主表）、错误表（_error）、临时表（_tmp），请按输入 SQL 的原始顺序逐表输出对应建表语句。
    2. 每张表字段顺序必须与源表 CREATE TABLE 中的字段顺序完全一致，不得重排。

    【二、Hive 建表模板（强制）】
    3. 字段定义统一为：`字段名 类型 COMMENT '注释'`，字段注释需整合源表中的行注释或 COMMENT ON COLUMN()。
    4. 每张表必须包含表注释：`COMMENT '...'` 用于注释表的用处（来源于 COMMENT ON TABLE 或表语义判断）。
    5. 每张表必须新增字段：
       - `col_batch string COMMENT '处理批次'`（gbasehd 数仓强制字段）。
    6. 存储与行格式必须严格使用以下模板，不得省略或替换：
       - `ROW FORMAT DELIMITED`
       - `FIELDS TERMINATED BY '\\u0001'`
       - `STORED AS TEXTFILE`

    【三、库名与表名规范】
    7. 目标 schema 统一为 `{target_schema}`，不沿用源 SQL 中的 schema。
    8. 表名保持与源表一致（包括 stg_ 前缀及 _error / _tmp 后缀），不得自行改名或合并。

    【四、类型映射规则】
    9. 按 Hive 语义进行类型映射：
       - varchar / char / bpchar / text → string
       - int2 / int4 → int
       - int8 / bigint → bigint
       - numeric / decimal(p,s) → decimal(p,s)
       - timestamptz / timestamp → timestamp
       - date → date
       移除 Hive 不支持的修饰（如 NOT NULL、DEFAULT、COLLATE、类型强转等）。

    【五、必须忽略的源端对象】
    10. 不输出任何约束或键（PRIMARY KEY / UNIQUE / FOREIGN KEY / CONSTRAINT），也不要据此生成分区表。
    11. 不输出索引、TABLESPACE、WITH(...) 等源端存储参数。
    12. COMMENT ON TABLE / COLUMN 必须转写为 Hive COMMENT 语法，不得原样保留。

    【六、输出要求】
    13. 仅输出最终 Hive 建表 SQL（DDL），不包含任何解释、说明或 Markdown。
    14. 每个 CREATE TABLE 语句以分号结束，可直接在 Hive 环境执行。
    """
    if CONFIG.GRAMMAR_CHECK:
        try:
            destination_sql_language=CONFIG.SQLGLOT_DIALECT_MAP[destination_format]
        except KeyError:
            warnings.warn(f"Unsupported sql checker format: {destination_format}, grammar check not available.")
            destination_sql_language=""

    with open(sql_file_path, "r", encoding="utf-8") as f:
        source_sql = f.read()

    if destination_sql_example_path:
        with open(destination_sql_example_path, "r", encoding="utf-8") as f:
            destination_sql = f.read()
    else:
        destination_sql = ""

    state=MainState(
        task_id=utils.task_id(general_prompt, source_format, destination_format, sql_file_path,
                              utils.stable_cache_key(source_sql)),
        general_prompt=general_prompt,
        source_format=source_format,
        destination_format=destination_format,
        destination_sql_language=destination_sql_language,
        source_sql=source_sql,
        destination_example=destination_sql,
    )

    result_sql=asyncio.run(main_graph.start_or_resume(state))

    # ===== 文件名生成逻辑 =====
    base_dir = os.path.dirname(sql_file_path)
    base_name = os.path.splitext(os.path.basename(sql_file_path))[0]

    os.makedirs("results", exist_ok=True)
    destination_file_path = os.path.join(
        "results",
        f"{base_name}_to_{destination_format}.sql"
    )

    # ===== 保存结果 =====
    with open(destination_file_path, "w", encoding="utf-8") as f:
        f.write(result_sql)

    print(f"转换完成，结果已保存至：{destination_file_path}")
