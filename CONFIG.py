from __future__ import annotations
import os

# Optional .env support (useful for local development and docker-compose).
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    # Do not hard-fail if python-dotenv is not installed.
    pass


# OpenAI-compatible endpoint (DashScope compatible-mode by default).
# Override by setting env var: API_BASE
API_BASE = os.getenv("API_BASE", "https://dashscope.aliyuncs.com/compatible-mode/v1")

# Never hard-code secrets; read from env.
# Preferred env var: DASHSCOPE_API_KEY
API_KEY = os.getenv("DASHSCOPE_API_KEY") or os.getenv("API_KEY")

LLM_TYPE = os.getenv("LLM_TYPE","qwen3-max")

assert API_KEY is not None ,"API_KEY Required"


LLM_RPM = float(os.getenv("LLM_RPM",88))
MAX_TRY=int(os.getenv("MAX_TRY",3))

TIME_WARN=0



# Concurrency for per-chunk processing (platform-level setting)
MAX_CONCURRENCY = 6



RESOURCES_DIR = "resources"
os.makedirs(RESOURCES_DIR, exist_ok=True)





GRAMMAR_CHECK=True
SQLGLOT_DIALECT_MAP = {
    # ===== GBase 系列 =====
    "gbase8c": "postgres",      # GBase 8c 语法高度接近 PostgreSQL
    "gbase_8c": "postgres",
    "gbase 8c": "postgres",

    "gbasehd": "hive",          # GBase HD 本质是 Hive / Spark SQL 规范
    "gbase_hd": "hive",
    "gbase hd": "hive",

    # ===== Hive / 大数据 =====
    "hive": "hive",
    "hiveql": "hive",
    "spark": "spark",
    "spark_sql": "spark",
    "databricks": "databricks",
    "presto": "presto",
    "trino": "trino",

    # ===== 关系型数据库 =====
    "postgres": "postgres",
    "postgresql": "postgres",
    "pgsql": "postgres",

    "mysql": "mysql",
    "oracle": "oracle",
    "sqlite": "sqlite",
    "sqlserver": "",            # sqlglot 没有专门 dialect，走通用
    "mssql": "",

    # ===== 云数仓 / MPP =====
    "clickhouse": "clickhouse",
    "doris": "doris",
    "starrocks": "starrocks",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",

    # ===== 兜底 =====
    "default": "",              # sqlglot 默认通用方言
}