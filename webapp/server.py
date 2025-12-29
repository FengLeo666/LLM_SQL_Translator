"""
Backend Web API for the SQL chunk translator.

Run:
  pip install -r requirements-web.txt
  export DASHSCOPE_API_KEY="..."          # required if CONFIG.API_KEY is empty
  uvicorn webapp.server:app --host 0.0.0.0 --port 8000 --reload

Then open:
  http://localhost:8000/
"""

from __future__ import annotations


from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette import status

import CONFIG
from utils import checkpointer_pool,singleflight
import utils
from graph import chunk_graph
from method import main_method
from states.main_state import ChunkState, ChunkResult, MainState

app = FastAPI(title="LLM SQL Chunk Translator", lifespan=checkpointer_pool.lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse("webapp/static/index.html")


# Serve any additional static assets if needed in the future.
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")

@app.post("/api/convert_chunk", response_model=ChunkResult)
@singleflight#必须在内层
async def convert_chunk(req: ChunkState) -> ChunkResult:
    # Compute sqlglot dialect for validation (optional feature).
    dst_lang = (req.destination_sql_language or "").strip()
    if not dst_lang and CONFIG.GRAMMAR_CHECK:
        dst_lang = CONFIG.SQLGLOT_DIALECT_MAP.get(req.destination_format.lower(), "")

    limiter = CONFIG.MAX_TRY

    # Build a stable-ish task_id for checkpoint thread_id.
    # We intentionally include prompt/sql hashes so that re-runs with different prompts don't accidentally reuse old checkpoints.
    task_id = utils.task_id(req.source_format, req.destination_format, req.general_prompt, req.sql)

    state = ChunkState(
        task_id=task_id,
        general_prompt=req.general_prompt,
        source_format=req.source_format,
        destination_format=req.destination_format,
        destination_sql_language=dst_lang,
        source_sql="",              # not used by chunk graph but required by schema
        destination_example="",     # not used by chunk graph but required by schema
        sql=req.sql,
        limiter=limiter,
    )

    try:
        out_sql = await chunk_graph.start_or_resume(state)
        if not out_sql:
            raise RuntimeError("API 模型错误")

        req.sql = out_sql
        return req

    except Exception as e:
        # ❗关键：抛 HTTPException，而不是 return
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Chunk 转换失败",
                "exception": str(e),
                "task_id": task_id,
            },
        )

@app.post("/api/normalize_prompt", response_model=MainState)
async def normalize_prompt(req: MainState) -> MainState:
    result=await main_method.prompt_normalize(req)
    req.general_prompt=result["general_prompt"]
    return req
