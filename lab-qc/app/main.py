"""环境实验室仪器读数入仓校验 API。

启动方式（容器内）：uvicorn app.main:app --host 0.0.0.0 --port 8000
交互文档：http://localhost:8000/docs
"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from . import services
from .database import DB_PATH, connect, recover_unfinished
from .models import BatchOut, HealthOut, RecordOut, ReplacementOut, RuleIn, RuleOut
from .services import ApiError, get_active_rule, get_rule

_recovered: list[int] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动恢复：上次未完成的替换一律标失败，已入库记录与生效规则原样保留
    global _recovered
    conn = connect()
    try:
        _recovered = recover_unfinished(conn)
    finally:
        conn.close()
    yield


app = FastAPI(
    title="环境实验室入仓质控校验 API",
    version="1.0.0",
    description=(
        "上传仪器 CSV（样品号、项目、整数读数），按当前生效规则（含端点整数闭区间）"
        "整批接收或拒绝；规则替换在后台核查历史数据，期间继续按旧规则接收与查询，"
        "切换时全部已提交记录合格才启用新规则，否则保留旧规则并返回违规样品。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "details": exc.details},
    )


@app.get("/health", response_model=HealthOut, tags=["系统"])
def health():
    conn = connect()
    try:
        active = get_active_rule(conn)
        return {
            "status": "ok",
            "active_rule_id": active["id"] if active else None,
            "unfinished_replacements_recovered": _recovered,
        }
    finally:
        conn.close()


# ---------- 规则 ----------

@app.post("/rules", response_model=RuleOut, status_code=201, tags=["规则"])
def submit_first_rule(payload: RuleIn):
    """提交首版规则。已有生效规则时必须走替换接口。"""
    conn = connect()
    try:
        rule_id = services.create_rule(conn, payload, active=True)
        return services.serialize_rule(conn, get_rule(conn, rule_id))
    finally:
        conn.close()


@app.get("/rules/active", response_model=RuleOut, tags=["规则"])
def current_active_rule():
    """查询当前生效规则（替换核查期间仍返回旧规则）。"""
    conn = connect()
    try:
        active = get_active_rule(conn)
        if active is None:
            raise ApiError(404, "尚无生效规则")
        return services.serialize_rule(conn, active)
    finally:
        conn.close()


@app.get("/rules/{rule_id}", response_model=RuleOut, tags=["规则"])
def get_rule_by_id(rule_id: int):
    conn = connect()
    try:
        row = get_rule(conn, rule_id)
        if row is None:
            raise ApiError(404, f"规则 {rule_id} 不存在")
        return services.serialize_rule(conn, row)
    finally:
        conn.close()


# ---------- 规则替换 ----------

@app.post("/rules/replacements", response_model=ReplacementOut, status_code=202,
          tags=["规则替换"])
def create_replacement(payload: RuleIn):
    """发起一次规则替换：立即返回，历史核查在后台线程执行，不暂停上传。"""
    conn = connect()
    try:
        replacement_id = services.start_replacement(conn, payload)
        row = services.get_replacement(conn, replacement_id)
        result = services.serialize_replacement(row)
    finally:
        conn.close()

    thread = threading.Thread(
        target=services.run_replacement_worker,
        args=(replacement_id, DB_PATH),
        name=f"replacement-{replacement_id}",
        daemon=True,
    )
    thread.start()
    return result


@app.get("/rules/replacements/{replacement_id}", response_model=ReplacementOut,
         tags=["规则替换"])
def replacement_status(replacement_id: int):
    """查询替换状态：checking / switched / failed（含违规样品明细）。"""
    conn = connect()
    try:
        row = services.get_replacement(conn, replacement_id)
        if row is None:
            raise ApiError(404, f"替换单 {replacement_id} 不存在")
        return services.serialize_replacement(row)
    finally:
        conn.close()


@app.get("/rules/replacements", response_model=list[ReplacementOut], tags=["规则替换"])
def replacements(limit: int = Query(default=50, ge=1, le=200)):
    conn = connect()
    try:
        return [services.serialize_replacement(r) for r in
                services.list_replacements(conn, limit)]
    finally:
        conn.close()


# ---------- 上传与查询 ----------

@app.post("/uploads", tags=["入库"])
async def upload_csv(file: UploadFile = File(..., description="仪器 CSV：表头 样品号,项目,读数")):
    """整批校验入库：全部符合当前生效规则才写入，随批保存所用规则号；否则整批拒收。"""
    raw = await file.read()
    conn = connect()
    try:
        result = services.ingest_batch(conn, raw, file.filename)
        return result
    finally:
        conn.close()


@app.get("/batches", response_model=list[BatchOut], tags=["入库"])
def list_batches(rule_id: int | None = Query(default=None),
                 limit: int = Query(default=100, ge=1, le=500)):
    """查询入库批次（可按校验时使用的规则号过滤）。"""
    conn = connect()
    try:
        if rule_id is not None:
            rows = conn.execute(
                "SELECT * FROM batches WHERE rule_id=? ORDER BY id DESC LIMIT ?",
                (rule_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [{
            "batch_id": r["id"], "rule_id": r["rule_id"],
            "filename": r["filename"], "row_count": r["row_count"],
            "created_at": r["created_at"],
        } for r in rows]
    finally:
        conn.close()


@app.get("/records", response_model=list[RecordOut], tags=["入库"])
def list_records(batch_id: int | None = Query(default=None),
                 project: str | None = Query(default=None),
                 limit: int = Query(default=200, ge=1, le=1000)):
    """查询已入库记录。"""
    conn = connect()
    try:
        sql = "SELECT * FROM records WHERE 1=1"
        params: list = []
        if batch_id is not None:
            sql += " AND batch_id=?"
            params.append(batch_id)
        if project is not None:
            sql += " AND project=?"
            params.append(project)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()
