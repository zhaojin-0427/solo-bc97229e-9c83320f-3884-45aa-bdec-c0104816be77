"""业务逻辑：规则提交、CSV 整批入库、规则替换与后台核查。

写锁韧性设计：
* 所有收尾写事务（标失败/切换/释放互斥）经 ``run_with_write_retry`` 在撞锁时
  整体回滚重试，外部写锁释放后自动完成，不靠重启或人工清理；
* 每个替换工作线程在 ``runtime.workers`` 登记；若线程意外死亡且收尾未完成，
  看门狗线程（或下一次替换请求）会以事务方式把在途替换标失败并同时释放互斥；
* 收尾 SQL 全部带 ``status IN ('checking','pending_switch')`` 条件且幂等，
  只有条件 UPDATE 命中时才删除互斥行，迟到收尾无法改写后续替换或释放其互斥。
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import threading
import time
from pathlib import Path

from . import runtime
from .database import connect, run_with_write_retry, transaction
from .models import RuleIn

# 违规明细最多保留条数（总数始终完整返回）
MAX_VIOLATION_DETAIL = 1000

# 接口路径（发起替换/上传）撞写锁时的有限重试次数，耗尽后返回 503
WRITE_LOCK_MAX_ATTEMPTS = int(os.environ.get("LAB_QC_WRITE_LOCK_ATTEMPTS", "3"))
# 看门狗巡检周期与无限重试间隔（秒），可用环境变量调小加速测试
WATCHDOG_INTERVAL_S = float(os.environ.get("LAB_QC_WATCHDOG_INTERVAL", "0.2"))

HEADER_ALIASES = {
    "sample_no": "sample_no", "sample": "sample_no", "sample id": "sample_no",
    "样品号": "sample_no", "样品编号": "sample_no", "样品": "sample_no",
    "project": "project", "item": "project",
    "项目": "project", "检测项目": "project",
    "reading": "reading", "value": "reading",
    "读数": "reading", "整数读数": "reading", "仪器读数": "reading",
}


class ApiError(Exception):
    def __init__(self, status_code: int, message: str, details: list | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.details = details or []


# ---------- 替换工作线程登记与看门狗 ----------
# 线程登记与看门狗线程状态存放在不被重载的 app.runtime 中，保证 services 模块
# 重载（测试/热更新）后旧看门狗仍可被精确停止，不会泄漏堆积。


def _watchdog_loop(stop_event: threading.Event) -> None:
    """周期性为死亡线程的在途替换补收尾：标失败并在同一事务释放互斥。

    自身撞写锁时无限重试，外部连接释放写锁后即自动完成；等待期间只读查询
    （WAL 模式）与本巡检的状态读取始终保持响应。
    """
    while not stop_event.wait(WATCHDOG_INTERVAL_S):
        try:
            conn = connect()
            try:
                rows = conn.execute(
                    "SELECT id FROM replacements "
                    "WHERE status IN ('checking','pending_switch')"
                ).fetchall()
                inflight = [r["id"] for r in rows]
                if not inflight:
                    continue
                _, dead_ids = runtime.workers.snapshot_inflight(inflight)
                for rid in dead_ids:
                    _finalize_failure(
                        conn, rid,
                        "替换工作线程异常退出，由看门狗按失败处理并释放替换互斥",
                    )  # 内部无限重试写锁
                    runtime.workers.clear_dead(rid)
            finally:
                conn.close()
        except Exception:
            # 看门狗自身任何意外都不得停摆，下一个巡检周期继续
            time.sleep(WATCHDOG_INTERVAL_S)


def start_watchdog() -> None:
    """启动唯一的看门狗守护线程（线程与停止事件由 app.runtime 统一持有）。"""
    runtime.start_watchdog(_watchdog_loop)


def stop_watchdog() -> None:
    """停止看门狗并等待其完全退出（供应用关闭/测试拆卸）。"""
    runtime.stop_watchdog()


# 兼容别名：让外部（含测试）仍可经 services 访问共享运行态，
# 实际对象保存在不被重载的 app.runtime 中
_workers = runtime.workers


class _WatchdogProxy:
    """暴露测试需要的 is_alive 视图，指向 runtime 持有的真实线程。"""

    @property
    def _thread(self):
        with runtime._watchdog_lock:
            return runtime._watchdog_thread

    def is_alive(self) -> bool:
        t = self._thread
        return bool(t is not None and t.is_alive())


_watchdog_thread = _WatchdogProxy()


def settle_inflight(conn: sqlite3.Connection) -> None:
    """在发起新替换前处理在途单：存活则 409；死亡/无主则同步补收尾。

    若此时另有连接持写锁，本函数会阻塞重试直到锁释放，保证“解锁后原替换
    自动结束、无需重启或人工清理”。
    """
    rows = conn.execute(
        "SELECT id FROM replacements WHERE status IN ('checking','pending_switch')"
    ).fetchall()
    inflight = [r["id"] for r in rows]
    if not inflight:
        return
    alive, dead_ids = runtime.workers.snapshot_inflight(inflight)
    if alive:
        raise ApiError(409, "已有一个规则替换在进行中，同一时间只允许一次替换")
    for rid in dead_ids:
        _finalize_failure(
            conn, rid,
            "替换工作线程异常退出，由后续请求按失败处理并释放替换互斥",
        )
        runtime.workers.clear_dead(rid)


# ---------- 规则 ----------

def get_active_rule(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM rules WHERE is_active=1"
    ).fetchone()


def get_rule(conn: sqlite3.Connection, rule_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM rules WHERE id=?", (rule_id,)).fetchone()


def rule_limits(conn: sqlite3.Connection, rule_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT project, lower_int, upper_int FROM rule_limits "
        "WHERE rule_id=? ORDER BY project", (rule_id,)
    ).fetchall()


def active_limits_map(conn: sqlite3.Connection, rule_id: int) -> dict[str, tuple[int, int]]:
    return {
        r["project"]: (r["lower_int"], r["upper_int"])
        for r in rule_limits(conn, rule_id)
    }


def insert_rule(conn: sqlite3.Connection, payload: RuleIn, active: bool) -> int:
    """插入规则，不自行管理事务，由调用方保证处于事务中。"""
    if active and get_active_rule(conn) is not None:
        raise ApiError(409, "已存在生效规则，规则变更必须走替换接口 /rules/replacements")
    cur = conn.execute(
        "INSERT INTO rules(is_active, note) VALUES (?, ?)",
        (1 if active else 0, payload.note),
    )
    rule_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO rule_limits(rule_id, project, lower_int, upper_int) "
        "VALUES (?,?,?,?)",
        [(rule_id, x.project, x.lower_int, x.upper_int) for x in payload.limits],
    )
    return rule_id


def create_rule(conn: sqlite3.Connection, payload: RuleIn, active: bool) -> int:
    """事务化地插入规则，供接口层使用；撞写锁时有限重试。"""
    def _do(c):
        with transaction(c):
            return insert_rule(c, payload, active)

    return run_with_write_retry(conn, _do, max_attempts=WRITE_LOCK_MAX_ATTEMPTS)


# ---------- CSV 解析与整批入库 ----------

def parse_csv(raw: bytes) -> list[dict]:
    """解析上传内容。任何格式问题直接抛错，整批拒收。"""
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ApiError(422, "CSV 必须为 UTF-8 编码")
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader]
    if not rows:
        raise ApiError(422, "CSV 内容为空")

    header = [h.strip().strip("﻿") for h in rows[0]]
    cols: list[str] = []
    for h in header:
        key = HEADER_ALIASES.get(h.lower())
        if key is None:
            raise ApiError(422, f"无法识别的表头列: {h!r}，允许: 样品号/项目/读数 (sample_no/project/reading)")
        cols.append(key)
    if sorted(cols) != ["project", "reading", "sample_no"]:
        raise ApiError(422, "CSV 必须且只能包含 样品号、项目、读数 三列")

    records: list[dict] = []
    for line_no, row in enumerate(rows[1:], start=2):
        if not row or all(not c.strip() for c in row):
            continue  # 跳过空行
        if len(row) != 3:
            raise ApiError(422, f"第 {line_no} 行列数不是 3")
        sample_no, project, reading_s = (c.strip() for c in row)
        if not sample_no or not project:
            raise ApiError(422, f"第 {line_no} 行样品号/项目不能为空")
        try:
            reading = int(reading_s)
        except ValueError:
            raise ApiError(422, f"第 {line_no} 行读数不是整数: {reading_s!r}")
        records.append({
            "line_no": line_no, "sample_no": sample_no,
            "project": project, "reading": reading,
        })
    if not records:
        raise ApiError(422, "CSV 没有数据行")
    return records


def ingest_batch(conn: sqlite3.Connection, raw: bytes, filename: str | None) -> dict:
    """按提交时生效规则校验，整批通过才写入；任一不合格则全部不写入。

    撞外部写锁时按有限次数重试（整批事务回滚后重来，规则号在事务内读取，
    始终按提交时生效规则校验）；仍拿不到写锁则向上抛 locked 错误，由接口层
    返回 503，客户端可在锁释放后原样重传，不会留下半批。
    """
    records = parse_csv(raw)

    def _do(conn):
        with transaction(conn):
            active = get_active_rule(conn)  # 行锁：整个事务内规则不会被切换
            if active is None:
                raise ApiError(409, "尚无生效规则，请先提交首版规则 POST /rules")
            rule_id = active["id"]
            limits = active_limits_map(conn, rule_id)

            errors: list[dict] = []
            for rec in records:
                bound = limits.get(rec["project"])
                if bound is None:
                    errors.append({
                        "line_no": rec["line_no"], "sample_no": rec["sample_no"],
                        "project": rec["project"], "reading": rec["reading"],
                        "reason": "该项目缺少质控规则，拒收",
                    })
                    continue
                low, high = bound
                if not (low <= rec["reading"] <= high):  # 含端点
                    errors.append({
                        "line_no": rec["line_no"], "sample_no": rec["sample_no"],
                        "project": rec["project"], "reading": rec["reading"],
                        "reason": f"读数 {rec['reading']} 超出闭区间 [{low}, {high}]",
                    })
            if errors:
                raise ApiError(
                    422, f"整批拒收：{len(errors)} 条记录不符合规则 {rule_id}，未写入任何数据",
                    errors,
                )

            cur = conn.execute(
                "INSERT INTO batches(rule_id, filename, row_count) VALUES (?,?,?)",
                (rule_id, filename, len(records)),
            )
            batch_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO records(batch_id, rule_id, sample_no, project, reading) "
                "VALUES (?,?,?,?,?)",
                [(batch_id, rule_id, r["sample_no"], r["project"], r["reading"]) for r in records],
            )
            return {
                "accepted": True, "batch_id": batch_id, "rule_id": rule_id,
                "row_count": len(records),
            }

    return run_with_write_retry(conn, _do, max_attempts=WRITE_LOCK_MAX_ATTEMPTS)


# ---------- 规则替换 ----------

def start_replacement(conn: sqlite3.Connection, payload: RuleIn) -> int:
    """创建新规则（inactive）与 checking 替换单；只允许一个在途替换。

    发起前先 ``settle_inflight``：若上一个替换的工作线程已死亡（例如被外部
    写锁耗尽重试），先在此同步补收尾（标失败 + 释放互斥），随后即可新建替换，
    不需要重启或人工清理。
    """
    # 先在事务外补做死亡替换的收尾（其内部自带事务与写锁无限重试），
    # 避免与下方写事务嵌套；存活在途单在此直接 409
    settle_inflight(conn)

    def _do(conn):
        with transaction(conn):
            # 兜底：补收尾与本请求之间若又出现死亡在途单，仅做只读判定
            busy = conn.execute(
                "SELECT id FROM replacements "
                "WHERE status IN ('checking','pending_switch')"
            ).fetchall()
            if busy:
                raise ApiError(409, "已有一个规则替换在进行中，同一时间只允许一次替换")
            active = get_active_rule(conn)
            if active is None:
                raise ApiError(409, "尚无生效规则，请先用 POST /rules 提交首版规则")
            try:
                conn.execute("INSERT INTO replacement_locks(id) VALUES (1)")
            except sqlite3.IntegrityError:
                raise ApiError(409, "替换锁被占用，请稍后再试")

            new_rule_id = insert_rule(conn, payload, active=False)
            snapshot_max = conn.execute(
                "SELECT COALESCE(MAX(id),0) AS m FROM records"
            ).fetchone()["m"]
            cur = conn.execute(
                "INSERT INTO replacements(new_rule_id, old_rule_id, status, snapshot_max_record) "
                "VALUES (?,?, 'checking', ?)",
                (new_rule_id, active["id"], snapshot_max),
            )
            return cur.lastrowid

    return run_with_write_retry(conn, _do, max_attempts=WRITE_LOCK_MAX_ATTEMPTS)


def _find_violations(conn: sqlite3.Connection, rule_id: int,
                     after_record_id: int = 0) -> list[dict]:
    """按给定规则核查全部（或快照之后的）已提交记录，返回违规明细。"""
    sql = (
        "SELECT r.id, r.sample_no, r.project, r.reading, r.batch_id, "
        "       l.lower_int, l.upper_int "
        "FROM records r JOIN rule_limits l ON l.rule_id=? AND l.project=r.project "
        "WHERE r.id > ? AND r.reading NOT BETWEEN l.lower_int AND l.upper_int "
        "UNION ALL "
        "SELECT r.id, r.sample_no, r.project, r.reading, r.batch_id, "
        "       NULL, NULL "
        "FROM records r WHERE r.id > ? AND NOT EXISTS ("
        "     SELECT 1 FROM rule_limits l WHERE l.rule_id=? AND l.project=r.project) "
        "ORDER BY id LIMIT ?"
    )
    rows = conn.execute(
        sql, (rule_id, after_record_id, after_record_id, rule_id,
              MAX_VIOLATION_DETAIL + 1)
    ).fetchall()
    violations = []
    for row in rows[:MAX_VIOLATION_DETAIL]:
        if row["lower_int"] is None:
            reason = "新项目缺少质控规则"
        else:
            reason = f"读数 {row['reading']} 超出新范围 [{row['lower_int']}, {row['upper_int']}]"
        violations.append({
            "record_id": row["id"], "batch_id": row["batch_id"],
            "sample_no": row["sample_no"], "project": row["project"],
            "reading": row["reading"], "reason": reason,
        })
    return violations


def _count_violations(conn: sqlite3.Connection, rule_id: int,
                      after_record_id: int = 0) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM records r WHERE r.id > ? AND ("
        "  NOT EXISTS (SELECT 1 FROM rule_limits l WHERE l.rule_id=? AND l.project=r.project) "
        "  OR EXISTS (SELECT 1 FROM rule_limits l WHERE l.rule_id=? AND l.project=r.project "
        "             AND r.reading NOT BETWEEN l.lower_int AND l.upper_int))",
        (after_record_id, rule_id, rule_id),
    ).fetchone()
    return row["c"]


def _mark_failed(conn: sqlite3.Connection, replacement_id: int, reason: str,
                 total: int | None = None, violations: list[dict] | None = None) -> bool:
    """在当前事务内把在途替换标失败，并仅在命中时释放互斥。

    条件 ``status IN ('checking','pending_switch')`` 保证幂等：迟到收尾不会
    改写已经结束（switched/failed）的替换；互斥删除与状态更新同事务生效。
    返回是否命中（True 表示本次确实完成了收尾并持有互斥删除）。
    """
    cur = conn.execute(
        "UPDATE replacements SET status='failed', failure_reason=?, "
        "violation_count=COALESCE(?, violation_count), "
        "violations_json=COALESCE(?, violations_json), "
        "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
        "WHERE id=? AND status IN ('checking','pending_switch')",
        (reason, total,
         json.dumps(violations, ensure_ascii=False) if violations is not None else None,
         replacement_id),
    )
    if cur.rowcount == 0:
        # 已被并发收尾/后续替换结束：不得释放别人的互斥，不得改写别人的状态
        return False
    conn.execute("DELETE FROM replacement_locks WHERE id=1")
    return True


def _finalize_failure(conn: sqlite3.Connection, replacement_id: int, reason: str,
                      total: int | None = None,
                      violations: list[dict] | None = None) -> bool:
    """带写锁无限重试的失败收尾：外部锁释放后必然完成。"""
    def _do(c):
        with transaction(c):
            return _mark_failed(c, replacement_id, reason, total, violations)

    return run_with_write_retry(conn, _do, max_attempts=None)


def _do_switch(conn: sqlite3.Connection, replacement_id: int,
               new_rule_id: int, snapshot: int) -> str:
    """切换事务内的全部动作，返回 'switched' / 'failed' / 'late'。"""
    tail = _count_violations(conn, new_rule_id, snapshot)
    if tail:
        detail = _find_violations(conn, new_rule_id, snapshot)
        ok = _mark_failed(
            conn, replacement_id,
            "切换时复查发现核查期间新入库的违规记录，保留旧规则",
            tail, detail,
        )
        return "failed" if ok else "late"

    # 条件 UPDATE：只有仍在途的本替换才能完成切换，迟到收尾直接放弃
    cur = conn.execute(
        "UPDATE replacements SET status='pending_switch' WHERE id=? "
        "AND status IN ('checking','pending_switch')",
        (replacement_id,),
    )
    if cur.rowcount == 0:
        return "late"

    old = conn.execute(
        "SELECT old_rule_id FROM replacements WHERE id=?", (replacement_id,)
    ).fetchone()["old_rule_id"]
    if old is not None:
        conn.execute("UPDATE rules SET is_active=0 WHERE id=?", (old,))
    conn.execute("UPDATE rules SET is_active=1 WHERE id=?", (new_rule_id,))
    cur = conn.execute(
        "UPDATE replacements SET status='switched', "
        "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? "
        "AND status IN ('checking','pending_switch')",
        (replacement_id,),
    )
    if cur.rowcount == 0:  # pragma: no cover - 同事务内不可能被并发改写
        return "late"
    conn.execute("DELETE FROM replacement_locks WHERE id=1")
    return "switched"


def run_replacement_worker(replacement_id: int, db_path: Path) -> None:
    """后台线程：先做不占写事务的历史核查，通过后再进入单次切换事务。

    所有写收尾都带写锁重试：另一连接持续持写锁时，线程在此等待（状态保持
    checking、只读查询与上传校验不受影响），锁释放后自动完成失败登记或切换，
    并在同一事务释放替换互斥。若线程因任何意外死亡，看门狗会补做收尾。
    """
    runtime.workers.register(replacement_id)
    conn = connect(db_path)
    try:
        rep = conn.execute(
            "SELECT * FROM replacements WHERE id=?", (replacement_id,)
        ).fetchone()
        if rep is None or rep["status"] not in ("checking", "pending_switch"):
            return
        new_rule_id = rep["new_rule_id"]
        snapshot = rep["snapshot_max_record"]

        # 1) 后台只读核查：独立事务，不阻塞并发上传写入（WAL 读不阻塞写）
        conn.execute("BEGIN")
        try:
            violations = _find_violations(conn, new_rule_id, 0)
            total = _count_violations(conn, new_rule_id, 0)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        if violations:
            # 未提交切换：标失败并保留旧规则；写锁释放后自动完成
            _finalize_failure(
                conn, replacement_id,
                "历史核查发现违规记录，保留旧规则", total, violations,
            )
            return

        # 2) 快照内全部合格，进入切换事务：
        #    复查快照之后新提交的记录（tail check），全部合格才原子启用新规则。
        #    已提交则保留成功结果；撞写锁则整体回滚重试，绝不留半切换。
        def _switch(c):
            with transaction(c):
                return _do_switch(c, replacement_id, new_rule_id, snapshot)

        result = run_with_write_retry(conn, _switch, max_attempts=None)
        if result == "late":
            # 迟到收尾：替换已被其他路径结束，不触碰其状态与互斥
            return
    except Exception as exc:
        # 线程兜底：尝试带无限重试的失败收尾；仅当本替换仍在途时才生效
        try:
            _finalize_failure(conn, replacement_id, f"核查/切换异常: {exc!r}")
        except Exception:
            # 连收尾都无法完成（例如连接本身损坏）：交给看门狗/后续请求补做
            runtime.workers.mark_dead(replacement_id)
            return
    finally:
        runtime.workers.unregister(replacement_id)
        conn.close()


def get_replacement(conn: sqlite3.Connection, replacement_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM replacements WHERE id=?", (replacement_id,)
    ).fetchone()


def list_replacements(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM replacements ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


def serialize_replacement(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "new_rule_id": row["new_rule_id"],
        "old_rule_id": row["old_rule_id"],
        "status": row["status"],
        "failure_reason": row["failure_reason"],
        "snapshot_max_record": row["snapshot_max_record"],
        "violation_count": row["violation_count"],
        "violations": json.loads(row["violations_json"]) if row["violations_json"] else [],
        "created_at": row["created_at"],
        "finished_at": row["finished_at"],
    }


def serialize_rule(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    return {
        "rule_id": row["id"],
        "is_active": bool(row["is_active"]),
        "note": row["note"],
        "created_at": row["created_at"],
        "limits": [dict(r) for r in rule_limits(conn, row["id"])],
    }
