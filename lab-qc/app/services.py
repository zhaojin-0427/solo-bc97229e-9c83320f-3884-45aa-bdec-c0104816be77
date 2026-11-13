"""业务逻辑：规则提交、CSV 整批入库、规则替换与后台核查。"""
from __future__ import annotations

import csv
import io
import json
import sqlite3
import threading
from pathlib import Path

from .database import connect, transaction
from .models import RuleIn

# 违规明细最多保留条数（总数始终完整返回）
MAX_VIOLATION_DETAIL = 1000

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
    """事务化地插入规则，供接口层使用。"""
    with transaction(conn):
        return insert_rule(conn, payload, active)


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
    """按提交时生效规则校验，整批通过才写入；任一不合格则全部不写入。"""
    records = parse_csv(raw)

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


# ---------- 规则替换 ----------

def start_replacement(conn: sqlite3.Connection, payload: RuleIn) -> int:
    """创建新规则（inactive）与 checking 替换单；只允许一个在途替换。"""
    with transaction(conn):
        active = get_active_rule(conn)
        if active is None:
            raise ApiError(409, "尚无生效规则，请先用 POST /rules 提交首版规则")
        busy = conn.execute(
            "SELECT 1 FROM replacements WHERE status IN ('checking','pending_switch')"
        ).fetchone()
        if busy is not None:
            raise ApiError(409, "已有一个规则替换在进行中，同一时间只允许一次替换")
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
        replacement_id = cur.lastrowid
    return replacement_id


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


def run_replacement_worker(replacement_id: int, db_path: Path) -> None:
    """后台线程：先做不占写事务的历史核查，通过后再进入单次切换事务。"""
    conn = connect(db_path)
    try:
        rep = conn.execute(
            "SELECT * FROM replacements WHERE id=?", (replacement_id,)
        ).fetchone()
        if rep is None or rep["status"] != "checking":
            return
        new_rule_id = rep["new_rule_id"]
        snapshot = rep["snapshot_max_record"]

        # 1) 后台只读核查：独立事务，不阻塞并发上传写入
        conn.execute("BEGIN")
        try:
            violations = _find_violations(conn, new_rule_id, 0)
            total = _count_violations(conn, new_rule_id, 0)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        if violations:
            with transaction(conn):
                conn.execute(
                    "UPDATE replacements SET status='failed', failure_reason=?, "
                    "violation_count=?, violations_json=?, "
                    "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    ("历史核查发现违规记录，保留旧规则", total,
                     json.dumps(violations, ensure_ascii=False), replacement_id),
                )
                conn.execute("DELETE FROM replacement_locks WHERE id=1")
            return

        # 2) 快照内全部合格，进入切换事务：
        #    复查快照之后新提交的记录（tail check），全部合格才原子切换
        with transaction(conn):
            tail = _count_violations(conn, new_rule_id, snapshot)
            if tail:
                detail = _find_violations(conn, new_rule_id, snapshot)
                conn.execute(
                    "UPDATE replacements SET status='failed', failure_reason=?, "
                    "violation_count=?, violations_json=?, "
                    "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    ("切换时复查发现核查期间新入库的违规记录，保留旧规则", tail,
                     json.dumps(detail, ensure_ascii=False), replacement_id),
                )
                conn.execute("DELETE FROM replacement_locks WHERE id=1")
                return

            old = conn.execute(
                "SELECT old_rule_id FROM replacements WHERE id=?", (replacement_id,)
            ).fetchone()["old_rule_id"]
            if old is not None:
                conn.execute("UPDATE rules SET is_active=0 WHERE id=?", (old,))
            conn.execute("UPDATE rules SET is_active=1 WHERE id=?", (new_rule_id,))
            conn.execute(
                "UPDATE replacements SET status='switched', "
                "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (replacement_id,),
            )
            conn.execute("DELETE FROM replacement_locks WHERE id=1")
    except Exception as exc:  # 线程兜底：任何中断都不得让替换悬挂
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE replacements SET status='failed', failure_reason=?, "
                    "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                    "WHERE id=? AND status IN ('checking','pending_switch')",
                    (f"核查/切换异常: {exc!r}", replacement_id),
                )
                conn.execute("DELETE FROM replacement_locks WHERE id=1")
        except Exception:
            pass
    finally:
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
