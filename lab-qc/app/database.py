"""SQLite 连接与表结构。

设计要点：
* WAL 模式 + busy_timeout，让后台核查的长读事务不阻塞入库写事务；
* 所有入库写操作在单个事务内完成，保证整批原子性；
* 生效规则通过 ``rules.is_active`` 唯一约束保证同一时刻只有一条。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(os.environ.get("LAB_QC_DB", "/data/lab_qc.db"))

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    is_active   INTEGER NOT NULL DEFAULT 0,
    note        TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS rules_one_active
    ON rules(is_active) WHERE is_active = 1;

CREATE TABLE IF NOT EXISTS rule_limits (
    rule_id    INTEGER NOT NULL REFERENCES rules(id),
    project    TEXT NOT NULL,
    lower_int  INTEGER NOT NULL,
    upper_int  INTEGER NOT NULL,
    PRIMARY KEY (rule_id, project),
    CHECK (lower_int <= upper_int)
);

CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id     INTEGER NOT NULL REFERENCES rules(id),
    filename    TEXT,
    row_count   INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id),
    rule_id     INTEGER NOT NULL REFERENCES rules(id),
    sample_no   TEXT NOT NULL,
    project     TEXT NOT NULL,
    reading     INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch_id);
CREATE INDEX IF NOT EXISTS idx_records_rule_project ON records(rule_id, project);

-- 同一时刻只允许存在一个未结束（checking/pending_switch）的替换
CREATE TABLE IF NOT EXISTS replacement_locks (
    id INTEGER PRIMARY KEY CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS replacements (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    new_rule_id           INTEGER NOT NULL REFERENCES rules(id),
    old_rule_id           INTEGER REFERENCES rules(id),
    status                TEXT NOT NULL CHECK (
                              status IN ('checking','pending_switch',
                                         'switched','failed')),
    failure_reason        TEXT,
    snapshot_max_record   INTEGER,
    violation_count       INTEGER NOT NULL DEFAULT 0,
    violations_json       TEXT,
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at           TEXT
);
CREATE INDEX IF NOT EXISTS idx_replacements_status ON replacements(status);
"""


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path is not None else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection):
    """显式事务上下文：异常回滚，正常提交。连接本身为 autocommit 模式。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def recover_unfinished(conn: sqlite3.Connection) -> list[int]:
    """启动恢复：未完成的替换一律标记失败，生效规则保持不变。"""
    init_db(conn)
    with transaction(conn):
        rows = conn.execute(
            "SELECT id FROM replacements WHERE status IN ('checking','pending_switch')"
        ).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            conn.execute(
                "UPDATE replacements SET status='failed', "
                "failure_reason='进程重启，替换中断，按失败处理', "
                "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE status IN ('checking','pending_switch')"
            )
            conn.execute("DELETE FROM replacement_locks WHERE id=1")
    return ids
