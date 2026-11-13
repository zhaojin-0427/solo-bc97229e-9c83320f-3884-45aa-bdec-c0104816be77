"""端到端测试：覆盖整批原子性、缺规则拒收、替换成功/失败、并发与重启恢复。"""
from __future__ import annotations

import importlib
import os
import threading
import time
from pathlib import Path

import pytest

from app.models import RuleIn

# 每个测试使用独立数据库
TEST_DB = Path("/tmp/lab_qc_test/test_%d.db" % os.getpid())


@pytest.fixture()
def client(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("LAB_QC_DB", str(db_path))

    import app.database as database
    import app.services as services
    import app.main as main
    importlib.reload(database)
    importlib.reload(services)
    importlib.reload(main)

    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c, database, services, db_path


RULE_V1 = {
    "note": "首版规则",
    "limits": [
        {"project": "pH", "lower_int": 6, "upper_int": 9},
        {"project": "COD", "lower_int": 10, "upper_int": 300},
    ],
}
RULE_V2_GOOD = {
    "note": "收紧后仍兼容存量",
    "limits": [
        {"project": "pH", "lower_int": 6, "upper_int": 8},
        {"project": "COD", "lower_int": 10, "upper_int": 300},
    ],
}
RULE_V2_BAD = {
    "note": "pH 上限收紧到 7，历史有 8/9 必违规",
    "limits": [
        {"project": "pH", "lower_int": 6, "upper_int": 7},
        {"project": "COD", "lower_int": 10, "upper_int": 300},
    ],
}


def _csv(text: str) -> dict:
    return {"file": ("b.csv", text.encode("utf-8"), "text/csv")}


def _wait_replacement(client, rid, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/rules/replacements/{rid}")
        if r.json()["status"] in ("switched", "failed"):
            return r.json()
        time.sleep(0.02)
    raise AssertionError("替换未在超时内结束")


def test_no_rule_rejects_upload(client):
    c, *_ = client
    r = c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,7\n"))
    assert r.status_code == 409
    assert "尚无生效规则" in r.json()["error"]
    assert c.get("/records").json() == []


def test_first_rule_then_batch_accept_and_reject(client):
    c, *_ = client
    assert c.post("/rules", json=RULE_V1).status_code == 201
    # 第二次提交首版规则被拒绝，必须走替换
    assert c.post("/rules", json=RULE_V1).status_code == 409

    ok = "样品号,项目,读数\nS1,pH,7\nS2,COD,250\n"
    r = c.post("/uploads", files=_csv(ok))
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] is True and body["rule_id"] == 1 and body["row_count"] == 2

    # 边界 6、9 含端点，通过
    r = c.post("/uploads", files=_csv("样品号,项目,读数\nS3,pH,6\nS4,pH,9\n"))
    assert r.status_code == 200

    # 超界 + 缺规则项目同时出现：整批拒收，不留半批
    bad = "样品号,项目,读数\nS5,pH,10\nS6,NH3N,5\n"
    r = c.post("/uploads", files=_csv(bad))
    assert r.status_code == 422
    details = r.json()["details"]
    assert {d["project"] for d in details} == {"pH", "NH3N"}
    assert c.get("/batches").json().__len__() == 2  # 仍是前两个合格批次


def test_replacement_success_switches_rule_and_keeps_upload_available(client):
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,8\nS2,COD,100\n"))

    # 用屏障阻塞后台核查，制造“替换进行中”窗口，验证期间仍按旧规则入库
    barrier = threading.Event()
    original = services._find_violations

    def slow_find(conn, rule_id, after=0):
        if after == 0:
            barrier.wait(5)
        return original(conn, rule_id, after)

    services._find_violations = slow_find
    try:
        r = c.post("/rules/replacements", json=RULE_V2_GOOD)
        assert r.status_code == 202
        rid = r.json()["id"]
        time.sleep(0.1)  # 确保后台线程已进入核查
        # 核查期间查询/上传仍走旧规则；选新旧规则都合格的读数 pH=8，
        # 这样切换事务的 tail check 也能通过
        assert c.get("/rules/active").json()["rule_id"] == 1
        up = c.post("/uploads", files=_csv("样品号,项目,读数\nS3,pH,8\n"))
        assert up.status_code == 200 and up.json()["rule_id"] == 1
        barrier.set()
    finally:
        services._find_violations = original

    result = _wait_replacement(c, rid)
    assert result["status"] == "switched"
    assert c.get("/rules/active").json()["rule_id"] == 2

    # 新规则生效：pH=9 不再合格
    r = c.post("/uploads", files=_csv("样品号,项目,读数\nS4,pH,9\n"))
    assert r.status_code == 422
    r = c.post("/uploads", files=_csv("样品号,项目,读数\nS5,pH,8\n"))
    assert r.status_code == 200 and r.json()["rule_id"] == 2

    # 所有批次都带着提交时生效的规则号
    batches = {b["batch_id"]: b["rule_id"] for b in c.get("/batches").json()}
    assert sorted(batches.values()) == [1, 1, 2]


def test_replacement_failure_keeps_old_rule_and_lists_violations(client):
    c, *_ = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,8\nS2,pH,6\nS3,COD,50\n"))

    r = c.post("/rules/replacements", json=RULE_V2_BAD)
    rid = r.json()["id"]
    result = _wait_replacement(c, rid)
    assert result["status"] == "failed"
    assert result["violation_count"] == 1
    assert result["violations"][0]["sample_no"] == "S1"
    assert "保留旧规则" in result["failure_reason"]

    # 旧规则仍是生效规则，上传照常
    assert c.get("/rules/active").json()["rule_id"] == 1
    assert c.post("/uploads", files=_csv("样品号,项目,读数\nS4,pH,9\n")).status_code == 200


def test_only_one_replacement_at_a_time(client):
    c, database, services, _ = client
    c.post("/rules", json=RULE_V1)
    barrier = threading.Event()
    services._find_violations = lambda *a, **k: (barrier.wait(5), [])[1]
    try:
        first = c.post("/rules/replacements", json=RULE_V2_GOOD).json()
        time.sleep(0.1)
        r = c.post("/rules/replacements", json=RULE_V2_GOOD)
        assert r.status_code == 409
        barrier.set()
        assert _wait_replacement(c, first["id"])["status"] == "switched"
    finally:
        # 恢复原函数，避免污染同进程后续测试
        importlib.reload(services)


def test_concurrent_uploads_validate_against_committed_rule_and_no_half_batches(client):
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    errors: list = []
    results: list = []

    def worker(idx):
        from fastapi.testclient import TestClient
        local = TestClient(main_app)
        try:
            # 一半批次含 pH=10（旧规则即违规），一半合格
            if idx % 2:
                text = "样品号,项目,读数\n" + "".join(
                    f"S{idx}-{i},pH,10\n" for i in range(5))
            else:
                text = "样品号,项目,读数\n" + "".join(
                    f"S{idx}-{i},pH,7\n" for i in range(5))
            r = local.post("/uploads", files=_csv(text))
            results.append((idx, r.status_code))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    import app.main as main
    main_app = main.app
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    accepted = [x for x in results if x[1] == 200]
    rejected = [x for x in results if x[1] == 422]
    assert len(accepted) == 4 and len(rejected) == 4

    # 无半批：记录数 = 合格批次数 × 5，且全部关联规则 1
    records = c.get("/records?limit=1000").json()
    assert len(records) == 20
    assert {r["rule_id"] for r in records} == {1}
    assert len(c.get("/batches").json()) == 4


def test_restart_marks_unfinished_replacement_failed_and_preserves_data(client):
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,7\n"))

    # 手工插入一个“进程中断时仍在 checking”的替换
    conn = database.connect(db_path)
    try:
        with database.transaction(conn):
            conn.execute("INSERT INTO replacement_locks(id) VALUES (1)")
            new_id = services.insert_rule(
                conn, RuleIn.model_validate(RULE_V2_GOOD), active=False)
            conn.execute(
                "INSERT INTO replacements(new_rule_id, old_rule_id, status, snapshot_max_record) "
                "VALUES (?, 1, 'checking', 1)", (new_id,))
            rid = conn.execute("SELECT MAX(id) m FROM replacements").fetchone()["m"]
    finally:
        conn.close()

    # 模拟重启：新进程执行启动恢复
    conn2 = database.connect(db_path)
    try:
        recovered = database.recover_unfinished(conn2)
    finally:
        conn2.close()
    assert recovered == [rid]

    rep = c.get(f"/rules/replacements/{rid}").json()
    assert rep["status"] == "failed" and "重启" in rep["failure_reason"]
    assert c.get("/rules/active").json()["rule_id"] == 1  # 生效规则保留
    assert len(c.get("/records").json()) == 1             # 已入库记录保留

    # 恢复后可以重新发起替换
    r = c.post("/rules/replacements", json=RULE_V2_GOOD)
    assert r.status_code == 202
    assert _wait_replacement(c, r.json()["id"])["status"] == "switched"


def test_tail_violation_during_switch_fails_and_keeps_old_rule(client):
    c, database, services, _ = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,7\n"))  # 新规则下也合格

    original = services._find_violations

    def patched_find(conn, rule_id, after=0):
        if after > 0:  # 切换事务内的 tail check：模拟核查期间混入新规则下的违规记录
            return [{
                "record_id": 99, "batch_id": 9, "sample_no": "SX",
                "project": "pH", "reading": 9,
                "reason": "读数 9 超出新范围 [6, 8]",
            }]
        return original(conn, rule_id, after)

    # 同时让 count 返回非零
    services._find_violations = patched_find
    services._count_violations = lambda conn, rid, after=0: 1 if after > 0 else 0
    try:
        r = c.post("/rules/replacements", json=RULE_V2_GOOD)
        result = _wait_replacement(c, r.json()["id"])
        assert result["status"] == "failed"
        assert "核查期间新入库" in result["failure_reason"]
        assert c.get("/rules/active").json()["rule_id"] == 1
    finally:
        importlib.reload(services)
