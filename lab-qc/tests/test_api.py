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
    # 测试环境调小写锁等待与看门狗巡检，加快锁冲突类用例
    monkeypatch.setenv("LAB_QC_BUSY_TIMEOUT", "300")
    monkeypatch.setenv("LAB_QC_WRITE_LOCK_ATTEMPTS", "3")
    monkeypatch.setenv("LAB_QC_WATCHDOG_INTERVAL", "0.05")

    # 先彻底停掉看门狗并等待上一个测试的替换工作线程退出。看门狗线程与停止
    # 事件由不被重载的 app.runtime 持有，因此这里总能精确停掉真实线程，
    # 避免旧线程在新测试的数据库上持锁/误收尾。
    from app import runtime
    runtime.stop_watchdog()
    for t in list(threading.enumerate()):
        if t.name.startswith("replacement-") and t is not threading.current_thread():
            t.join(timeout=10)
            assert not t.is_alive(), f"替换工作线程 {t.name} 未在超时内退出"

    import app.database as database
    import app.services as services
    import app.main as main
    # 注意：app.runtime 不参与重载，保持线程/登记等进程态唯一
    importlib.reload(database)
    importlib.reload(services)
    importlib.reload(main)

    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c, database, services, db_path
    # 拆卸：停看门狗并等待本测试的替换工作线程退出，避免污染下一个测试。
    # 各测试在 finally 中自行恢复打补丁的函数。
    services.stop_watchdog()
    for t in list(threading.enumerate()):
        if t.name.startswith("replacement-") and t is not threading.current_thread():
            t.join(timeout=10)
            assert not t.is_alive(), f"替换工作线程 {t.name} 拆卸后仍未退出"


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
# 与 V1 范围完全相同的新规则：任何按 V1 入库的记录在其下都合格，
# 专供写锁回归测试，消除“核查期间新记录导致 tail check 失败”的时序不确定性
RULE_V2_SAME = {
    "note": "范围不变的规则",
    "limits": [
        {"project": "pH", "lower_int": 6, "upper_int": 9},
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

    # 模拟进程中断：先停掉运行期看门狗（真实重启时新进程尚未启动看门狗），
    # 并确认线程已退出，避免它与启动恢复事务抢同一把写锁
    services.stop_watchdog()
    wd = services._watchdog_thread
    assert wd is None or not wd.is_alive(), "看门狗未能在测试前停止"

    conn = database.connect(db_path)
    try:
        # 手工插入一个“中断时仍在 checking”的替换
        with database.transaction(conn):
            conn.execute("INSERT INTO replacement_locks(id) VALUES (1)")
            new_id = services.insert_rule(
                conn, RuleIn.model_validate(RULE_V2_GOOD), active=False)
            conn.execute(
                "INSERT INTO replacements(new_rule_id, old_rule_id, status, snapshot_max_record) "
                "VALUES (?, 1, 'checking', 1)", (new_id,))
            rid = conn.execute("SELECT MAX(id) m FROM replacements").fetchone()["m"]
        # 看门狗已停：在途单必须仍然悬挂，证明下面的恢复确实由启动逻辑完成
        assert conn.execute(
            "SELECT status FROM replacements WHERE id=?", (rid,)
        ).fetchone()["status"] == "checking"
    finally:
        conn.close()

    # 模拟重启：新进程执行启动恢复（此时看门狗未运行，避免与恢复事务抢锁）
    conn2 = database.connect(db_path)
    try:
        recovered = database.recover_unfinished(conn2)
    finally:
        conn2.close()
    assert recovered == [rid]
    services.start_watchdog()  # 模拟新进程启动看门狗，供本测试后续替换使用

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


# ---------- 写锁韧性回归测试 ----------

class WorkerTxGate:
    """确定性拦截替换工作线程的每次写事务（条件变量会合，无等待/放行竞态）。

    工作线程每次进入写事务先在闸门前与测试会合（``handoff``），测试架好外部
    锁后 ``release``，线程才真正执行事务并在 SQLite busy handler 中等待；撞锁
    重试会再次会合。``restore()`` 后切换为直通，后续尝试不再拦截。
    """

    def __init__(self, services):
        self.services = services
        self.original = services.transaction
        self.attempts = 0
        self._cond = threading.Condition()
        self._armed = False
        self._opened = False
        services.transaction = self._gated

    def _gated(self, conn):
        if (not threading.current_thread().name.startswith("replacement-")) or self._opened:
            return self.original(conn)
        with self._cond:
            self.attempts += 1
            self._armed = True
            self._cond.notify_all()
            # 等待测试调用 release()（或 restore()）；短超时保证测试异常时
            # 工作线程不会被闸门长期挂住
            while self._armed and not self._opened:
                self._cond.wait(2)
        return self.original(conn)

    def handoff(self, expected_attempt, timeout=5):
        """阻塞直到工作线程到达第 expected_attempt 次写事务的闸门。"""
        deadline = time.time() + timeout
        with self._cond:
            while self.attempts < expected_attempt:
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cond.wait(remaining):
                    raise AssertionError(f"工作线程未在超时内到达第 {expected_attempt} 次写事务")

    def release(self):
        """放行当前正在闸门前等待的那一次尝试，让其进入写事务（撞锁则 busy 等待）。"""
        with self._cond:
            self._armed = False
            self._cond.notify_all()

    def restore(self):
        """永久直通：放行当前等待且后续尝试不再拦截。"""
        with self._cond:
            self._opened = True
            self._armed = False
            self._cond.notify_all()
        self.services.transaction = self.original


def test_two_consecutive_locks_auto_finalize_then_next_replacement_succeeds(client):
    """两次连续遇锁：解锁后原替换自动 switched，互斥释放，后续替换与上传恢复。"""
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,7\nS2,COD,100\n"))

    # 让后台线程停在只读核查完成、写切换事务之前
    barrier = threading.Event()
    original_find = services._find_violations

    def slow_find(conn, rule_id, after=0):
        if after == 0:
            barrier.wait(10)
        return original_find(conn, rule_id, after)

    services._find_violations = slow_find
    gate = WorkerTxGate(services)
    try:
        r = c.post("/rules/replacements", json=RULE_V2_SAME)
        assert r.status_code == 202
        rid1 = r.json()["id"]
        time.sleep(0.15)  # 确保线程已进入只读核查

        # —— 第一次遇锁：先让工作线程到达写事务闸门（此时还没碰锁）——
        barrier.set()
        gate.handoff(expected_attempt=1)
        locker1 = database.connect(db_path)
        locker1.execute("BEGIN IMMEDIATE")
        gate.release()  # 放行第 1 次：撞锁，回滚后重试，回到第 2 道闸门

        # 等待期间状态查询保持响应（WAL 读不阻塞写），替换始终 checking
        for _ in range(5):
            q = c.get(f"/rules/replacements/{rid1}")
            assert q.status_code == 200 and q.json()["status"] == "checking"
            assert c.get("/rules/active").status_code == 200
            assert c.get("/batches").status_code == 200
            time.sleep(0.05)

        # 上传写事务拿不到写锁：有限重试后明确 503，不留半批
        up = c.post("/uploads", files=_csv("样品号,项目,读数\nS3,pH,7\n"))
        assert up.status_code == 503
        batches_during_lock = len(c.get("/batches").json())

        # —— 解锁 1 后立刻第二次遇锁（线程已确定停在第 2 道闸门前）——
        gate.handoff(expected_attempt=2)
        locker1.execute("ROLLBACK")
        locker1.close()
        locker2 = database.connect(db_path)
        locker2.execute("BEGIN IMMEDIATE")
        gate.release()  # 放行第 2 次：再次撞锁，busy 等待后回到第 3 道闸门
        gate.handoff(expected_attempt=3)
        assert c.get(f"/rules/replacements/{rid1}").json()["status"] == "checking"
        assert c.get("/rules/active").json()["rule_id"] == 1

        # —— 最终解锁：先放行重试再释放锁，原替换自动结束，无需重启或人工清理 ——
        gate.release()
        locker2.execute("ROLLBACK")
        locker2.close()
    finally:
        services._find_violations = original_find
        gate.restore()

    result = _wait_replacement(c, rid1)
    assert result["status"] == "switched" and result["finished_at"]
    # 已提交的切换保留成功结果
    assert c.get("/rules/active").json()["rule_id"] == 2

    # 失败状态与互斥的释放一起生效：后续替换不再 409，且能正常完成
    r2 = c.post("/rules/replacements", json=RULE_V2_SAME)
    assert r2.status_code == 202
    assert _wait_replacement(c, r2.json()["id"])["status"] == "switched"

    # 恢复后可继续上传；锁期间被 503 的批次没有留下数据
    up = c.post("/uploads", files=_csv("样品号,项目,读数\nS4,pH,8\n"))
    assert up.status_code == 200 and up.json()["rule_id"] == 3
    assert len(c.get("/batches").json()) == batches_during_lock + 1


def test_failure_finalization_under_lock_keeps_old_rule_and_releases_mutex(client):
    """未提交切换的替换遇锁：解锁后自动标 failed、保留旧规则，互斥同事务释放。"""
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,8\n"))  # V2_BAD 下必违规

    # 工作线程先在失败收尾写事务入口确定性等待，再架外部写锁
    gate = WorkerTxGate(services)
    try:
        r = c.post("/rules/replacements", json=RULE_V2_BAD)
        assert r.status_code == 202
        rid = r.json()["id"]
        gate.handoff(expected_attempt=1)
        assert c.get(f"/rules/replacements/{rid}").json()["status"] == "checking"

        locker = database.connect(db_path)
        locker.execute("BEGIN IMMEDIATE")
        gate.release()  # 放行第 1 次：收尾撞锁，在 SQLite busy 等待后回到第 2 道闸门
        gate.handoff(expected_attempt=2)
        assert c.get(f"/rules/replacements/{rid}").json()["status"] == "checking"
        # 等待期间状态查询保持响应，旧规则全程保留
        assert c.get(f"/rules/replacements/{rid}").status_code == 200
        assert c.get("/rules/active").json()["rule_id"] == 1

        # 先放行重试（进入 busy 等待），再解锁：收尾在锁释放后自动完成
        gate.release()
        locker.execute("ROLLBACK")
        locker.close()
        result = _wait_replacement(c, rid)
    finally:
        gate.restore()

    assert result["status"] == "failed"
    assert result["violation_count"] == 1
    assert result["violations"][0]["sample_no"] == "S1"
    assert result["finished_at"]
    assert c.get("/rules/active").json()["rule_id"] == 1

    # 互斥已随失败状态在同一事务释放：立即可以发起并完成下一次替换
    r2 = c.post("/rules/replacements", json=RULE_V2_BAD)
    assert r2.status_code == 202
    next_result = _wait_replacement(c, r2.json()["id"])
    assert next_result["status"] == "failed"
    assert c.get("/rules/active").json()["rule_id"] == 1


def test_dead_worker_watchdog_settles_and_late_finalize_is_idempotent(client):
    """线程死亡后看门狗补收尾；迟到收尾不改写后续替换、不释放其互斥。"""
    c, database, services, db_path = client
    c.post("/rules", json=RULE_V1)
    c.post("/uploads", files=_csv("样品号,项目,读数\nS1,pH,7\n"))

    # 1) 工作线程在核查后意外死亡，且模拟其自行收尾也失败（连接损坏）
    die = threading.Event()
    original_find = services._find_violations
    original_finalize = services._finalize_failure

    def boom(conn, rule_id, after=0):
        if after == 0:
            die.wait(5)
            raise RuntimeError("模拟工作线程意外退出")
        return original_find(conn, rule_id, after)

    def broken_finalize(conn, rid, reason, *args, **kwargs):
        if threading.current_thread().name == f"replacement-{rid1}":
            raise RuntimeError("模拟收尾连接损坏")
        return original_finalize(conn, rid, reason, *args, **kwargs)

    services._find_violations = boom
    services._finalize_failure = broken_finalize
    rid1 = None
    try:
        r = c.post("/rules/replacements", json=RULE_V2_GOOD)
        rid1 = r.json()["id"]
        die.set()
        deadline = time.time() + 5
        while time.time() < deadline and services._workers.is_live(rid1):
            time.sleep(0.02)
        assert not services._workers.is_live(rid1)  # 线程已死亡并登记
    finally:
        services._find_violations = original_find
        services._finalize_failure = original_finalize

    # 2) 看门狗自动补收尾：标 failed 并在同一事务释放互斥
    deadline = time.time() + 5
    while time.time() < deadline:
        body = c.get(f"/rules/replacements/{rid1}").json()
        if body["status"] == "failed":
            break
        time.sleep(0.05)
    assert body["status"] == "failed" and body["finished_at"]
    assert "看门狗" in body["failure_reason"]
    assert c.get("/rules/active").json()["rule_id"] == 1

    conn = database.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) c FROM replacement_locks").fetchone()["c"] == 0

        # 3) 手工造一个新的在途替换占用互斥（看门狗只收尾“无主（非 alive）”单，
        #    这里把它登记为存活，确保看门狗不会抢先处理，无需停线程）
        with database.transaction(conn):
            conn.execute("INSERT INTO replacement_locks(id) VALUES (1)")
            new_id = services.insert_rule(
                conn, RuleIn.model_validate(RULE_V2_GOOD), active=False)
            conn.execute(
                "INSERT INTO replacements(new_rule_id, old_rule_id, status, snapshot_max_record) "
                "VALUES (?, 1, 'checking', 1)", (new_id,))
            rid2 = conn.execute("SELECT MAX(id) m FROM replacements").fetchone()["m"]
        services._workers.register(rid2)

        # 4) 迟到收尾针对已结束的 rid1：必须返回未命中，不得改写其状态，
        #    也绝不能释放 rid2 持有的互斥
        applied = services._finalize_failure(conn, rid1, "迟到收尾不应生效")
        assert applied is False
        row1 = conn.execute(
            "SELECT status, failure_reason FROM replacements WHERE id=?", (rid1,)
        ).fetchone()
        assert row1["status"] == "failed" and "看门狗" in row1["failure_reason"]
        row2 = conn.execute(
            "SELECT status FROM replacements WHERE id=?", (rid2,)
        ).fetchone()
        assert row2["status"] == "checking"
        assert conn.execute("SELECT COUNT(*) c FROM replacement_locks").fetchone()["c"] == 1

        # 5) 注销存活登记后，看门狗会自然把 rid2 收尾为 failed 并释放互斥
        services._workers.unregister(rid2)
        deadline = time.time() + 5
        while time.time() < deadline:
            st = conn.execute(
                "SELECT status FROM replacements WHERE id=?", (rid2,)
            ).fetchone()["status"]
            lock_n = conn.execute(
                "SELECT COUNT(*) c FROM replacement_locks"
            ).fetchone()["c"]
            if st == "failed" and lock_n == 0:
                break
            time.sleep(0.05)
        assert st == "failed" and lock_n == 0
    finally:
        conn.close()

    # 6) 收尾后无需重启即可继续发起替换并成功切换
    r3 = c.post("/rules/replacements", json=RULE_V2_GOOD)
    assert r3.status_code == 202
    assert _wait_replacement(c, r3.json()["id"])["status"] == "switched"
    assert c.get("/rules/active").json()["rule_id"] >= 3
