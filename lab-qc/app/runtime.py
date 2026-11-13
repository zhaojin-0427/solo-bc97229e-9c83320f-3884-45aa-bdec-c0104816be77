"""进程级运行态：替换工作线程登记与看门狗线程管理。

本模块刻意保持无数据库依赖，并且不在测试/重载流程中被 ``importlib.reload``：
``app.services`` 可能被整体重载，但 ``sys.modules`` 中的本模块对象始终是同一个
实例，因此看门狗线程、停止事件与工作线程登记能跨越模块代次共享——否则重载后
旧看门狗线程会失去停止事件的引用而泄漏、堆积，并在别的数据库上误做收尾。
"""
from __future__ import annotations

import threading


class WorkerRegistry:
    """进程内替换工作线程存活登记。

    * ``alive``：正在执行（含重试等待）的替换 id；
    * ``dead``：线程已结束但收尾可能未完成的替换 id，等待看门狗/后续请求补收尾。

    单进程（单 worker 部署）下，内存登记与 SQLite 互斥表配套使用。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.alive: set[int] = set()
        self.dead: set[int] = set()

    def register(self, replacement_id: int) -> None:
        with self._lock:
            self.dead.discard(replacement_id)
            self.alive.add(replacement_id)

    def mark_dead(self, replacement_id: int) -> None:
        """线程确认自己无法完成收尾时调用（随后即退出）。"""
        with self._lock:
            self.alive.discard(replacement_id)
            self.dead.add(replacement_id)

    def unregister(self, replacement_id: int) -> None:
        """正常收尾完成后调用；dead 标记留给看门狗清理。"""
        with self._lock:
            self.alive.discard(replacement_id)

    def is_live(self, replacement_id: int) -> bool:
        with self._lock:
            return replacement_id in self.alive

    def snapshot_inflight(self, db_inflight: list[int]) -> tuple[list[int], list[int]]:
        """返回 (存活在途, 死亡待收尾) 两个 id 列表。"""
        with self._lock:
            alive = [i for i in db_inflight if i in self.alive]
            dead = [i for i in db_inflight if i not in self.alive]
        return alive, dead

    def clear_dead(self, replacement_id: int) -> None:
        with self._lock:
            self.dead.discard(replacement_id)


# 跨模块重载共享的唯一登记实例
workers = WorkerRegistry()

_watchdog_thread: threading.Thread | None = None
_watchdog_stop: threading.Event | None = None
_watchdog_lock = threading.Lock()


def start_watchdog(loop) -> None:
    """启动唯一的看门狗守护线程；已存活则忽略，保证全局只有一个。

    ``loop`` 由当前代次的 services 提供（接收 stop_event）；若线程早已启动，
    沿用旧 loop 即可——它操作的是同一数据库与本模块的共享登记。
    """
    global _watchdog_thread, _watchdog_stop
    with _watchdog_lock:
        if _watchdog_thread is not None and _watchdog_thread.is_alive():
            return
        stop_event = threading.Event()
        _watchdog_stop = stop_event
        t = threading.Thread(target=loop, args=(stop_event,),
                             name="replacement-watchdog", daemon=True)
        _watchdog_thread = t
        t.start()


def stop_watchdog(timeout: float = 10) -> None:
    """停止巡检循环并等待线程完全退出。

    状态保存在不被重载的本模块中，因此任何代次的 services 调用都能精确停掉
    实际运行的看门狗线程。
    """
    with _watchdog_lock:
        stop_event = _watchdog_stop
        t = _watchdog_thread
    if stop_event is not None:
        stop_event.set()
    if t is not None and t.is_alive() and t is not threading.current_thread():
        t.join(timeout=timeout)
