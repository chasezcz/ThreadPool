"""
ThreadPool class, which supports run tasks with timeout.
"""

import ctypes
import time
from queue import Queue
from threading import Thread
from typing import List


def shutdown_thread(thread_ident):
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread_ident),
        ctypes.py_object(SystemExit),
    )


class STOP:
    pass


class WorkerThread:
    name: str
    thread: Thread = None
    start_time: int = 0
    task_timeout: int = 0
    tasks: Queue

    enable: bool = True

    def __init__(self, name: str, task_queue: Queue) -> None:
        self.name = name
        self.tasks = task_queue

    def is_alive(self) -> bool:
        if self.thread is not None:
            return self.thread.is_alive()
        return False

    def is_busy(self) -> bool:
        return self.start_time > 0

    def activate(self):
        self.task_timeout = 0
        self.start_time = 0
        self.thread = Thread(target=self.work, daemon=True, name=self.name)
        self.thread.start()

    def deactivate(self):
        if self.is_alive():
            shutdown_thread(self.thread.ident)

    def shutdown(self):
        self.enable = False
        self.tasks.put(STOP())
        self.tasks = None
        if self.is_alive():
            shutdown_thread(self.thread.ident)

    def work(self):
        """工作线程的守护程序，从任务队列中读取任务，每次执行任务前，同步执行开始时间，任务超时限额，线程名称"""
        while self.enable and self.tasks:
            message = self.tasks.get()
            if isinstance(message, STOP):
                # 不废话，直接退出！
                return

            # 解包，解不出来就报错
            try:
                func, task_name, timeout, args, kwargs = message
                self.start_time = time.time()
                self.task_timeout = timeout
                self.thread.name = f"{self.name}_{task_name}"
                # 解出来了，直接开干
                func(*args, **kwargs)
            except Exception as e:
                print(e)
                raise Exception(f"Worker raise error {e}")
            finally:
                self.start_time = 0
                self.task_timeout = 0
                self.thread.name = self.name


class ThreadPool:
    """
    ThreadPool class, which supports run tasks with timeout.
    """

    busy_count: int = 0
    enable: bool = True

    def __init__(
        self,
        num_workers: int = 1,
        timeout: float = 0,
        name_prefix: str = "Thread",
        enable_shutdown: bool = False,
    ) -> None:
        self.num_workers = num_workers
        self.timeout = timeout
        self.name_prefix = name_prefix
        self.enable_shutdown = enable_shutdown

        self.tasks = Queue()
        self.pool: List[WorkerThread] = [
            WorkerThread(name=f"{self.name_prefix}_{i}", task_queue=self.tasks) for i in range(num_workers)
        ]
        self.watch_thread = Thread(target=self.watch, daemon=True, name=f"{self.name_prefix}_watcher")
        self.watch_thread.start()

    def watch(self):
        """
        watch thread pool, if thread is timeout, kill it and create a new thread.
        """
        while self.enable and self.tasks:
            busy_count = 0
            # 检查线程执行是否超时或结束。
            for worker in self.pool:
                if not worker.is_alive():
                    worker.activate()
                elif worker.is_busy():
                    # 判断是否超时
                    timeout = worker.task_timeout if worker.task_timeout > 0 else self.timeout
                    if self.enable_shutdown and timeout > 0 and time.time() - worker.start_time > timeout:
                        # 超时了，重启
                        worker.deactivate()
                        worker.activate()
                    else:
                        # 没超时，更新状态
                        busy_count += 1

            self.busy_count = busy_count
            time.sleep(0.0001)

    def submit(self, func, task_name: str = "", time: int = 0, *args, **kwargs) -> None:
        """
        run a task without timeout.

        Args:
            func: target function.
            task_name: task name.
            *args:  function args.
            **kwargs: function kwargs.
        """
        if self.tasks is None:
            return
        self.tasks.put([func, task_name, time, args, kwargs])

    @property
    def size(self) -> int:
        """
        count of thread pool which is alive..

        Returns:
            int: the count.
        """

        return self.busy_count

    @property
    def empty(self) -> bool:
        """
        whether empty the thread pools.

        Returns:
            bool: the result of whether all the thread tasks are done.
        """
        return self.busy_count == 0 and (self.tasks is None or self.tasks.empty())

    @property
    def names(self) -> list:
        """
        return names of cur pool.
        """
        names = []
        for thread in self.pool:
            names.append(thread.name)
        return names

    def shutdown(self):
        """shutdown the thread pool."""
        self.enable = False
        self.tasks = None

        if self.watch_thread.is_alive():
            shutdown_thread(self.watch_thread.ident)

        for worker in self.pool:
            worker.shutdown()
        self.pool.clear()


if __name__ == "__main__":
    pool = ThreadPool(3, enable_shutdown=True)

    def test(n):
        print(f"{n} Start")
        time.sleep(n)
        print(f"{n} Stop")

    pool.submit(test, "test", 0, 1)
    pool.submit(test, "test", 0, 2)
    pool.submit(test, "test", 0, 3)
    pool.submit(test, "test", 0, 4)

    time.sleep(8)
    pool.shutdown()
    print("现在应该啥也没有了")
    while True:
        pass
