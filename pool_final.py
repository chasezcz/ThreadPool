"""
ThreadPool class, which supports run tasks with timeout.
"""

import ctypes
import logging
import threading
import time
from queue import Queue
from threading import Thread

# get threadpool logger
logger = logging.getLogger("threadpool")


class ThreadPool:
    """
    ThreadPool class, which supports run tasks with timeout.
    """

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

        self.pool = []
        self.active_num = 0
        self.tasks = Queue()
        self.enable_shutdown = enable_shutdown
        self.water_thread = Thread(target=self.watch, daemon=True, name="pool_watcher")
        self.water_thread.start()

    def new_worker_thread(self, id: int):
        """
        _summary_: create a new thread.

        Args:
            id (int): create a new thread and add it to the thread pool.
        """
        while id >= len(self.pool):
            self.pool.append({})

        thread = Thread(target=self.worker_watch, args=(id,), daemon=True)
        self.pool[id] = {"thread": thread}
        thread.start()

    def kill_thread(self, thread_ident):
        if thread_ident is not None:
            try:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_ident), ctypes.py_object(SystemExit))
            except Exception:
                ...

    def kill_worker_thread(self, id: int):
        """
        _summary_: kill a thread.

        Args:
            id (int): the index of thread pool.
        """
        if id < len(self.pool) and "thread" in self.pool[id]:
            thread = self.pool[id]["thread"]
            self.kill_thread(thread.ident)

            self.pool[id].pop("thread", "")
            self.pool[id].pop("start_time", "")
            del thread

    def watch(self):
        """
        _summary_: watch thread pool, if thread is timeout, kill it and create a new thread.
        """

        while True and self.tasks is not None:
            active_num = 0
            # Check whether thread execution times out or ends.
            for id in range(self.num_workers):
                # If the thread is not in the thread pool, create a new thread.
                if id >= len(self.pool):
                    self.new_worker_thread(id)
                # Check if the thread is timeout, create a new thread.
                thread_item: dict = self.pool[id]
                if "thread" not in thread_item:
                    self.new_worker_thread(id)
                # 判断线程执行时间是否超时
                if self.enable_shutdown and "start_time" in thread_item and "timeout" in thread_item:
                    if time.time() - thread_item["start_time"] > thread_item["timeout"]:
                        thread_name = thread_item["thread"].name
                        logger.debug(f"{thread_name} execute timeout, kill it.")
                        self.kill_worker_thread(id)
                        self.new_worker_thread(id)
                # 判断线程是否存活
                if "thread" in thread_item and "start_time" in thread_item:
                    active_num += 1

            self.active_num = active_num
            time.sleep(0.1)

    def worker_watch(self, id: int):
        """
        _summary_: worker thread, run task.

        Args:
            id (int): the index of thread pool.
        """
        while True and self.tasks is not None:
            threading.current_thread().name = f"{self.name_prefix}_{id}"

            func, task_name, timeout, args, kwargs = self.tasks.get(True)
            timeout = timeout if timeout != 0.0 else self.timeout

            threading.current_thread().name = f"{self.name_prefix}_{id}_{task_name}"
            try:
                self.pool[id].update({"start_time": time.time(), "timeout": timeout})
                func(*args, **kwargs)
                logger.debug(f"{self.name_prefix}_{id}_{task_name} done.")
            except Exception as e:
                logger.error(f"{self.name_prefix}_{id}_{task_name} error: {e}")
            finally:
                self.pool[id].pop("start_time", "")
                self.pool[id].pop("timeout", "")

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

        return self.active_num

    @property
    def empty(self) -> bool:
        """
        whether empty the thread pools.

        Returns:
            bool: the result of whether all the thread tasks are done.
        """
        return self.active_num == 0 and (self.tasks is None or self.tasks.empty())

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
        self.tasks = None
        logger.debug("shutdown thread pool.")
        for id, _ in enumerate(self.pool):
            self.kill_worker_thread(id)
        self.pool.clear()
        logger.debug("thread pool shutdown done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

    pool = ThreadPool(num_workers=3, timeout=0, enable_shutdown=True, name_prefix="test")

    def sleep_and_print(t):
        time.sleep(t)
        logger.info(f"print {t}")

    pool.submit(sleep_and_print, "sleep_and_print", 1, 3)
    pool.submit(sleep_and_print, "sleep_and_print", 2, 3)
    pool.submit(sleep_and_print, "sleep_and_print", 3, 3)
    pool.shutdown()
    pool.submit(sleep_and_print, "sleep_and_print", 4, 3)
    while not pool.empty:
        ...
    logger.info("all done.")
