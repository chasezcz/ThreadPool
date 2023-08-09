"""
ThreadPool class, which supports run tasks with timeout.
"""

import threading
import time
import logging
from queue import Queue, LifoQueue
from threading import Thread
from typing import Dict, Tuple


# get threadpool logger
logger = logging.getLogger("threadpool")


class ThreadPool:
    """
    ThreadPool class, which supports run tasks with timeout.
    """

    # TODO: 每个 worker 的 task 完成时，不能立即删除，而是等待一段时间，如果没有新的 task 加入，则删除该 worker。

    def __init__(
        self,
        num_workers: int = 1,
        timeout: float = 0,
        name_prefix: str = "Thread",
        shutdown: bool = False,
    ) -> None:
        self._num_workers = num_workers
        self._timeout = timeout
        self._name_prefix = name_prefix
        self._pool: Dict[Thread, Tuple] = {}

        self._tasks = LifoQueue()
        self._shutdown = shutdown
        Thread(target=self._watch, daemon=True, name="pool_watcher").start()

    def _watch(self):
        while True:
            # Check whether thread execution times out or ends.
            for thread in list(self._pool.keys()):
                start_time, timeout = self._pool[thread]
                if not thread.is_alive():
                    self._pool.pop(thread)
                elif 0 < timeout < time.time() - start_time:
                    if self._shutdown:
                        # !!!!! Forcibly stop the thread, but this operation may cause the spark server to crash !!!!
                        import ctypes

                        if thread.ident is not None:
                            try:
                                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                                    ctypes.c_long(thread.ident), ctypes.py_object(SystemExit)
                                )
                            except Exception:
                                ...
                    self._pool.pop(thread)
                    logger.info(f"thread-{thread.name} time out. drop it.")

            # Add a new task when the size of thread_pool is lower than num_workers.
            if not self._tasks.empty() and len(self._pool) < self._num_workers:
                # If this thread has no task and there are tasks in the task pool that are not executed.
                func, task_name, timeout, args, kwargs = self._tasks.get()
                name = f"{self._name_prefix}_{task_name}"
                timeout = timeout if timeout != 0.0 else self._timeout
                thread = Thread(target=func, args=args, kwargs=kwargs, name=name, daemon=True)
                self._pool[thread] = (time.time(), timeout)
                thread.start()

    def submit_with_timeout(self, func, task_name: str = "", timeout: float = 0.0, *args, **kwargs) -> None:
        """
        run a task with timeout.

        Args:
            func: target function.
            task_name: task_name.
            timeout: function timeout.
            *args:  function args.
            **kwargs: function kwargs.
        """

        self._tasks.put([func, task_name, timeout, args, kwargs])

    def submit(self, func, task_name: str = "", *args, **kwargs) -> None:
        """
        run a task without timeout.

        Args:
            func: target function.
            task_name: task name.
            *args:  function args.
            **kwargs: function kwargs.
        """
        self._tasks.put([func, task_name, self._timeout, args, kwargs])

    @property
    def size(self) -> int:
        """
        count of thread pool which is alive..

        Returns:
            int: the count.
        """
        return len(self._pool)

    @property
    def empty(self) -> bool:
        """
        whether empty the thread pools.

        Returns:
            bool: the result of whether all the thread tasks are done.
        """
        return len(self._pool) == 0 and self._tasks.empty()

    @property
    def names(self) -> list:
        """
        return names of cur pool.
        """
        names = []
        for thread in self._pool:
            names.append(thread.name)
        return names

    def shutdown(self):
        """ shutdown the thread pool. """
        self._pool.clear()
        self._tasks = Queue()