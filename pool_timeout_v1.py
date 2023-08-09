from concurrent.futures import ThreadPoolExecutor, TimeoutError
import time


def return_future_result(message):
    time.sleep(2)
    return message

pool = ThreadPoolExecutor(max_workers=2)
future1 = pool.submit(return_future_result, ("hello"))
future2 = pool.submit(return_future_result, ("world"))
print(future1.done())
# time.sleep(3)
try:
    print(future1.result(timeout=1))
except TimeoutError as err:
    pool.shutdown()
    print(f"TimeoutError: {err}")
print(future1.result())
print(future2.result())
print(future2.done())