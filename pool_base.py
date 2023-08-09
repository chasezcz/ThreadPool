import time
from concurrent.futures import ThreadPoolExecutor


# 参数 times 用来模拟网络请求的时间
def get_html(times):
    time.sleep(times)
    print("get page {}s finished".format(times))
    return times


executor = ThreadPoolExecutor(max_workers=2)
# 通过 submit 函数提交执行的函数到线程池中，submit 函数立即返回，不阻塞
task1 = executor.submit(get_html, (3))
task2 = executor.submit(get_html, (2))
# done 方法用于判定某个任务是否完成
print(task1.done())
# cancel 方法用于取消某个任务，该任务没有放入线程池中才能取消成功
print(task2.cancel())
time.sleep(4)
print(task1.done())
# result 方法可以获取 task 的执行结果


print(task1.result())

# 执行结果
# False  # 表明 task1 未执行完成
# False  # 表明 task2 取消失败，因为已经放入了线程池中
# get page 2s finished
# get page 3s finished
# True  # 由于在 get page 3s finished 之后才打印，所以此时 task1 必然完成了
# 3     # 得到 task1 的任务返回值
