from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, Future
from gcandle.objects.basic_objects import DB_CLIENT

import time


def pool_init():
    DB_CLIENT.init()


class CodeListTaskExecutor:
    def __init__(self, executor_cls, n_workers=8):
        self.n_workers = n_workers
        self.executor = executor_cls(max_workers=n_workers)

    def execute(self, codes: list, task_func, *args):
        executor: Executor = self.executor
        start = time.time()
        futures_of_task = {
            self.executor.submit(task_func, c, *args) for c in codes
        }
        print('submitted')
        i = 0
        ntotal = len(codes)
        for future in as_completed(futures_of_task):
            i = i+1
            if i % 100 == 0:
                print("Task execution progress: {:.2f}% of {}".format(i * 100.0 / ntotal, ntotal))

        print("Task all done")
        executor.shutdown()

