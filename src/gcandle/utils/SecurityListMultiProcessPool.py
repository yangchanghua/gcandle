from multiprocessing import Pool
from gcandle.objects.basic_objects import DB_CLIENT
from time import time

DEFAULT_POOL_SIZE = 8

def pool_init():
    DB_CLIENT.init()


class SecurityListMultiProcessPool:
    def __init__(self):
        pass
    
    def submit(self, codes, task_func, *args):
        n_start = 0
        n_end = len(codes)
        codes = codes[n_start:n_end]

        t_start = time()
        pool = Pool(DEFAULT_POOL_SIZE, initializer=pool_init)
        if len(args) > 0:
            results = [pool.apply_async(task_func, args=(c, *args)) for c in codes]
        else:
            # Fucking python feature: if a tuple literal is created with one single str,
            # the str will be splitted as chars
            results = [pool.apply_async(task_func, args=(c,)) for c in codes]
        count = n_start
        total = len(codes) + n_start
        for r in results:
            insert_result = r.get()
            count += 1
            if count % 300 == 0:
                print('Updating progress {:.2f}%, {}'.format(float(count / total) * 100.0, count))
            if insert_result is not None:
                if insert_result.acknowledged:
                    pass
                else:
                    print('Failed to insert results')

        t_end = time()
        print('Time used: ', t_end - t_start)