from functools import wraps
from multiprocessing.pool import ThreadPool


class ThreadPoolConcurrencyMiddleware:

    def __init__(self, max_processes=1):
        self.max_processes = max_processes

    def __call__(self, func):
        @wraps(func)
        def wrapper(database, *queries, **kwargs):
            with ThreadPool(processes=self.max_processes) as pool:
                results = pool.map(lambda query: func(database, query, **kwargs)[0], queries)
                pool.close()

            return results

        return wrapper
