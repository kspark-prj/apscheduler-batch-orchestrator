import time
from contextlib import contextmanager


class StopWatch:
    def __init__(self):
        self.start_time = None
        self.laps = {}

    @contextmanager
    def task(self, task_name):
        start = time.perf_counter()
        yield
        end = time.perf_counter()
        self.laps[task_name] = end - start

    def pretty_print(self):
        print("\n=== StopWatch Summary ===")
        total = 0
        for task, duration in self.laps.items():
            print(f"Task [{task}]: {duration:.6f} s")
            total += duration
        print(f"Total Time: {total:.6f} s\n=========================")
