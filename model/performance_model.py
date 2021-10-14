import math
import numpy as np
from model.execution_log import compute_cost, ExecutionLog


class PerformanceModel:
    def __init__(self, t0, _lambda, t_min):
        self.t0 = t0
        self._lambda = _lambda
        self.t_min = t_min
        self.durations = {}
        self.costs = {}

    def evaluate(self, x):
        return self.get_duration(x), self.get_cost(x)

    def get_cost(self, memory_size):
        duration = self.get_duration(memory_size)
        billed_duration = math.ceil(duration)
        cost = compute_cost(memory_size, billed_duration)
        return cost

    def get_duration(self, memory_size):
        duration = self.t0 * np.exp(-self._lambda * memory_size) + self.t_min
        return duration

    def create_logs(self, sizes):
        logs = []
        for size in sizes:
            duration = self.get_duration(size)
            logs.append(ExecutionLog(duration, math.ceil(duration), size))

        return logs

    @staticmethod
    def _nearest_config(memory_size):
        x = 64 * round(memory_size / 64)
        return x
