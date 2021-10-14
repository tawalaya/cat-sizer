import logging
import numpy as np
import math
import json
import os
from sizer.lambda_sizer import LambdaSizer
from scipy.optimize import curve_fit
from model.execution_log import ExecutionLog
import base64

logger = logging.getLogger(__name__)


class SizingResult:
    def __init__(self, memory_size: int, cost: float, duration: float ):
        self.memory_size = memory_size
        self.cost = cost
        self.duration = duration


class RegressionSizer(LambdaSizer):

    def __init__(self, lambda_arn: str, payload: dict, balanced_weight: float = 0.5, sample_runs: int = 5 , memory_sizes: list = [128, 512, 1024, 2048, 3008]):
        super().__init__(lambda_arn, payload, balanced_weight)
        self.sample_runs = sample_runs
        self.memory_sizes = memory_sizes

    def _execute_function(self, memory_size: int, payload: dict):
        logger.info(f'Running function with memory size: {memory_size} MB')
        alias = f'{memory_size}MB'
        cost = 0.0
        self._create_alias_if_needed(memory_size)

        log = self.lambda_function.invoke(alias=alias, payload=payload)
        cost += log.cost
        if log.init_duration > 0:
            # ignore cold start invocations
            log = self.lambda_function.invoke(alias=alias, payload=payload)
            cost += log.cost
        else:
            logger.info("droping execution due to cold_start " + log.to_string())
        logger.info("Execution log: " + log.to_string())
        return log, cost

    def _sample(self):
        avg_logs = []
        total_cost = 0.0
        initial_memory_size = self.lambda_function.get_memory_size()
        for memory_size in self.memory_sizes:
            logs = []
            for i in range(self.sample_runs):
                log, cost = self._execute_function(memory_size=memory_size, payload=self.payload)
                logs.append(log)
                total_cost += cost

            self._save_logs(logs, function_name=f'{self.function_name}', filepath=f'./logs/{self.function_name}/{memory_size}MB.csv')
            avg_duration = sum(log.duration for log in logs) / len(logs)
            avg_billed_duration = sum(log.billed_duration for log in logs) / len(logs)
            avg_logs.append(ExecutionLog(memory_size=memory_size, duration=avg_duration, billed_duration=avg_billed_duration))

        # reset to initial memory size
        self.lambda_function.set_memory_size(initial_memory_size)

        logs_path = f'../logs/{self.function_name}/avg.csv'
        self._save_logs(avg_logs, function_name=self.function_name, filepath=logs_path)

        return logs_path, total_cost

    @staticmethod
    def _predict(memory_size, func, params):
        duration = func(memory_size, *params)
        return ExecutionLog(memory_size=memory_size, duration=duration, billed_duration=math.ceil(duration))

    @staticmethod
    def _find_cheapest(logs):
        sorted_logs = sorted(logs, key=lambda x: (x.cost, x.duration))
        return sorted_logs[0]

    @staticmethod
    def _find_fastest(logs):
        sorted_logs = sorted(logs, key=lambda x: (x.duration, x.cost))
        return sorted_logs[0]

    @staticmethod
    def _find_by_weight(logs, weight: float):
        max_cost = max(log.cost for log in logs)
        max_duration = max(log.duration for log in logs)

        def weighted_sum(log: ExecutionLog):
            return weight * log.cost / max_cost + (1 - weight) * log.duration / max_duration

        return min(logs, key=lambda log: weighted_sum(log))

    def _save_model(self, model):
        path = os.path.join(os.path.dirname(__file__), 'performance_model_repository.json')
        params = list(model)
        with open(path, 'r') as f:
            models = json.load(f)
            models[self.lambda_function.arn] = params
        with open(path, 'w') as f:
            json.dump(models, f, indent=4)

    def configure_function(self, logs_path=None, cleanup=False):
        if not logs_path:
            logs_path, total_sampling_cost = self._sample()
        else:
            total_sampling_cost = 0

        data = np.genfromtxt(logs_path, delimiter=',', skip_header=1)
        data = data[data[:, 1].argsort()]
        xdata = data[:, 1].transpose()
        ydata = data[:, 4].transpose()

        def func(x, a, b, c):
            return a * np.exp(-b * x) + c

        init_values = [50, 0, 1]

        popt, pcov = curve_fit(func, xdata, ydata, p0=init_values, bounds=([0, 0, 0], [100000, 10, min(ydata)]))

        # save to repository
        self._save_model(popt)

        memory_step_size = 64
        all_memory_sizes = list(map(lambda x: x * memory_step_size, range(2, 47)))
        logs = list(map(lambda x: self._predict(x, func, popt), all_memory_sizes))

        if self.balanced_weight == 0:
            log = self._find_cheapest(logs)
        elif self.balanced_weight == 1:
            log = self._find_fastest(logs)
        else:
            log = self._find_by_weight(logs, self.balanced_weight)

        result = SizingResult(log.memory_size, log.cost, log.duration)

        if cleanup:
            self.lambda_function.delete_all_lambda_aliases()

        return result, logs, list(popt), total_sampling_cost