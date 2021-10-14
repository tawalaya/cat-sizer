import json
from model.step_function import StepFunction, TIME_PER_TRANSITION
from model.performance_model import PerformanceModel
from util.lambda_constants import MIN_MEMORY_SIZE
from scipy.optimize import dual_annealing


class WorkflowSizer:
    def __init__(self, state_machine_arn: str, elat_constraint: int, performance_models=None):
        self.state_machine_arn = state_machine_arn
        self.elat_constraint = elat_constraint
        self.step_function = StepFunction(arn=state_machine_arn)
        self.performance_models = performance_models

    def run(self,max_memory_size = 3008):
        if not self.performance_models:
            lambda_arns = self.step_function.get_lambda_resources()
            self.performance_models = self.load_performance_models(lambda_arns)

        performance_models = self.performance_models

        # TODO: generalize this for all workflows
        def get_elat(memory_sizes):
            elat = TIME_PER_TRANSITION * len(performance_models)
            elat += performance_models[0].get_duration(memory_sizes[0])
            # assuming two parallel branches with one function each
            branches = [performance_models[1].get_duration(memory_sizes[1]),
                        performance_models[2].get_duration(memory_sizes[2])]
            elat += max(branches)
            elat += performance_models[3].get_duration(memory_sizes[3])
            return elat

        def get_cost(memory_sizes):
            elat_diff = get_elat(memory_sizes) - self.elat_constraint
            if elat_diff > 0:
                # penalty for violating constraint
                return 1
            k = 0
            for i, size in enumerate(memory_sizes):
                k += performance_models[i].get_cost(size)
            return k

        bounds = [(MIN_MEMORY_SIZE, max_memory_size) for i in performance_models]
        # dual annealing enables global optimization, does not support constraints out of the box
        # modified objective function to support constraint
        result = dual_annealing(get_cost, bounds=bounds, maxiter=1000)
        if result.success:
            selected_sizes = list(map(lambda x: int(x), result.x))
            return selected_sizes,get_elat(selected_sizes), result.fun
        else:
            raise ValueError(result.message)

    @staticmethod
    def load_performance_models(lambda_arns: list):
        with open('./performance_model_repository.json', 'r') as f:
            repo = json.load(f)

        models = []

        for arn in lambda_arns:
            arn = arn.replace(":128MB", "")
            if arn in repo:
                variables = repo[arn]
                models.append(PerformanceModel(t0=variables[0], _lambda=variables[1], t_min=variables[2]))
        return models
