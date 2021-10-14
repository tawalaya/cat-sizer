import json
import math
from gekko import GEKKO
from model.step_function import StepFunction, TIME_PER_TRANSITION, COST_PER_TRANSITION
from model.performance_model import PerformanceModel
from util.lambda_constants import MIN_MEMORY_SIZE, MIN_COST


class ChainSizer:
    def __init__(self, state_machine_arn: str, constraint_type: str, duration_constraint: int, cost_constraint: float):
        self.state_machine_arn = state_machine_arn
        self.constraint_type = constraint_type
        self.duration_constraint = duration_constraint
        self.cost_constraint = cost_constraint
        self.step_function = StepFunction(arn=state_machine_arn)

    def run(self, performance_models=None):
        # extract lambda arns from state machine
        if not performance_models:
            lambda_arns = self.step_function.get_lambda_resources()
            print(lambda_arns)
            performance_models = self.load_performance_models(lambda_arns)

        a = [p.t0 for p in performance_models]
        b = [p._lambda for p in performance_models]
        c = [p.t_min for p in performance_models]

        m = GEKKO(remote=True)

        max_memory_size = 3008

        # create variables
        x = m.Array(m.Var, len(performance_models), lb=MIN_MEMORY_SIZE, ub=max_memory_size)
        I = range(len(x))

        state_machine_transition_time = TIME_PER_TRANSITION * (len(x) + 1)
        state_machine_transition_cost = COST_PER_TRANSITION * (len(x) + 1)

        def duration(i):
            return a[i] * math.e ** (-b[i] * x[i]) + c[i]

        def base_cost(i):
            return MIN_COST * (x[i] / MIN_MEMORY_SIZE)

        def aggr_duration():
            y = state_machine_transition_time + m.sum([duration(i) for i in I])
            return y

        def aggr_cost():
            y = state_machine_transition_cost + m.sum([base_cost(i) * duration(i) for i in I])
            return y

        if self.constraint_type == 'Cost':
            print(f"Cost constraint: {self.cost_constraint}")
            m.Equation(aggr_cost() < self.cost_constraint)
            m.Minimize(aggr_duration())
        else:
            print(f"Duration constraint: {self.duration_constraint}")
            m.Equation(aggr_duration() < self.duration_constraint)
            m.Minimize(aggr_cost())

        m.solve(disp=False)
        res = [var.value[0] for var in x]

        sum_d = state_machine_transition_time
        sum_c = state_machine_transition_cost
        sizes = []
        for i, model in enumerate(performance_models):
            memory_size = res[i]
            # round up
            memory_size = math.ceil(memory_size)  # 64 * round(memory_size / 64)
            sizes.append(memory_size)
            d, c = model.evaluate(memory_size)
            sum_d += d
            sum_c += c

        return sizes, sum_d, sum_c

    @staticmethod
    def load_performance_models(lambda_arns: list):
        models = []

        for arn in lambda_arns:
            arn = arn.replace(":128MB", "")
            model = ChainSizer.load_performance_model(arn)
            if model:
                models.append(model)
            else:
                print(f"Model not found for {arn}")
        return models

    @staticmethod
    def load_performance_model(arn):
        with open('./performance_model_repository.json', 'r') as f:
            repo = json.load(f)

        if arn in repo:
            variables = repo[arn]
            return PerformanceModel(t0=variables[0], _lambda=variables[1], t_min=variables[2])

        return None
