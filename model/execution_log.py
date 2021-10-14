from util.lambda_constants import MIN_COST, MIN_MEMORY_SIZE, STATIC_INVOCATION_COST

def compute_cost(memory_size, billed_duration):
    return MIN_COST * (memory_size / MIN_MEMORY_SIZE) * billed_duration + STATIC_INVOCATION_COST

class ExecutionLog:
    """
    Class representing the execution log of a AWS Lambda function
    """

    def __init__(self, duration, billed_duration, memory_size, init_duration=0):
        self.duration = duration
        self.billed_duration = billed_duration
        self.memory_size = memory_size
        self.init_duration = init_duration
        self.cost = compute_cost(memory_size, billed_duration)

    def to_string(self):
        return f"MemorySize: {self.memory_size} MB, Duration: {self.duration}, Billed Duration: {self.billed_duration}, Init Duration: {self.init_duration}, Cost: {'{0:.12f}'.format(self.cost)}"
