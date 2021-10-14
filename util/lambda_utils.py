import re
from model.execution_log import ExecutionLog


def extract_data_from_log(log):
    init_duration = 0

    match = re.search('Init Duration: (([0-9]*[.])?[0-9]+) ms', log)
    if match:
        init_duration = float(match.group(1))
    match = re.search('Duration: (([0-9]*[.])?[0-9]+) ms\s*Billed', log)
    duration = float(match.group(1))
    match = re.search('Billed Duration: ([0-9]*) ms', log)
    billed_duration = int(match.group(1))
    match = re.search('Memory Size: ([0-9]*) MB', log)
    memory_size = int(match.group(1))
    return ExecutionLog(duration, billed_duration, memory_size, init_duration)



def get_function_name(arn):
    return arn.split('-')[-2]
