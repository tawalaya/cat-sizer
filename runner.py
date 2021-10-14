# Tool to run individual or worklfow experimentes for fixed siezes.
from model.execution_log import ExecutionLog
from model.lambda_function import LambdaFunction
import boto3
import os
import csv
from util.utils import timeit
from datetime import datetime
import numpy as np
import time
import json
from model.step_function import StepFunction
from util.utils import get_recursively


client = boto3.client('lambda')


class StepFunctionExecutionLog:
    def __init__(self, duration, cost):
        self.duration = duration
        self.cost = cost


def get_workflow_name(arn):
    return arn.split(":")[-1]


def get_payload():
    raise Exception("needs to be implemented by you")


def save_logs(logs: list, filepath: str):
    fieldnames = ['Memory Size', 'Init Duration', 'Duration', 'Billed Duration', 'Cost']

    try:
        os.makedirs(os.path.dirname(filepath))
    except FileExistsError:
        pass

    with open(filepath, 'w+') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for log in logs:
            writer.writerow({'Memory Size': log.memory_size,
                             'Init Duration': log.init_duration,
                             'Duration': log.duration, 'Billed Duration': log.billed_duration,
                             'Cost': '{0:.10f}'.format(log.cost)})


def get_alias_for_memory_size(memory_size: int):
    return f"{memory_size}MB"


def create_alias_if_needed(lambda_function, memory_size: int):
    alias = get_alias_for_memory_size(memory_size)
    if lambda_function.verify_alias_exists(alias):
        print(f'{alias} already exists, skipping creation.')
    else:
        lambda_function.create_memory_config(value=memory_size, alias=alias)

    return alias


def execute_function(lambda_function: LambdaFunction, memory_size: int, payload: dict):
    print(f'Running function with memory size: {memory_size} MB')
    alias = f'{memory_size}MB'
    cost = 0.0
    create_alias_if_needed(lambda_function, memory_size)

    log = lambda_function.invoke(alias=alias, payload=payload)
    cost += log.cost
    if log.init_duration > 0:
        # re-invoke on cold start
        log = lambda_function.invoke(alias=alias, payload=payload)
        cost += log.cost
    print("Execution log: " + log.to_string())
    return log, cost


@timeit
def run(arn: str, payload: dict, memory_sizes: list, runs_per_size: int = 5):
    f = LambdaFunction(arn, client)
    avg_logs = []
    total_sampling_cost = 0.0
    initial_memory_size = f.get_memory_size()
    all_logs = []
    for memory_size in memory_sizes:
        logs = []
        for i in range(runs_per_size):
            log, cost = execute_function(lambda_function=f, memory_size=memory_size, payload=payload)
            # parse_from_csv(f"./logs/{self.function_name}/{memory_size}.csv")
            logs.append(log)
            all_logs.append(log)
            total_sampling_cost += cost

        avg_duration = sum(log.duration for log in logs) / len(logs)
        avg_billed_duration = sum(log.billed_duration for log in logs) / len(logs)
        avg_logs.append(
            ExecutionLog(memory_size=memory_size, duration=avg_duration, billed_duration=avg_billed_duration))
    timestamp = datetime.now().strftime("%d_%b_%Y_%H_%M_%S")
    save_logs(all_logs, filepath=f'./logs/{arn}/raw_{timestamp}.csv')
    # reset to initial memory size
    f.set_memory_size(initial_memory_size)

    logs_path = f'./logs/{arn}/avg.csv'
    save_logs(avg_logs, filepath=logs_path)
    return avg_logs, logs_path, total_sampling_cost

def save_logs(logs, file_name):
    fieldnames = ['Duration', 'Cost']

    with open(f'./logs/{file_name}.csv', 'w+') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for log in logs:
            writer.writerow({'Duration': log.duration,
                             'Cost': '{0:.10f}'.format(log.cost)})





def run_workflow(arn, sizes, outfile,lambdas ,number_of_runs = 6):
    """ Updates memory sizes for each Lambda and executes state machine """
    
    def set_memory_sizes(sizes):
        for i, size in enumerate(sizes):
            lambdas[i].set_memory_size(size)

    set_memory_sizes(sizes)

    step = StepFunction(arn=arn)
    logs = []
    for i in range(number_of_runs):
        res = step.invoke(payload=get_payload())
        arn = res['executionArn']
        print(f"Execution ARN: {arn}")
        # wait to finish
        print("Waiting for execution to finish")
        time.sleep(10)
        while True:
            if step.check_if_execution_finished(arn):
                print("Execution has finished")
                break
            time.sleep(10)

        # wait for logs
        time.sleep(10)
        cost = step.calculate_execution_cost(arn)
        duration = step.get_duration(arn)
        log = StepFunctionExecutionLog(duration, cost)
        print(f"Duration: {duration}s, Cost: {cost}")
        if i == 0:
            print("Ignoring first run due to cold start")
            continue
        logs.append(log)

    save_logs(logs, outfile + '_raw')
    avg_duration = sum(log.duration for log in logs) / len(logs)
    avg_cost = sum(log.cost for log in logs) / len(logs)
    save_logs([StepFunctionExecutionLog(avg_duration, avg_cost)],
              file_name=outfile + '_avg')
    print(f"Average duration: {avg_duration}")
    print(f"Average cost: {'{0:.10f}'.format(avg_cost)}")