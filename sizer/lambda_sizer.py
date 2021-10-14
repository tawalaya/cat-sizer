import logging
import boto3
import csv
import os
from model.lambda_function import LambdaFunction
from util.lambda_utils import get_function_name
from model.cleaner import Cleaner

logger = logging.getLogger(__name__)


class LambdaSizer:

    def __init__(self, lambda_arn: str, payload: dict, balanced_weight: float):
        self.client = boto3.client('lambda')
        self.lambda_function = LambdaFunction(arn=lambda_arn, lambda_client=self.client)
        self.function_name = get_function_name(lambda_arn)
        self.payload = payload
        self.balanced_weight = balanced_weight
        self.cleaner = Cleaner()

    @staticmethod
    def _save_logs(logs: list, function_name: str, filepath: str):
        fieldnames = ['Function Name', 'Memory Size', 'Init Duration', 'Duration', 'Billed Duration', 'Cost']

        try:
            os.makedirs(os.path.dirname(filepath))
        except FileExistsError:
            pass

        with open(filepath, 'w+') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for log in logs:
                writer.writerow({'Function Name': function_name, 'Memory Size': log.memory_size,
                                 'Init Duration': log.init_duration,
                                 'Duration': log.duration, 'Billed Duration': log.billed_duration,
                                 'Cost': '{0:.10f}'.format(log.cost)})

    @staticmethod
    def get_alias_for_memory_size(memory_size: int):
        return f"{memory_size}MB"

    def _create_alias_if_needed(self, memory_size: int):
        alias = self.get_alias_for_memory_size(memory_size)
        if self.lambda_function.verify_alias_exists(alias):
            logger.info(f'{alias} already exists, skipping creation.')
        else:
            self.lambda_function.create_memory_config(value=memory_size, alias=alias)

        return alias

    def _remove_aliases(self, aliases: list):
        for alias in aliases:
            self.cleaner.delete_lambda_alias(lambda_func=self.lambda_function, alias=alias)
