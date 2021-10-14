import boto3
import logging
import json
from util.lambda_utils import extract_data_from_log
from util.utils import get_recursively

logger = logging.getLogger(__name__)

TIME_PER_TRANSITION = 20
COST_PER_TRANSITION = 0.000025


class StepFunction:

    def __init__(self, arn: str):
        self.state_machine_arn = arn
        self.logs_client = boto3.client('logs')
        self.step_functions = boto3.client('stepfunctions')

    def calculate_execution_cost(self, execution_arn: str):
        """ Calculate aggregated cost of a StepFunction execution
        :param execution_arn: ARN if StepFunction execution
        :return cost of execution
        """

        history = self.get_execution_history(execution_arn)
        lambda_arns = self._extract_lambda_arns(history)
        logs = []
        for arn in lambda_arns:
            log_group_name = self._log_group_name_from_lambda_arn(arn)
            latest_log_stream = self._get_latest_log_stream(log_group_name)
            if not latest_log_stream:
                logger.info(f"Execution Log not found for {arn}")
                continue
            log_stream_name = latest_log_stream['logStreamName']
            logger.info(log_stream_name)
            events = self.logs_client.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name)[
                'events']
            if events:
                last_report = list(filter(lambda e: e['message'].startswith('REPORT'), events))[-1]
                if last_report:
                    execution_log = extract_data_from_log(last_report['message'])
                    logs.append(execution_log)
        for log in logs:
            logger.info(log.to_string())
        cost = sum(log.cost for log in logs)
        return cost

    def get_execution_history(self, execution_arn: str):
        """ Returns the history of a StepFunction execution
        :param execution_arn: ARN of StepFunction execution
        :return History of execution
        """
        return self.step_functions.get_execution_history(executionArn=execution_arn)

    def check_if_execution_finished(self, execution_arn: str):
        """ Check execution history to see if execution has finished
        :param execution_arn: ARN of StepFunction execution
        :return True if execution has finished, False if it has not finished
        """
        events = self.get_execution_history(execution_arn)['events']
        if len(events) > 0:
            last_event = events[-1]
            if last_event['type'] == 'ExecutionSucceeded':
                return True
            if last_event['type'] == 'ExecutionFailed':
                raise Exception("Execution failed")
        return False

    def get_duration(self, execution_arn: str):
        """ Returns the duration of an execution
        :param execution_arn: ARN of StepFunction execution
        :return duration of execution in seconds
        """
        description = self.step_functions.describe_execution(executionArn=execution_arn)
        start = description['startDate']
        stop = description['stopDate']
        return (stop - start).total_seconds()

    def invoke(self, payload: str):
        """ Invoke the StepFunction with given input
        :param payload: Input for StepFunction
        :return ARN and start time of execution
        """
        return self.step_functions.start_execution(stateMachineArn=self.state_machine_arn, input=payload)

    def get_lambda_resources(self):
        """
        Extracts all Lambda ARNs from state machine definition
        """
        definition = self.step_functions.describe_state_machine(stateMachineArn=self.state_machine_arn)['definition']
        definition = json.loads(definition)
        return get_recursively(definition, 'Resource')

    @staticmethod
    def _extract_lambda_arns(history: dict):
        events = history['events']
        schedule_events = filter(lambda x: x['type'] == 'LambdaFunctionScheduled', events)
        return list(map(lambda x: x['lambdaFunctionScheduledEventDetails']['resource'], schedule_events))

    def _get_latest_log_stream(self, log_group_name: str):
        log_streams = \
            self.logs_client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime',
                                                  descending=True)[
                'logStreams']
        if len(log_streams) > 0:
            return log_streams[0]
        return None

    @staticmethod
    def _log_group_name_from_lambda_arn(arn: str):
        function_name = arn.split(':function:')[-1]
        # remove alias suffix if present
        function_name = function_name.split(":")[0]
        return f'/aws/lambda/{function_name}'
