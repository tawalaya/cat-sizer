import json
import base64
import logging
from util.lambda_utils import extract_data_from_log
from model.execution_log import ExecutionLog

logger = logging.getLogger(__name__)


class LambdaFunction:
    """ Class representing AWS Lambda function """

    def __init__(self, arn: str, lambda_client):
        self.arn = arn
        self.client = lambda_client

    def list_aliases(self):
        """ Returns all aliases of Lambda function """
        return self.client.list_aliases(FunctionName=self.arn)['Aliases']

    def delete_all_lambda_aliases(self):
        aliases = self.list_aliases()
        for alias in aliases:
            self.delete_alias(alias=alias['Name'])
            self.delete_version(version=alias['FunctionVersion'])

    def get_alias(self, alias: str):
        """ Returns details about a Lambda alias
        :param alias: Alias of Lambda
        :return dict: details of Lambda alias (AliasArn, Name, FunctionVersion, ...)
        """
        logger.info(f"Checking alias {alias}")
        return self.client.get_alias(FunctionName=self.arn, Name=alias)

    def get_config(self, alias: str = None):
        """ Returns the configuration for the Lambda
         :param alias: Alias of Lambda
         :return configuration of Lambda or Lambda alias
         """
        if alias:
            return self.client.get_function_configuration(FunctionName=self.arn, Qualifier=alias)
        else:
            return self.client.get_function_configuration(FunctionName=self.arn)

    def get_memory_size(self, alias: str = None):
        """ Returns the configured memory size for the Lambda
        :param alias: Alias of Lambda
        :return memory size of Lambda or Lambda alias
        """
        return self.get_config(alias)['MemorySize']

    def get_time_out(self, alias: str = None):
        """ Returns the configured timeout for the Lambda
        :param alias: Alias of Lambda
        :return timeout of Lambda or Lambda alias in seconds
       """
        return self.get_config(alias)['Timeout']

    def verify_alias_exists(self, alias: str) -> bool:
        """ Checks if an alias exists for the Lambda function
        :param alias: Alias to check
        :return True if alias exists, else False
        """
        try:
            self.get_alias(alias)
            return True
        except self.client.exceptions.ResourceNotFoundException:
            return False
        except Exception as err:
            # real exception
            raise err

    def create_memory_config(self, value: int, alias: str):
        """ Creates a new Lambda alias with given memory size
        :param value: memory size for configuration
        :param alias: Alias for the memory config
        """
        try:
            self.set_memory_size(value)
            version = self.publish_version()['Version']
            if self.verify_alias_exists(alias):
                self.update_alias(alias, version)
            else:
                self.create_alias(alias, version)
        except Exception as error:
            if 'Alias already exists' not in str(error):
                raise error

    def set_memory_size(self, value: int):
        """ Set memory size of Lambda to given value
        :param value: memory size to set
        :return details about new Lambda configuration
        """
        if self.get_config()['MemorySize'] != value:
            logger.info(f"Setting memory size to: {value}")
            return self.client.update_function_configuration(FunctionName=self.arn, MemorySize=value)
        else:
            logger.info("Function already has given memory size")

    def publish_version(self):
        """ Create new version from current code and configuration """
        logger.info("Publishing new version")
        return self.client.publish_version(FunctionName=self.arn)

    def create_alias(self, alias: str, version: str):
        """ Creates an alias for a Lambda function version """
        logger.info(f"Creating alias: {alias}")
        return self.client.create_alias(FunctionName=self.arn, FunctionVersion=version, Name=alias)

    def update_alias(self, alias: str, version: str):
        """ Updates the configuration of a Lambda function alias
        :param alias: alias to update
        :param version: version to update
        """
        logger.info(f"Updating alias: {alias}")
        return self.client.update_alias(FunctionName=self.arn, FunctionVersion=version, Name=alias)

    def delete_alias(self, alias: str):
        """ Deletes a Lambda function alias
        :param alias: alias to be deleted
        """
        logger.info(f"Deleting alias: {alias}")
        return self.client.delete_alias(FunctionName=self.arn, Name=alias)

    def delete_version(self, version: str):
        """ Deletes a Lambda function version
        :param version: version to be deleted
        """
        logger.info(f"Deleting version: {version}")
        return self.client.delete_function(FunctionName=self.arn, Qualifier=version)

    def invoke(self, alias: str, payload: dict, log_type: str = 'Tail'):
        """ Invokes Lambda function
        :param alias: alias of Lambda function to be invoked. If not given, the standard Lambda function will be invoked.
        :param payload: input payload for Lambda function
        :param log_type: (optional) log type
        :return `ExecutionLog` containing details about execution
        """
        logger.info(f"Invoking function {self.arn}:{alias if alias else '$LATEST'} with payload {payload}")
        bytes_payload = bytes(json.dumps(payload), "utf-8")
        try:
            if alias:
                res = self.client.invoke(FunctionName=self.arn, Qualifier=alias, Payload=bytes_payload, LogType=log_type)
            else:
                res = self.client.invoke(FunctionName=self.arn, Payload=bytes_payload, LogType=log_type)
            log_result = res['LogResult']
            log_str = base64.b64decode(log_result).decode('utf-8')

            log = extract_data_from_log(log_str)
        except Exception as e:
            logger.error("Function invocation failed: " + str(e))
            memory_size = self.get_memory_size(alias=alias)
            timeout_ms = self.get_time_out(alias=alias) * 1000
            log = ExecutionLog(memory_size=memory_size, init_duration=0, duration=timeout_ms,
                               billed_duration=timeout_ms)
        return log
