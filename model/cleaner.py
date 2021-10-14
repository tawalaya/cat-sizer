import boto3
from model.lambda_function import LambdaFunction


class Cleaner:

    def __init__(self):
        self.s3 = boto3.resource('s3')

    def clear_s3_bucket(self, bucket_name: str):
        bucket = self.s3.Bucket(bucket_name)
        bucket.objects.all().delete()

    @staticmethod
    def delete_lambda_alias(lambda_func: LambdaFunction, alias: str):
        version = lambda_func.get_alias(alias=alias)['FunctionVersion']
        lambda_func.delete_alias(alias=alias)
        lambda_func.delete_version(version=version)