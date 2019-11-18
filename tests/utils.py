import json
from pathlib import Path

import boto3
from igata import settings

S3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)
SQS = boto3.client("sqs", endpoint_url=settings.SQS_ENDPOINT, region_name="ap-northeast-1")

DYANMODB = boto3.resource("dynamodb", endpoint_url=settings.DYNAMODB_ENDPOINT, region_name=settings.AWS_REGION)
DYANMODB_CLIENT = boto3.client("dynamodb", endpoint_url=settings.DYNAMODB_ENDPOINT, region_name=settings.AWS_REGION)
SNS_CLIENT = boto3.client("sns", endpoint_url=settings.SNS_ENDPOINT, region_name=settings.AWS_REGION)


def sqs_queue_send_message(queue_name, message_body):
    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]

    if not isinstance(message_body, str):
        message_body = json.dumps(message_body)
    SQS.send_message(QueueUrl=queue_url, MessageBody=message_body)
    return queue_url


def sqs_queue_get_attributes(queue_name) -> dict:
    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    return SQS.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])


def _create_bucket(bucket):
    S3.create_bucket(Bucket=bucket)


def _delete_bucket(bucket):
    response = S3.list_objects(Bucket=bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            S3.delete_object(Bucket=bucket, Key=obj["Key"])
    S3.delete_bucket(Bucket=bucket)


def _upload_to_s3(filepath, bucket, key):
    with filepath.open("rb") as bin_file:
        response = S3.put_object(Bucket=bucket, Key=key, Body=bin_file)


def _delete_from_s3(bucket, key):
    response = S3.delete_object(Bucket=bucket, Key=key)


def setup_teardown_s3_file(local_filepath: Path, bucket, key):
    def decorator(function):
        def wrapper(*args, **kwargs):
            _create_bucket(bucket)
            _upload_to_s3(local_filepath, bucket, key)
            result = function(*args, **kwargs)
            _delete_from_s3(bucket, key)
            _delete_bucket(bucket)
            return result

        return wrapper

    return decorator


def setup_teardown_s3_bucket(bucket):
    def decorator(function):
        def wrapper(*args, **kwargs):
            _create_bucket(bucket)
            result = function(*args, **kwargs)
            _delete_bucket(bucket)
            return result

        return wrapper

    return decorator


def _create_sqs_queue(queue_name: str, purge: bool = False) -> str:
    response = SQS.create_queue(QueueName=queue_name)
    if purge:
        SQS.purge_queue(QueueUrl=response["QueueUrl"])
    return response["QueueUrl"]


def _get_queue_url(queue_name):
    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    return queue_url


def _delete_sqs_queue(queue_name):
    queue_url = _get_queue_url(queue_name)
    SQS.delete_queue(QueueUrl=queue_url)


def setup_teardown_sqs_queue(queue_name):
    def decorator(function):
        def wrapper(*args, **kwargs):
            _create_sqs_queue(queue_name)
            result = function(*args, **kwargs)
            _delete_sqs_queue(queue_name)
            return result

        return wrapper

    return decorator


DEFAULT_FIELDS = {"request_id": ("S", "HASH")}


def _dynamodb_create_table(tablename="test-table", fields=DEFAULT_FIELDS):

    attribute_type_index = 0
    attribute_defintions = [{"AttributeName": k, "AttributeType": values[attribute_type_index]} for k, values in fields.items()]

    key_type_index = 1
    key_schema = [{"AttributeName": k, "KeyType": values[key_type_index]} for k, values in fields.items()]

    table = DYANMODB.create_table(
        TableName=tablename,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_defintions,
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    waiter = DYANMODB_CLIENT.get_waiter("table_exists")
    waiter.wait(TableName=tablename)
    return table


def _get_dynamodb_table_resource(tablename="test-table"):
    table = DYANMODB.Table(tablename)
    return table


def _dynamodb_delete_table(tablename="test-table"):
    table = DYANMODB.Table(tablename)
    table.delete()


def setup_teardown_dyanmodb_table(tablename="test-table", fields=DEFAULT_FIELDS):
    def decorator(function):
        def wrapper(*args, **kwargs):
            _dynamodb_create_table(tablename, fields)
            deleted = False
            result = None
            raised_exception = None
            try:
                result = function(*args, **kwargs)
            except Exception as e:
                _dynamodb_delete_table(tablename)
                deleted = True
                raised_exception = e

            if not deleted:
                _dynamodb_delete_table(tablename)
            if raised_exception:
                raise raised_exception
            return result

        return wrapper

    return decorator


def _create_sns_topic(topic_name="test-sns-topic"):
    response = SNS_CLIENT.create_topic(Name=topic_name)
    arn = response["TopicArn"]
    return arn
