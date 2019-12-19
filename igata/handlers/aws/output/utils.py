import datetime
import json
import logging
from decimal import Decimal
from typing import Generator, Tuple

import boto3
from botocore.config import Config
from igata import settings

logger = logging.getLogger("cliexecutor")


TEN_SECONDS = 10
config = Config(connect_timeout=TEN_SECONDS, retries={"max_attempts": 5})
DYNAMODB = boto3.resource("dynamodb", config=config, region_name=settings.AWS_REGION, endpoint_url=settings.DYNAMODB_ENDPOINT)


def update_item(item: dict, tablename: str) -> dict:
    """
    Update the given item entry in the Dynamodb REQUESTS table

    item is expected to have the following keys:
    - REQUESTS_TABLE_HASHKEY_KEYNAME
    - RESULTS_TABLE_STATE_FIELDNAME
    """
    table = DYNAMODB.Table(tablename)
    logger.info(f"Updating item in Table({tablename})...")
    logger.debug(f"item: {item}")
    try:
        # Assure that updated `errors` field is not None
        errors_field_value = "[]"
        if "errors" in item and item["errors"] is not None:
            errors_field_value = item["errors"]
        response = table.update_item(
            Key={settings.DYNAMODB_REQUESTS_TABLE_HASHKEY_KEYNAME: item[settings.DYNAMODB_REQUESTS_TABLE_HASHKEY_KEYNAME]},
            UpdateExpression=(
                # should be REQUESTS_TABLE_RESULTS_KEYNAME
                "SET #s = :predictor_status, #r = :result_s3_uris, #e = :errors, #u = :updated_timestamp, #c = :completed_timestamp"
            ),
            ExpressionAttributeNames={
                "#s": "predictor_status",  # settings.DYNAMODB_RESULTS_TABLE_STATE_FIELDNAME,
                "#u": "updated_timestamp",  # settings.DYNAMODB_REQUESTS_TABLE_RESULTS_KEYNAME,
                "#c": "completed_timestamp",
                "#r": "result_s3_uris",
                "#e": "errors",
            },
            ExpressionAttributeValues={
                ":predictor_status": item[settings.DYNAMODB_RESULTS_TABLE_STATE_FIELDNAME],
                f":result_s3_uris": item["result_s3_uris"],
                ":errors": errors_field_value,
                ":updated_timestamp": item.get("updated_timestamp", int(datetime.datetime.now(datetime.timezone.utc).timestamp())),
                ":completed_timestamp": item.get("completed_timestamp", int(datetime.datetime.now(datetime.timezone.utc).timestamp())),
            },
        )
    except Exception as e:
        logger.exception(e)
        logger.error(f"unable to put_item() to table: {tablename}")
        response = {}

    return response


def get_nested_keys(record: dict) -> Generator[str, None, None]:
    """get all keys in a dictionary that contains nested mappings/elements"""
    for k, v in record.items():
        if isinstance(v, (list, tuple, dict)):
            yield k


def check_and_convert(value, precision=settings.DYNAMODB_DECIMAL_PRECISION_DIGITS):
    """convert float to decimal for dynamodb"""
    return value if not isinstance(value, float) else round(Decimal(value), precision)


def prepare_record(record: dict) -> Tuple[dict, dict]:
    """
    Convert record data for DynamoDB insertion
    record: updated record with json dumps fields for nested record values
    original_nested_data: untouched nested key record data
    """
    original_nested_data = {}  # used for processing the results into the results table
    nested_keys = get_nested_keys(record)
    if not nested_keys:
        logger.warning(f"No nested_keys found for record: {record}")
    else:
        for nested_key in nested_keys:
            # jsonize and byteify nested items
            value = record[nested_key]
            original_nested_data[nested_key] = value  # keep original value for later processing
            value_json_bytes = json.dumps(value)
            record[nested_key] = value_json_bytes
    non_nested_keys = set(record.keys()) - set(nested_keys)
    for k in non_nested_keys:
        v = record[k]
        record[k] = check_and_convert(v)
    return record, original_nested_data
