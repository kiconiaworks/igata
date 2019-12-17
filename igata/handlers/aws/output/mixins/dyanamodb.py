import datetime
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any, Generator, List, Optional, Tuple

import boto3
from botocore.config import Config
from igata import settings

from . import PostPredictHookMixInBase

logger = logging.getLogger("cliexecutor")


TEN_SECONDS = 10
config = Config(connect_timeout=TEN_SECONDS, retries={"max_attempts": 5})
DYNAMODB = boto3.resource("dynamodb", config=config, region_name=settings.AWS_REGION, endpoint_url=settings.DYNAMODB_ENDPOINT)


DYNAMODB_RESULTS_TABLE_STATE_FIELDNAME = os.getenv("DYNAMODB_RESULTS_TABLE_STATE_FIELDNAME", "predictor_status")


class ResultExpectedKeyError(KeyError):
    """Raised when a prediction result does not contain the expected Key(s)"""

    pass


def get_nested_keys(record: dict) -> Generator[str, None, None]:
    """get all keys in a dictionary that contains nested mappings/elements"""
    for k, v in record.items():
        if isinstance(v, (list, tuple, dict)):
            yield k


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


class DynamodbRequestUpdateMixIn(PostPredictHookMixInBase):
    """Output records to dynamodb"""

    _record_results = []
    results_keyname = "result_s3_uris"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requests_tablename = kwargs["requests_tablename"]

        if "get_additional_dynamodb_request_update_attributes" in kwargs and kwargs["get_additional_dynamodb_request_update_attributes"]:
            logger.info('updating with "get_additional_dynamodb_request_update_attributes" with optional staticmethod...')
            self.get_additional_dynamodb_request_update_attributes = kwargs["get_additional_dynamodb_request_update_attributes"]

        self.executor = ThreadPoolExecutor()
        self.futures = []

    def post_predict_hook(self, record: List[dict], response: Any, meta: Optional[dict] = None) -> Any:
        """Update record results with any necessary additional fields"""
        # add additional dynamodb attributes to update
        item = None
        updated_record = []
        for item in record:
            additional_dynamodb_attributes = self.get_additional_dynamodb_request_update_attributes(item, response, meta)
            if additional_dynamodb_attributes:
                item.update(additional_dynamodb_attributes)
            updated_record.append(item)
        return item

    @staticmethod
    def get_additional_dynamodb_request_update_attributes(record: Any, response: Any, meta: Optional[dict] = None) -> dict:
        """Hook that allows defining field attributes not contained in the result record"""
        return {}

    def put_records(self, records: List[dict], **kwargs) -> dict:
        """
        Puts records to the desired output target

        Expects 2 tables:
           - Original requests table
               - for returning the full result related to the original request
        """
        start = time.time()
        request_update_items = 0
        detailed_results_put_items = 0
        total_results = 0

        # create local references for minor speedup
        DYNAMODB_RESULTS_PROCESSED_STATE = settings.DYNAMODB_RESULTS_PROCESSED_STATE
        DYNAMODB_RESULTS_ERROR_STATE = settings.DYNAMODB_RESULTS_ERROR_STATE

        logger.debug(f"DYNAMODB_RESULTS_PROCESSED_STATE: {DYNAMODB_RESULTS_PROCESSED_STATE}")

        for record in records:
            # update record with state, so it is included in the resulting nested_keys
            state = DYNAMODB_RESULTS_PROCESSED_STATE
            if "errors" in record and record["errors"]:
                state = DYNAMODB_RESULTS_ERROR_STATE
            record["predictor_status"] = state
            prepared_record, original_record_nested_data = prepare_record(record)

            if self.results_keyname not in prepared_record:
                logger.warning(f'Expected Key("{self.results_keyname}") not in {prepared_record}, setting "{self.results_keyname}" to "[]"')
                prepared_record[self.results_keyname] = "[]"
            logger.debug(f"update_item (prepared_record): {prepared_record}")

            future = self.executor.submit(update_item, prepared_record, self.requests_tablename)
            self.futures.append(future)
            request_update_items += 1

        end = time.time()
        summary = {
            "request_update_items": request_update_items,
            "detailed_results_put_items": detailed_results_put_items,
            "total_results": total_results,
            "elapsed": end - start,
        }
        return summary

    def __exit__(self, *args, **kwargs):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)

        for f in self.futures:
            response = f.result(timeout=None)
            logger.debug(f"future response: {response}")
            if response and "ResponseMetadata" in response and "HTTPStatusCode" in response["ResponseMetadata"]:
                status_code = response["ResponseMetadata"]["HTTPStatusCode"]
                if status_code != 200:
                    logger.error(f"(update_item) future response: [{status_code}] {response}")
                else:
                    logger.info(f"(update_item) future response: [{status_code}]")
            else:
                logger.warning(f"future UNKNOWN response: {response}")
        self.executor.shutdown()

    @classmethod
    def required_kwargs(cls) -> Tuple:
        """
        Define the required instantiation kwarg argument names

        ex:
            OUTPUT_CTXMGR_TABLENAME
        """
        required = ("requests_tablename", "requests_results_fieldname", "requests_state_fieldname")
        return required
