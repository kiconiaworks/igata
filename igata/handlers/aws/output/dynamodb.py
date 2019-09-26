import datetime
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from hashlib import md5
from typing import Generator, List, Tuple

import boto3
import pytz
from botocore.config import Config

from .... import settings
from ....utils import flatten
from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")


TEN_SECONDS = 10
config = Config(connect_timeout=TEN_SECONDS, retries={"max_attempts": 5})
DYNAMODB = boto3.resource("dynamodb", config=config, region_name=settings.AWS_REGION, endpoint_url=settings.DYNAMODB_ENDPOINT)
DEFAULT_RESULTS_KEYNAME = "result"

RESULTS_ADDITIONAL_PARENT_FIELDS = os.getenv("RESULTS_ADDITIONAL_PARENT_FIELDS", "request_id,s3_uri")  # comma separated field to include from parent
RESULTS_SORTKEY_KEYNAME = os.getenv("RESULTS_SORTKEY_KEYNAME", "s3_uri")

REQUESTS_TABLE_HASHKEY_KEYNAME = os.getenv("REQUESTS_TABLE_HASHKEY_KEYNAME", "request_id")
REQUESTS_TABLE_RESULTS_KEYNAME = os.getenv("REQUESTS_TABLE_RESULTS_KEYNAME", DEFAULT_RESULTS_KEYNAME)
DYNAMODB_DECIMAL_PRECISION_DIGITS = settings.DYNAMODB_DECIMAL_PRECISION_DIGITS  # make local reference for minor speedup

JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")

# fields dependent on api implementation
RESULTS_TABLE_STATE_FIELDNAME = "state"
RESULTS_ERROR_STATE = "error"
RESULTS_PROCESSED_STATE = "processed"


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
    - REQUESTS_TABLE_RESULTS_KEYNAME
    - RESULTS_TABLE_STATE_FIELDNAME
    """
    table = DYNAMODB.Table(tablename)
    logger.info(f"Updating item in Table({tablename})...")
    logger.debug(f"item: {item}")
    # update None to empty list for results
    if item[REQUESTS_TABLE_RESULTS_KEYNAME] is None:
        msg = f'item[REQUESTS_TABLE_RESULTS_KEYNAME] is None, setting REQUESTS_TABLE_RESULTS_KEYNAME({item[REQUESTS_TABLE_RESULTS_KEYNAME]}) to "[]"'
        logger.warning(msg)
        item[REQUESTS_TABLE_RESULTS_KEYNAME] = "[]"  # to resolve issue with read from Pynamodb

    try:
        # Assure that updated `errors` field is not None
        errors_field_value = "[]"
        if "errors" in item and item["errors"] is not None:
            errors_field_value = item["errors"]
        response = table.update_item(
            Key={REQUESTS_TABLE_HASHKEY_KEYNAME: item[REQUESTS_TABLE_HASHKEY_KEYNAME]},
            UpdateExpression=(
                "SET " "#s = :state, " "#r = :result, " "#e = :errors, " "#u = :updated_at_timestamp"  # should be REQUESTS_TABLE_RESULTS_KEYNAME
            ),
            ExpressionAttributeNames={
                "#s": RESULTS_TABLE_STATE_FIELDNAME,
                "#r": REQUESTS_TABLE_RESULTS_KEYNAME,
                "#e": "errors",
                "#u": "updated_at_timestamp",
            },
            ExpressionAttributeValues={
                ":state": item[RESULTS_TABLE_STATE_FIELDNAME],
                f":{REQUESTS_TABLE_RESULTS_KEYNAME}": item[REQUESTS_TABLE_RESULTS_KEYNAME],
                ":errors": errors_field_value,
                ":updated_at_timestamp": item.get("updated_at_timestamp", int(datetime.datetime.utcnow().replace(tzinfo=pytz.UTC).timestamp())),
            },
        )
    except Exception as e:
        logger.exception(e)
        logger.error(f"unable to put_item() to table: {tablename}")
        response = {}

    return response


def check_and_convert(value, precision=DYNAMODB_DECIMAL_PRECISION_DIGITS):
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


class DynamodbOutputCtxManager(OutputCtxManagerBase):
    """Output records to dynamodb"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requests_tablename = kwargs["requests_tablename"]
        self.results_tablename = kwargs["results_tablename"]
        if "results_keyname" in kwargs:
            raise ValueError(
                f'results_keyname({kwargs["results_keyname"]}) given, '
                f"but not supported, RESULTS_KEYNAME *MUST* be: {REQUESTS_TABLE_RESULTS_KEYNAME}"
            )
        self.results_keyname = REQUESTS_TABLE_RESULTS_KEYNAME
        self.executor = ThreadPoolExecutor()
        self.futures = []
        self.results_additional_parent_keys = kwargs.get("results_additional_parent_keys", None)
        if not self.results_additional_parent_keys:
            if RESULTS_ADDITIONAL_PARENT_FIELDS:
                fields = RESULTS_ADDITIONAL_PARENT_FIELDS.split(",")
                self.results_additional_parent_keys = fields
        else:
            assert isinstance(self.results_additional_parent_keys, (list, tuple))
        logger.info(f"RESULTS_ADDITIONALPARENT_FIELDS will be added to results: {self.results_additional_parent_keys}")

    def put_records(self, records: List[dict], **kwargs) -> dict:
        """
        Define method to put data to the desired output target

        Expects 2 tables:
           - Original requests table
               - for returning the full result related to the original request
           - Detailed results table
               - for analyzing the detailed results for a specific request
        """
        detailed_results_table = DYNAMODB.Table(self.results_tablename)

        start = time.time()
        request_update_items = 0
        detailed_results_put_items = 0
        total_results = 0
        logger.debug(f"RESULTS_PROCESSED_STATE: {RESULTS_PROCESSED_STATE}")
        logger.debug(f"REQUESTS_TABLE_RESULTS_KEYNAME: {REQUESTS_TABLE_RESULTS_KEYNAME}")
        with detailed_results_table.batch_writer() as detailed_writer:
            for record in records:
                # update record with state, so it is included in the resulting nested_keys
                state = RESULTS_PROCESSED_STATE
                if "errors" in record and record["errors"]:
                    state = RESULTS_ERROR_STATE
                record[RESULTS_TABLE_STATE_FIELDNAME] = state
                prepared_record, original_record_nested_data = prepare_record(record)

                if self.results_keyname not in prepared_record:
                    logger.warning(f'Expected Key("{self.results_keyname}") not in {prepared_record}, setting "{self.results_keyname}" to "[]"')
                    prepared_record[self.results_keyname] = "[]"
                logger.debug(f"update_item (prepared_record): {prepared_record}")

                future = self.executor.submit(update_item, prepared_record, self.requests_tablename)
                self.futures.append(future)
                request_update_items += 1

                # output to detailed table
                if self.results_keyname not in original_record_nested_data:
                    logger.warning(
                        f'Expected Key("{self.results_keyname}") not in original_record_nested_data({original_record_nested_data}), '
                        f"no detailed_results will be inserted!!!"
                    )

                else:
                    logger.debug(f"original_record_nested_data: {original_record_nested_data}")
                    prediction_results = original_record_nested_data[self.results_keyname]
                    logger.debug(f"prediction_results: {prediction_results}")
                    for result in prediction_results:
                        # add parent keys if defined
                        if self.results_additional_parent_keys:
                            for additional_key in self.results_additional_parent_keys:
                                if additional_key not in prepared_record:
                                    # find all missing keys (even if 1 is missing)
                                    missing = [k for k in self.results_additional_parent_keys if k not in prepared_record]
                                    msg = f"expected additional_key(s) missing {missing} in prepared_record: {prepared_record}"
                                    logger.error(msg)
                                    raise ResultExpectedKeyError(msg)

                                result[additional_key] = prepared_record[additional_key]

                        flattened_result = tuple(flatten(result, allow_null_strings=False))
                        # dynamodb doesn't support Float types are not supported. Use Decimal types instead.
                        output_item = {k: check_and_convert(v) for k, v in flattened_result}

                        if RESULTS_SORTKEY_KEYNAME not in output_item:  # make sure that required sortkey is included
                            raise ValueError(f"Expected SortKey({RESULTS_SORTKEY_KEYNAME} not in: {output_item}")

                        # generate unique hashkey
                        output_item["hashkey"] = md5(str(sorted(flattened_result)).encode("utf8")).hexdigest()
                        logger.debug(f"detailed_writer.put_item: {output_item}")
                        detailed_writer.put_item(output_item)
                        detailed_results_put_items += 1

        end = time.time()
        summary = {
            "request_update_items": request_update_items,
            "detailed_results_put_items": detailed_results_put_items,
            "total_results": total_results,
            "elapsed": end - start,
        }
        return summary

    def __enter__(self):
        return self

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
        required = ("results_tablename", "requests_tablename")
        return required
