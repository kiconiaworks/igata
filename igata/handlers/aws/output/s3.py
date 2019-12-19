import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO
from typing import List, Union

import boto3

from .... import settings
from . import OutputCtxManagerBase
from .utils import prepare_record, update_item

logger = logging.getLogger("cliexecutor")
S3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)
S3BUCKET_OUTPUT_FILENAME_PREFIX = os.getenv("S3BUCKET_OUTPUT_FILENAME_PREFIX", "results-xyz34567yh-")
DEFAULT_OUTPUT_FILENAME_PREFIX = "output-"
DEFAULT_OUTPUT_HEADERS = True
JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")


class S3BucketPandasDataFrameCsvFileOutputCtxManager(OutputCtxManagerBase):
    """Context manger for outputting results to an s3 bucket"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.output_s3_bucket = kwargs["output_s3_bucket"]

        self.output_s3_prefix = kwargs.get("output_s3_prefix", None)
        if self.output_s3_prefix and self.output_s3_prefix.startswith("/"):
            self.output_s3_prefix = self.output_s3_prefix[1:]
        if self.output_s3_prefix and self.output_s3_prefix.endswith("/"):
            self.output_s3_prefix = self.output_s3_prefix[:-1]
        if "get_pandas_to_csv_kwargs" in kwargs and kwargs["get_pandas_to_csv_kwargs"]:
            logger.info('overwritting "self.get_pandas_to_csv_kwargs" with provided staticmethod')
            self.get_pandas_to_csv_kwargs = kwargs["get_pandas_to_csv_kwargs"]

        self.requests_tablename = kwargs.get("requests_tablename", None)
        if not self.requests_tablename:
            logger.debug(f'setting "requests_tablename" to: {settings.DYNAMODB_REQUESTS_TABLENAME}')
            self.requests_tablename = settings.DYNAMODB_REQUESTS_TABLENAME

        if "get_additional_dynamodb_request_update_attributes" in kwargs and kwargs["get_additional_dynamodb_request_update_attributes"]:
            logger.info('updating with "get_additional_dynamodb_request_update_attributes" with optional staticmethod...')
            self.get_additional_dynamodb_request_update_attributes = kwargs["get_additional_dynamodb_request_update_attributes"]

        self.results_keyname = kwargs.get("results_keyname", "result_s3_uris")

        self.executor = ThreadPoolExecutor()
        self.futures = []

    @classmethod
    def required_kwargs(cls):
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'OUTPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            OUTPUT_CTXMGR_OUTPUT_S3_BUCKET
            OUTPUT_CTXMGR_CSV_FIELDNAMES

        """
        required_keys = ("output_s3_bucket",)
        return required_keys

    def put_records(self, records: List[Union[list, tuple, dict]], encoding: str = "utf8"):
        """Required implementation method."""
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

    @staticmethod
    def get_pandas_to_csv_kwargs(result: dict) -> dict:
        """
        Determine the appropriate pandas.to_csv(**kwargs) needed to write out the file
        Result Record:
            [
                {
                    "job_id": {JOB_ID|REQUEST_ID},
                    "filename": {OUTPUT_FILENAME},
                    "dataframe": {result dataframe},
                }
            ]
        """
        filename = result["filename"]
        gzip_result = False
        if filename.endswith("gz"):
            logger.warning(f'filename({filename}).endswith(".gz"), will gzip results!')
            gzip_result = True

        compression_type = "gzip" if gzip_result else None
        kwargs = {
            "sep": settings.DEFAULT_INPUT_CSV_DELIMITER,
            "encoding": settings.DEFAULT_INPUT_CSV_ENCODING,
            "header": settings.DEFAULT_INPUT_CSV_HEADER_LINES,
            "index": False,
            "compression": compression_type,
        }
        return kwargs

    def put_record(self, record: dict, *args, **kwargs) -> dict:
        """
        Result Record:
            {
                "job_id": {JOB_ID|REQUEST_ID},
                "filename": {OUTPUT_FILENAME},
                "gzip": True,
                "dataframe": {record dataframe},
            }
        """
        output_info = {}
        if record["is_valid"]:
            job_id = record["job_id"]
            filename = record.get("filename", None)
            if not filename:
                filename = f"{job_id}.csv"

            key = f"{self.output_s3_prefix}/{filename}"
            logger.info(f"preparing ({filename})...")

            df_csv_buffer = StringIO()
            df = record["dataframe"]
            kwargs = self.get_pandas_to_csv_kwargs(record)

            logger.debug(f"csv output kwargs: {kwargs}")
            df.to_csv(df_csv_buffer, **kwargs)
            logger.info(f"preparing: SUCCESS!")

            logger.info(f"writing results to: s3://{self.output_s3_bucket}/{key}")
            df_csv_buffer.seek(0)  # reset file for reading
            encoded_buffer = BytesIO(df_csv_buffer.read().encode("utf8"))
            encoded_buffer.seek(0)
            S3.upload_fileobj(Fileobj=encoded_buffer, Bucket=self.output_s3_bucket, Key=key)
            logger.info("writing results: SUCCESS!")
            self._record_results.append(record)
            output_info = {"Bucket": self.output_s3_bucket, "Key": key}
        return output_info

    @property
    def key(self):
        """Define the output key"""
        now = datetime.datetime.now()
        prefix = self.__class__.__name__.lower()
        key = f"{prefix}/{self.filename_prefix}_{self.unique_hash.hexdigest()}_{now:%Y%m%d_%H%M%S}.csv"
        return key

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
