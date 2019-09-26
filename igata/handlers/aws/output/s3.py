import datetime
import logging
import os
from hashlib import md5
from io import BytesIO
from typing import List, Union

import boto3

from .... import settings
from ....utils import flatten
from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")
S3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)
S3BUCKET_OUTPUT_FILENAME_PREFIX = os.getenv("S3BUCKET_OUTPUT_FILENAME_PREFIX", "results-xyz34567yh-")
DEFAULT_OUTPUT_FILENAME_PREFIX = "output-"
DEFAULT_OUTPUT_HEADERS = True
JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")


class S3BucketCsvFileOutputCtxManager(OutputCtxManagerBase):
    """Context manger for outputting results to an s3 bucket"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unique_hash = md5()
        self.output_s3_bucket = kwargs["output_s3_bucket"]
        self.filename_prefix = kwargs.get("filename_prefix", DEFAULT_OUTPUT_FILENAME_PREFIX)
        fieldnames_value = kwargs["csv_fieldnames"]
        if isinstance(fieldnames_value, str):
            # parse comma separated list
            assert "," in fieldnames_value
            self.csv_fieldnames = [v.strip() for v in fieldnames_value.split(",")]
        elif isinstance(fieldnames_value, (list, tuple)):
            self.csv_fieldnames = fieldnames_value
        else:
            raise ValueError(f'Invalid value given for "csv_fieldnames": {fieldnames_value}')
        self.output_headers = kwargs.get("output_headers", DEFAULT_OUTPUT_HEADERS)
        self.line_ending = kwargs.get("line_ending", "\n")
        self.open_file = None

    @classmethod
    def required_kwargs(cls):
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'OUTPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            OUTPUT_CTXMGR_OUTPUT_S3_BUCKET
            OUTPUT_CTXMGR_CSV_FIELDNAMES

        """
        required_keys = ("output_s3_bucket", "csv_fieldnames")
        return required_keys

    def put_records(self, records: List[Union[list, tuple, dict]], encoding: str = "utf8"):
        """
        Accept a list of values to output to file.
        Given list/tuple converted to CSV string and encoded as UTF8
        """
        summary = {}
        lines = 0
        csv_line = None
        is_first_line = True  # for generating unique key
        for lines, record in enumerate(records, 1):
            if isinstance(record, dict):
                flattened_record = flatten(record)
                record = [v for k, v in sorted(flattened_record)]
            csv_line = ",".join(str(v) for v in record).encode(encoding)
            if csv_line:
                self.open_file.write(csv_line)
                self.open_file.write(self.line_ending.encode(encoding))
                if is_first_line:
                    self.unique_hash.update(csv_line)
        if csv_line:
            self.unique_hash.update(csv_line)
        summary["lines"] = lines
        return summary

    @property
    def key(self):
        """Define the output key"""
        now = datetime.datetime.now()
        prefix = self.__class__.__name__.lower()
        key = f"{prefix}/{self.filename_prefix}_{self.unique_hash.hexdigest()}_{now:%Y%m%d_%H%M%S}.csv"
        return key

    def __enter__(self):
        self.open_file = BytesIO()
        if self.output_headers:
            logger.debug(f"Outputting headers: {self.csv_fieldnames}")
            self.put_records(self.csv_fieldnames)
        return self

    def __exit__(self, *args, **kwargs):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)

        logger.info(f"Writing results to: s3://{self.output_s3_bucket}/{self.key}")
        self.open_file.seek(0)  # set pointer to 0 so data can be read
        S3.upload_fileobj(Fileobj=self.open_file, Bucket=self.output_s3_bucket, Key=self.key)
        self.open_file.close()
