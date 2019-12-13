import datetime
import logging
import os
from io import BytesIO, StringIO, TextIOWrapper
from typing import List, Union

import boto3

from .... import settings
from . import OutputCtxManagerBase

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

        self.to_csv_kwargs = kwargs.get("to_csv_kwargs", None)
        if not self.to_csv_kwargs:
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
            self.to_csv_kwargs = dict(
                encoding=settings.DEFAULT_OUTPUT_CSV_ENCODING,
                sep=settings.DEFAULT_OUTPUT_CSV_DELIMITER,
                header=False,
                index=False,
                compression="gzip",
            )

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
        pass

    def put_record(self, record: List[dict], *args, **kwargs) -> int:
        """
        Result Record:
            [
                {
                    "job_id": {JOB_ID|REQUEST_ID},
                    "filename": {OUTPUT_FILENAME},
                    "gzip": True,
                    "dataframe": {result dataframe},
                    "to_csv_kwargs": {}
                }
            ]
        """
        outputs_info = []
        for result in record:
            job_id = result["job_id"]
            filename = result.get("filename", None)
            if not filename:
                filename = f"{job_id}.csv"

            gzip_result = result.get("gzip", False)
            if filename.endswith("gzip"):
                logger.warning(f'filename({filename}).endswith(".gz"), will gzip results!')
                gzip_result = True
            compression_type = "gzip" if gzip_result else None

            if gzip_result:
                if not filename.endswith(".gz"):
                    filename += ".gz"

            key = f"{self.output_s3_prefix}/{filename}"
            logger.info(f"preparing ({filename})...")

            df_csv_buffer = StringIO()  # BytesIO()
            df = result["dataframe"]
            kwargs = self.to_csv_kwargs
            if "to_csv_kwargs" in result:
                for k, v in result["to_csv_kwargs"].items():
                    kwargs[k] = v
            logger.debug(f"csv output kwargs: {kwargs}")
            df.to_csv(df_csv_buffer, **kwargs)
            logger.info(f"preparing: SUCCESS!")

            logger.info(f"writing results to: s3://{self.output_s3_bucket}/{key}")
            df_csv_buffer.seek(0)  # reset file for reading
            encoded_buffer = BytesIO(df_csv_buffer.read().encode("utf8"))
            encoded_buffer.seek(0)
            S3.upload_fileobj(Fileobj=encoded_buffer, Bucket=self.output_s3_bucket, Key=key)
            logger.info("writing results: SUCCESS!")
            output_info = {"Bucket": self.output_s3_bucket, "Key": key}

            outputs_info.append(output_info)
        return outputs_info

    @property
    def key(self):
        """Define the output key"""
        now = datetime.datetime.now()
        prefix = self.__class__.__name__.lower()
        key = f"{prefix}/{self.filename_prefix}_{self.unique_hash.hexdigest()}_{now:%Y%m%d_%H%M%S}.csv"
        return key

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
