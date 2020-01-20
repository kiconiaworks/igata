import csv
import datetime
import logging
import os
from multiprocessing.pool import ThreadPool
from typing import Generator, List, Tuple, Union

import numpy as np

from .... import settings
from ....utils import parse_s3_uri, prepare_csv_reader, prepare_images
from . import InputCtxManagerBase

logger = logging.getLogger("cliexecutor")

S3BUCKET_OUTPUT_FILENAME_PREFIX = os.getenv("S3BUCKET_OUTPUT_FILENAME_PREFIX", "results-xyz34567yh-")
JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")


class S3BucketImageInputCtxManager(InputCtxManagerBase):
    """Context manager for handling S3 Uris for Image files as `get_records` input"""

    @classmethod
    def required_kwargs(cls) -> Tuple:
        """Define the returning value here to force the field to be provided to this class on instantiation"""
        return tuple()

    def get_records(self, s3_uris: List[str]) -> Generator[Tuple[np.array, dict], None, None]:
        """
        Download the image data from the given s3_uris yielding the results as
        :param s3_uris:
        :return:
        """
        if not s3_uris:
            logger.warning(f"s3_uris not given as input!")
        # prepare arguments for processing
        if s3_uris:
            args = []
            logger.debug(f"s3_uris: {s3_uris}")
            for s3_uri in s3_uris:
                bucket, key = parse_s3_uri(s3_uri)
                logger.info(f"parser_s3_uri() bucket: {bucket}")
                logger.info(f"parser_s3_uri() key: {key}")
                args.append((bucket, key))

            pool = ThreadPool(settings.DOWNLOAD_WORKERS)
            for (bucket, key), image, download_time, error_message in pool.starmap(prepare_images, args):
                info = {}
                if error_message:
                    # add error message to request in order to return info to user
                    if "errors" not in info:
                        info["errors"] = [error_message]
                    else:
                        if not info["errors"]:
                            info["errors"] = []

                        info["errors"].append(error_message)
                    logger.error(error_message)

                info["bucket"] = bucket
                info["key"] = key
                info["download_time"] = download_time

                yield image, info

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass


class S3BucketCSVInputCtxManager(InputCtxManagerBase):
    """Context manager for handling S3 Uris for CSV files as `get_records` input"""

    def __init__(self, reader: Union[csv.reader, csv.DictReader] = csv.DictReader, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert reader in (csv.reader, csv.DictReader)
        self.reader = reader

    @classmethod
    def required_kwargs(cls) -> Tuple:
        """Define the returning value here to force the field to be provided to this class on instantiation"""
        return tuple()

    def get_records(self, s3_uris: List[str]) -> Generator[Tuple[Union[csv.reader, csv.DictReader], dict], None, None]:
        """
        Download the image data from the given s3_uris yielding the results as
        :param s3_uris:
        :return:
        """
        if not s3_uris:
            logger.warning(f"s3_uris not given as input!")
        # prepare arguments for processing
        if s3_uris:
            args = []
            logger.debug(f"s3_uris: {s3_uris}")
            for s3_uri in s3_uris:
                bucket, key = parse_s3_uri(s3_uri)
                logger.info(f"parser_s3_uri() bucket: {bucket}")
                logger.info(f"parser_s3_uri() key: {key}")
                args.append((bucket, key, self.reader))

            pool = ThreadPool(settings.DOWNLOAD_WORKERS)
            for (bucket, key), csvreader, download_time, error_message in pool.starmap(prepare_csv_reader, args):
                info = {}
                if error_message:
                    # add error message to request in order to return info to user
                    if "errors" not in info:
                        info["errors"] = [error_message]
                    else:
                        if not info["errors"]:
                            info["errors"] = []

                        info["errors"].append(error_message)
                    logger.error(error_message)

                info["bucket"] = bucket
                info["key"] = key
                info["download_time"] = download_time

                yield csvreader, info

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
