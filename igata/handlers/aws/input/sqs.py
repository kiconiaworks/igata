import csv
import json
import logging
import time
from collections.abc import Iterable
from multiprocessing.pool import ThreadPool
from typing import Dict, Generator, Tuple, Union

import boto3
import numpy as np
import pandas

from .... import settings
from ....utils import parse_s3_uri, prepare_csv_dataframe, prepare_csv_reader, prepare_images, s3_key_exists
from . import InputCtxManagerBase

logger = logging.getLogger("cliexecutor")

SQS = boto3.resource("sqs", endpoint_url=settings.SQS_ENDPOINT, region_name=settings.AWS_REGION)


class SQSMessageS3InputImageCtxManager(InputCtxManagerBase):
    """get_records() is called by results will use `put_records()` to output to the envar defined SQS Queue"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue_url = kwargs.get("sqs_queue_url")
        self.s3uri_keys = kwargs.get("s3uri_keys", settings.REQUEST_S3URI_KEYS)
        assert isinstance(self.s3uri_keys, Iterable)
        self.max_processing_requests = kwargs.get("max_processing_requests", settings.MAX_PROCESSING_REQUESTS)
        logger.info(f"max_processing_requests: {self.max_processing_requests}")
        self.processed_messages = []
        self.context_manager_specific_info_keys = ("bucket", "key", "download_time")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'INPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            INPUT_CTXMGR_SQS_QUEUE_URL
            INPUT_CTXMGR_S3URI_KEY
        """
        required = ("sqs_queue_url",)
        return required

    def get_records(self, *args, **kwargs) -> Generator[Tuple[np.array, dict], None, None]:
        """
        Receive available messages from queue upto MAX_PROCESSING_REQUESTS

        .. note::

            If a single message causes the len() > MAX_PROCESSING_REQUESTS, all requests in the message will be processed/included.

        """
        queue = SQS.Queue(url=self.sqs_queue_url)
        all_processing_requests: Union[list, dict] = []  # SQS messages (should be a *list*, may be given an single dict)

        estimated_visibility_timeout = self.max_processing_requests * settings.MAX_PER_REQUEST_PROCESSING_SECONDS
        logger.info(f"estimated_visibility_timeout: {estimated_visibility_timeout}")

        max_processing_requests_exceeded = False
        while True:  # NOTE: if a single 'sqs message' exceeds this count it will be accepted!
            # メッセージを取得
            messages = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=estimated_visibility_timeout, WaitTimeSeconds=0)
            logger.debug(f"queue.receive_messages() response: {messages}")
            if messages:
                logger.debug(f"Number of messages retrieved: {len(messages)}")
                for message in messages:
                    try:
                        processing_requests = json.loads(message.body)
                        logger.info(f"--> Adding Requests: {len(processing_requests)}")
                        all_processing_requests.extend(processing_requests)
                        self.processed_messages.append(message)  # for deleting on success
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSONDecodeError (message will be deleted and not processed!!!) message.body: {message.body}")
                        message.delete()
                    if len(all_processing_requests) >= self.max_processing_requests:
                        max_processing_requests_exceeded = True
                        break
            else:
                # メッセージがなくなったらbreak
                logger.debug(f"No messages returned in queue.receive_messages() request for queue_url: {self.sqs_queue_url}")
                break
            if max_processing_requests_exceeded:
                logger.info(
                    f"len(all_processing_requests)[{len(all_processing_requests)}] > "
                    f"self.max_processing_requests[{self.max_processing_requests}], breaking..."
                )
                break

        logger.info(f"Total Processing Requests [len(all_processing_request)]: {len(all_processing_requests)}")
        if all_processing_requests:
            if not isinstance(all_processing_requests, list):
                logger.warning(f"SQS MessageBody not list!!! Putting object in list: {all_processing_requests}")
                all_processing_requests = [all_processing_requests]

            for request in all_processing_requests:
                logger.info(f"Processing request: {request}")
                for s3uri_key in self.s3uri_keys:
                    s3uri = request[s3uri_key]
                    logger.debug(f"s3uri: {s3uri}")
                    bucket, key = parse_s3_uri(s3uri)

                    (bucket, key), image, download_time, error_message = prepare_images(bucket, key)
                    if error_message:
                        logger.error(f"error_message returned from prepare_images(): {error_message}")
                        # add error message to request in order to return info to user
                        if "errors" not in request:
                            request["errors"] = [error_message]
                        else:
                            if not request["errors"]:
                                request["errors"] = []
                            request["errors"].append(error_message)
                        logger.error(error_message)

                    info = {"bucket": bucket, "key": key, "download_time": download_time, "current_s3uri_key": s3uri_key}
                    logger.debug(f"Adding request attributes to info: {request}")
                    info.update(request)  # add request info to returned info
                    yield image, info

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if all(o is None for o in (exception_type, exception_value, traceback)):
            logger.debug("deleting (SQS) self.processed_messages...")
            for message in self.processed_messages:
                message.delete()
        else:
            logger.error("exception occurred, SQS messages NOT deleted!")
            if self.processed_messages:
                logger.info(f"changing visibility for self.processed_messages: {len(self.processed_messages)}")
                for message in self.processed_messages:
                    message.change_visibility(VisibilityTimeout=settings.SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)


class SQSMessageS3InputCSVReaderCtxManager(InputCtxManagerBase):
    """get_records() is called by results will use `put_records()` to output to the envar defined SQS Queue"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue_url = kwargs.get("sqs_queue_url")
        self.s3uri_keys = kwargs.get("s3uri_keys", settings.REQUEST_S3URI_KEYS)
        assert isinstance(self.s3uri_keys, Iterable)
        self.max_processing_requests = kwargs.get("max_processing_requests", settings.MAX_PROCESSING_REQUESTS)
        logger.info(f"max_processing_requests: {self.max_processing_requests}")
        self.processed_messages = []
        self.context_manager_specific_info_keys = ("bucket", "key", "download_time")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'INPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            INPUT_CTXMGR_SQS_QUEUE_URL
            INPUT_CTXMGR_S3URI_KEY
        """
        required = ("sqs_queue_url",)
        return required

    def _get_processing_requests(self) -> Union[list, dict]:
        """Collect processing requests from SQS Queue"""
        queue = SQS.Queue(url=self.sqs_queue_url)
        all_processing_requests: Union[list, dict] = []  # SQS messages (should be a *list*, may be given an single dict)

        estimated_visibility_timeout = self.max_processing_requests * settings.MAX_PER_REQUEST_PROCESSING_SECONDS
        logger.info(f"estimated_visibility_timeout: {estimated_visibility_timeout}")

        max_processing_requests_exceeded = False
        while True:  # NOTE: if a single 'sqs message' exceeds this count it will be accepted!
            # メッセージを取得
            messages = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=estimated_visibility_timeout, WaitTimeSeconds=0)
            logger.debug(f"queue.receive_messages() response: {messages}")
            if messages:
                logger.debug(f"Number of messages retrieved: {len(messages)}")
                for message in messages:
                    try:
                        processing_requests = json.loads(message.body)
                        logger.info(f"--> Adding Requests: {len(processing_requests)}")
                        all_processing_requests.extend(processing_requests)
                        self.processed_messages.append(message)  # for deleting on success
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSONDecodeError (message will be deleted and not processed!!!) message.body: {message.body}")
                        message.delete()
                    if len(all_processing_requests) >= self.max_processing_requests:
                        max_processing_requests_exceeded = True
                        break
            else:
                # メッセージがなくなったらbreak
                logger.debug(f"No messages returned in queue.receive_messages() request for queue_url: {self.sqs_queue_url}")
                break
            if max_processing_requests_exceeded:
                logger.info(
                    f"len(all_processing_requests)[{len(all_processing_requests)}] > "
                    f"self.max_processing_requests[{self.max_processing_requests}], breaking..."
                )
                break
        return all_processing_requests

    def get_records(self, *args, **kwargs) -> Generator[Tuple[Union[csv.reader, csv.DictReader], dict], None, None]:
        """
        Receive available messages from queue upto MAX_PROCESSING_REQUESTS

        .. note::

            If a single message causes the len() > MAX_PROCESSING_REQUESTS, all requests in the message will be processed/included.

        """
        processing_requests = self._get_processing_requests()
        logger.info(f"Total Processing Requests [len(all_processing_request)]: {len(processing_requests)}")
        if processing_requests:
            if not isinstance(processing_requests, Iterable):
                logger.warning(f"SQS MessageBody not list!!! Putting object in list: {processing_requests}")
                processing_requests = [processing_requests]

            args = []
            requests_mapping = {}
            for request in processing_requests:
                for s3uri_key in self.s3uri_keys:
                    request["current_s3uri_key"] = s3uri_key
                    s3uri = request[s3uri_key]
                    bucket, key = parse_s3_uri(s3uri)
                    logger.info(f"parser_s3_uri() bucket: {bucket}")
                    logger.info(f"parser_s3_uri() key: {key}")
                    args.append((bucket, key))
                    requests_mapping[(bucket, key)] = request

            pool = ThreadPool(settings.DOWNLOAD_WORKERS)
            for (bucket, key), csvreader, download_time, error_message in pool.starmap(prepare_csv_reader, args):
                request = requests_mapping[(bucket, key)]
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

                info = {"bucket": bucket, "key": key, "download_time": download_time}
                logger.debug(f"Adding request attributes to info: {request}")
                info.update(request)  # add request info to returned info
                yield csvreader, info

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if all(o is None for o in (exception_type, exception_value, traceback)):
            logger.debug("deleting (SQS) self.processed_messages...")
            for message in self.processed_messages:
                message.delete()
        else:
            logger.error("exception occurred, SQS messages NOT deleted!")
            if self.processed_messages:
                logger.info(f"changing visibility for self.processed_messages: {len(self.processed_messages)}")
                for message in self.processed_messages:
                    message.change_visibility(VisibilityTimeout=settings.SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)


class SQSMessagePassthroughCtxManager(InputCtxManagerBase):

    """
    SQS Message Fields defined by 's3uri_keys' are checked for existence before being yielded by get_records()
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue_url = kwargs.get("sqs_queue_url")
        self.s3uri_keys = kwargs.get("s3uri_keys", settings.REQUEST_S3URI_KEYS)
        assert isinstance(self.s3uri_keys, Iterable)
        self.max_processing_requests = kwargs.get("max_processing_requests", settings.MAX_PROCESSING_REQUESTS)
        logger.info(f"max_processing_requests: {self.max_processing_requests}")
        self.processed_messages = []
        self.context_manager_specific_info_keys = ("s3uri_keys", "sqs_queue_url", "max_processing_requests")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'INPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            INPUT_CTXMGR_SQS_QUEUE_URL
            INPUT_CTXMGR_S3URI_KEY
        """
        required = ("sqs_queue_url",)
        return required

    def _get_processing_requests(self) -> Union[list, dict]:
        """Collect processing requests from SQS Queue"""
        queue = SQS.Queue(url=self.sqs_queue_url)
        all_processing_requests: Union[list, dict] = []  # SQS messages (should be a *list*, may be given an single dict)

        estimated_visibility_timeout = self.max_processing_requests * settings.MAX_PER_REQUEST_PROCESSING_SECONDS
        logger.info(f"estimated_visibility_timeout: {estimated_visibility_timeout}")

        max_processing_requests_exceeded = False
        while True:  # NOTE: if a single 'sqs message' exceeds this count it will be accepted!
            # メッセージを取得
            messages = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=estimated_visibility_timeout, WaitTimeSeconds=0)
            logger.debug(f"queue.receive_messages() response: {messages}")
            if messages:
                logger.debug(f"Number of messages retrieved: {len(messages)}")
                for message in messages:
                    try:
                        processing_requests = json.loads(message.body)
                        logger.info(f"--> Adding Requests: {len(processing_requests)}")
                        all_processing_requests.extend(processing_requests)
                        self.processed_messages.append(message)  # for deleting on success
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSONDecodeError (message will be deleted and not processed!!!) message.body: {message.body}")
                        message.delete()
                    if len(all_processing_requests) >= self.max_processing_requests:
                        max_processing_requests_exceeded = True
                        break
            else:
                # メッセージがなくなったらbreak
                logger.debug(f"No messages returned in queue.receive_messages() request for queue_url: {self.sqs_queue_url}")
                break
            if max_processing_requests_exceeded:
                logger.info(
                    f"len(all_processing_requests)[{len(all_processing_requests)}] > "
                    f"self.max_processing_requests[{self.max_processing_requests}], breaking..."
                )
                break
        return all_processing_requests

    def get_records(self, *args, **kwargs) -> Generator[Tuple[dict, dict], None, None]:
        """
        Receive available messages from queue upto MAX_PROCESSING_REQUESTS.

        .. note::

            If a single message causes the len() > MAX_PROCESSING_REQUESTS, all requests in the message will be processed/included.

        """
        processing_requests = self._get_processing_requests()
        logger.info(f"Total Processing Requests [len(all_processing_request)]: {len(processing_requests)}")
        if processing_requests:
            if not isinstance(processing_requests, Iterable):
                logger.warning(f"SQS MessageBody not list!!! Putting object in list: {processing_requests}")
                processing_requests = [processing_requests]

            for request in processing_requests:
                # check that keys exist
                info = {
                    "s3uri_keys": self.s3uri_keys,
                    "sqs_queue_url": self.sqs_queue_url,
                    "max_processing_requests": self.max_processing_requests,
                    "is_valid": True,
                }
                for s3uri_key in self.s3uri_keys:
                    s3uri = request[s3uri_key]
                    bucket, key = parse_s3_uri(s3uri)
                    logger.info(f"parser_s3_uri() bucket: {bucket}")
                    logger.info(f"parser_s3_uri() key: {key}")
                    if not s3_key_exists(bucket, key):
                        info["is_valid"] = False
                        info["errors"] = [f"s3uri({s3uri}) DoesNotExist: request={request}"]
                yield request, info
        else:
            logger.warning("no processing_requests found!")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if all(o is None for o in (exception_type, exception_value, traceback)):
            logger.debug("deleting (SQS) self.processed_messages...")
            for message in self.processed_messages:
                message.delete()
        else:
            logger.error("exception occurred, SQS messages NOT deleted!")
            if self.processed_messages:
                logger.info(f"changing visibility for self.processed_messages: {len(self.processed_messages)}")
                for message in self.processed_messages:
                    message.change_visibility(VisibilityTimeout=settings.SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)


class SQSMessageS3InputCSVPandasDataFrameCtxManager(InputCtxManagerBase):
    """get_records() is called by results will use `put_records()` to output to the envar defined SQS Queue"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue_url = kwargs.get("sqs_queue_url")
        self.s3uri_keys = kwargs.get("s3uri_keys", settings.REQUEST_S3URI_KEYS)
        assert isinstance(self.s3uri_keys, Iterable)
        self.max_processing_requests = kwargs.get("max_processing_requests", settings.MAX_PROCESSING_REQUESTS)
        logger.info(f"max_processing_requests: {self.max_processing_requests}")
        self.processed_messages = []
        self.context_manager_specific_info_keys = ("s3uri_keys", "sqs_queue_url", "max_processing_requests", "request", "is_valid")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'INPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            INPUT_CTXMGR_SQS_QUEUE_URL
            INPUT_CTXMGR_S3URI_KEY
        """
        required = ("sqs_queue_url",)
        return required

    def _get_processing_requests(self) -> Union[list, dict]:
        """Collect processing requests from SQS Queue"""
        queue = SQS.Queue(url=self.sqs_queue_url)
        all_processing_requests: Union[list, dict] = []  # SQS messages (should be a *list*, may be given an single dict)

        estimated_visibility_timeout = self.max_processing_requests * settings.MAX_PER_REQUEST_PROCESSING_SECONDS
        logger.info(f"estimated_visibility_timeout: {estimated_visibility_timeout}")

        max_processing_requests_exceeded = False
        while True:  # NOTE: if a single 'sqs message' exceeds this count it will be accepted!
            # メッセージを取得
            messages = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=estimated_visibility_timeout, WaitTimeSeconds=0)
            logger.debug(f"queue.receive_messages() response: {messages}")
            if messages:
                logger.debug(f"Number of messages retrieved: {len(messages)}")
                for message in messages:
                    try:
                        processing_requests = json.loads(message.body)
                        logger.info(f"--> Adding Requests: {len(processing_requests)}")
                        all_processing_requests.extend(processing_requests)
                        self.processed_messages.append(message)  # for deleting on success
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSONDecodeError (message will be deleted and not processed!!!) message.body: {message.body}")
                        message.delete()
                    if len(all_processing_requests) >= self.max_processing_requests:
                        max_processing_requests_exceeded = True
                        break
            else:
                # メッセージがなくなったらbreak
                logger.debug(f"No messages returned in queue.receive_messages() request for queue_url: {self.sqs_queue_url}")
                break
            if max_processing_requests_exceeded:
                logger.info(
                    f"len(all_processing_requests)[{len(all_processing_requests)}] > "
                    f"self.max_processing_requests[{self.max_processing_requests}], breaking..."
                )
                break
        return all_processing_requests

    def get_records(self, *args, **kwargs) -> Generator[Tuple[Dict[str, pandas.DataFrame], dict], None, None]:
        """
        Receive available messages from queue upto MAX_PROCESSING_REQUESTS
        Downloads fields defined as 's3uri_keys' from s3 to pandas DataFrame(s)

        .. note::

            If a single message causes the len() > MAX_PROCESSING_REQUESTS, all requests in the message will be processed/included.

        """
        processing_requests = self._get_processing_requests()
        logger.info(f"Total Processing Requests [len(all_processing_request)]: {len(processing_requests)}")
        if processing_requests:
            if not isinstance(processing_requests, Iterable):
                logger.warning(f"SQS MessageBody not list!!! Putting object in list: {processing_requests}")
                processing_requests = [processing_requests]

            for request in processing_requests:
                record = {}
                info = {
                    "s3uri_keys": self.s3uri_keys,
                    "sqs_queue_url": self.sqs_queue_url,
                    "max_processing_requests": self.max_processing_requests,
                    "is_valid": True,
                    "errors": [],
                }
                args = []
                s3uri_key_mapping = {}
                for s3uri_key in self.s3uri_keys:
                    s3uri = request[s3uri_key]
                    bucket, key = parse_s3_uri(s3uri)
                    logger.info(f"parser_s3_uri() bucket: {bucket}")
                    logger.info(f"parser_s3_uri() key: {key}")
                    # s3uri, encoding, delimiter, header_lines
                    args.append((bucket, key))
                    s3uri_key_mapping[(bucket, key)] = s3uri_key
                    if not s3_key_exists(bucket, key):
                        info["is_valid"] = False
                        info["errors"].append(f"s3uri({s3uri}) DoesNotExist: request={request}")

                if info["is_valid"]:
                    download_start = time.time()
                    pool = ThreadPool(settings.DOWNLOAD_WORKERS)

                    for (bucket, key), df, _, error_message in pool.starmap(prepare_csv_dataframe, args):
                        original_s3uri_key = s3uri_key_mapping[(bucket, key)]
                        dataframe_key = f"{original_s3uri_key}__dataframe"

                        # add to record
                        record[dataframe_key] = df

                        if error_message:
                            # add error message to request in order to return info to user
                            info["errors"].append(error_message)
                            logger.error(error_message)

                    download_end = time.time()
                    download_time = download_end - download_start
                    info["download_time"] = download_time

                logger.debug(f"Adding request attributes to info: {request}")
                info["request"] = request  # add request info to returned info
                yield record, info

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if all(o is None for o in (exception_type, exception_value, traceback)):
            logger.debug("deleting (SQS) self.processed_messages...")
            for message in self.processed_messages:
                message.delete()
        else:
            logger.error("exception occurred, SQS messages NOT deleted!")
            if self.processed_messages:
                logger.info(f"changing visibility for self.processed_messages: {len(self.processed_messages)}")
                for message in self.processed_messages:
                    message.change_visibility(VisibilityTimeout=settings.SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)
