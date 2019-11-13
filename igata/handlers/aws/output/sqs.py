import json
import logging
from collections import Counter
from typing import Union

import boto3

from .... import settings
from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")
SQS = boto3.client("sqs", endpoint_url=settings.SQS_ENDPOINT, region_name="ap-northeast-1")


class SQSRecordOutputCtxManager(OutputCtxManagerBase):
    """Predictor.predict() resutls will use `put_records()` to output to the envar defined SQS Queue"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqs_queue_url = kwargs.get("sqs_queue_url", None)
        assert self.sqs_queue_url.startswith("http")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'OUTPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            OUTPUT_CTXMGR_SQS_QUEUE_URL
        """
        required = ("sqs_queue_url",)
        return required

    def put_records(self, records: Union[dict, list]):
        """
        Call to send result defined in JSON parsable `message_body` to SQS.

        .. note::

            given `message_body` will be converted to JSON and sent to the defined SQS Queue.

        """
        summary = Counter()
        max_sqs_message_body_bytes = 2048
        for record in records:
            message_body_json = json.dumps(record)
            message_body_utf8_bytes = len(message_body_json.encode("utf8"))
            logger.info(f"Message Bytes={message_body_utf8_bytes}")
            if message_body_utf8_bytes > max_sqs_message_body_bytes:
                logger.error(f"message_body_utf8_bytes({message_body_utf8_bytes}) > max_sqs_message_body_bytes({max_sqs_message_body_bytes})")
            logger.debug(f"Queuing({self.sqs_queue_url}): {record}")
            response = SQS.send_message(QueueUrl=self.sqs_queue_url, MessageBody=message_body_json)
            logger.debug(f"response: {response}")
            summary["sent_messages"] += 1
        return summary

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)
