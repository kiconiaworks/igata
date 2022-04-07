import logging
from copy import deepcopy
from typing import Union
from urllib.parse import urljoin

import boto3
import requests
from requests.auth import HTTPBasicAuth

from .... import settings
from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")
SQS = boto3.client("sqs", endpoint_url=settings.SQS_ENDPOINT, region_name="ap-northeast-1")


class AframaxRecordOutputCtxManager(OutputCtxManagerBase):
    """Predictor.predict() resutls will use `put_records()` to output to the envar defined Aframax service"""

    """Notice that you can just use SQS message based input ctxmgr in case of input also uses aframax"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aframax_url = kwargs.get("aframax_url", None)
        self.aframax_basicauth_user = kwargs.get("aframax_basicauth_user", None)
        self.aframax_basicauth_password = kwargs.get("aframax_basicauth_password", None)
        self.aframax_prediction_key = kwargs.get("aframax_prediction_key", "prediction")
        assert self.aframax_url.startswith("http")

    @classmethod
    def required_kwargs(cls) -> tuple:
        """
        Define the required fields for Class instantiation.
        Fields defined here can be used as environment variables by prefixing the value with 'OUTPUT_CTXMGR_' and putting values in uppercase.

        Ex:
            OUTPUT_CTXMGR_AFRAMAX_URL
            OUTPUT_CTXMGR_AFRAMAX_BASICAUTH_USER
            OUTPUT_CTXMGR_AFRAMAX_BASICAUTH_PASSWORD
        """
        required = ("aframax_url", "aframax_basicauth_user", "aframax_basicauth_password")
        return required

    @classmethod
    def validate_result_records(cls, records: Union[dict, list]):
        """
        Checks whether the records format is correct.
        minimum valid format:
        {
            "request": {
                "job_id": "...",
                "request_payload": {...}
            },
            "prediction": JSON parsable anything
        }
        :param records: data that you want to validate
        :return: nothing, but AssertError will be thrown if data was bad format.
        """
        assert isinstance(records, dict), f"{cls.__name__} requires result records is not a {type(records)} but a dictionary."
        assert "request" in records.keys(), f"{cls.__name__} requires result records contains `request` key."
        assert "job_id" in records["request"].keys(), f"{cls.__name__} requires `request` part of result records contains `job_id` key."
        assert (
            "request_payload" in records["request"].keys()
        ), f"{cls.__name__} requires `request` part of result records contains `request_payload` key."
        assert "prediction" in records.keys(), f"{cls.__name__} requires result records contains `prediction` key."

    @staticmethod
    def compose_patch_url(aframax_url: str, records: dict) -> str:
        """compose aframax job patch url."""
        return urljoin(aframax_url, f"/jobs/{records['request']['job_id']}")

    def compose_patch_body(self, records: dict) -> dict:
        """compose actual posting body. Override here if you need."""
        patch_body = deepcopy(records["request"]["request_payload"])
        patch_body[self.aframax_prediction_key] = records["prediction"]
        return patch_body

    def put_records(self, records: Union[dict, list]) -> dict:
        """
        Call to send result defined in JSON parsable `message_body` to SQS.

        .. note::

            given `message_body` will be converted to JSON and sent to the defined Aframax service.

        """
        self.validate_result_records(records)
        patch_body = self.compose_patch_body(records)
        patch_url = self.compose_patch_url(self.aframax_url, records)
        response = requests.patch(patch_url, json=patch_body, auth=HTTPBasicAuth(self.aframax_basicauth_user, self.aframax_basicauth_password))
        summary = {"status_code": response.status_code}
        if 200 <= response.status_code <= 300:
            summary["error"] = ""
            summary["message"] = response.json()
        else:
            summary["message"] = []
            summary["error"] = response.text
        return summary

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)
