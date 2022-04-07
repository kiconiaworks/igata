import logging
from copy import deepcopy
from typing import Any, Union
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")


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
    def validate_result_record(cls, record: Union[dict, Any]):
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
        :param record: data that you want to validate
        :return: nothing, but AssertError will be thrown if data was bad format.
        """
        assert isinstance(record, dict), f"{cls.__name__} requires result record is not a {type(record)} but a dictionary."
        assert "request" in record.keys(), f"{cls.__name__} requires result record contains `request` key."
        assert "job_id" in record["request"].keys(), f"{cls.__name__} requires `request` part of result record contains `job_id` key."
        assert (
            "request_payload" in record["request"].keys()
        ), f"{cls.__name__} requires `request` part of result record contains `request_payload` key."
        assert "prediction" in record.keys(), f"{cls.__name__} requires result record contains `prediction` key."

    @staticmethod
    def compose_patch_url(aframax_url: str, record: dict) -> str:
        """compose aframax job patch url."""
        return urljoin(aframax_url, f"/jobs/{record['request']['job_id']}")

    def compose_patch_body(self, record: dict) -> dict:
        """compose actual posting body. Override here if you need."""
        patch_body = deepcopy(record["request"]["request_payload"])
        patch_body[self.aframax_prediction_key] = record["prediction"]
        return patch_body

    def put_records(self, records: Union[dict, list]) -> dict:
        """
        Call to send result defined in JSON parsable `message_body` to SQS.

        .. note::

            given `message_body` will be converted to JSON and sent to the defined Aframax service.

        """
        summary = {"success_count": 0, "error_count": 0, "details": []}
        for record in records:
            self.validate_result_record(record)
            patch_body = self.compose_patch_body(record)
            patch_url = self.compose_patch_url(self.aframax_url, record)
            logger.info(f"Sending Patch request to Aframax at {patch_url} with body {patch_body}")
            response = requests.patch(patch_url, json=patch_body, auth=HTTPBasicAuth(self.aframax_basicauth_user, self.aframax_basicauth_password))
            a_result = {"status_code": response.status_code}
            if 200 <= response.status_code <= 300:
                logger.info(f"Patch request was processed successfully with response {response.text}")
                summary["success_count"] += 1
                a_result["error"] = ""
                a_result["message"] = response.json()
            else:
                logger.warn(f"Patch request was processed successfully with status {response.status_code} and response {response.text}")
                summary["error_count"] += 1
                a_result["message"] = []
                a_result["error"] = response.text
            summary["details"].append(a_result)
        return summary

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)
