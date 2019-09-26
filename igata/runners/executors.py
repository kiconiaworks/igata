import gc
import json
import logging
import os
import time
import traceback
from collections import Counter, defaultdict
from typing import Tuple, Union

import boto3
from botocore.exceptions import ClientError

from .. import settings
from ..handlers.aws.input import InputImageCtxManagerBase
from ..handlers.aws.output import OutputCtxManagerBase
from ..predictors import PredictorBase
from ..utils import serialize_json_and_chunk_by_bytes

logger = logging.getLogger("cliexecutor")

PREDICTOR_MODULE = os.getenv("PREDICTOR_MODULE", None)
DEFAULT_PREDICTOR_CLASS_NAME = "Predictor"
PREDICTOR_CLASS_NAME = os.getenv("PREDICTOR_CLASS_NAME", DEFAULT_PREDICTOR_CLASS_NAME)
REQUEST_UNIQUE_ID_FIELDNAME = os.getenv("REQUEST_UNIQUE_ID_FIELDNAME", "request_id")
REQUEST_SNS_TOPIC_ARN_FIELDNAME = os.getenv("REQUEST_SNS_TOPIC_ARN_FIELDNAME", "sns_topic_arn")
REQUESTS_COMPLETE_SNS_TOPIC_ARN = os.getenv("REQUESTS_COMPLETE_SNS_TOPIC_ARN", None)

# expected this is value provided in predictor.predict() dict result
DEFAULT_PREDICTOR_RESULTS_KEYNAME = "result"
PREDICTOR_RESULTS_KEYNAME = os.getenv("PREDICTOR_RESULTS_KEYNAME", DEFAULT_PREDICTOR_RESULTS_KEYNAME)
SNS_MAX_MESSAGE_SIZE_BYTES = 262144

SNS = boto3.client("sns", region_name=settings.AWS_REGION, endpoint_url=settings.SNS_ENDPOINT)


class PredictionExecutor:
    """Main executor for running user-defined Predictors (Predictor classes that sub-class igata.predictors.PredictorBase)"""

    def __init__(
        self,
        predictor: PredictorBase,
        input_ctx_manager: InputImageCtxManagerBase,
        input_settings: dict,
        output_ctx_manager: OutputCtxManagerBase,
        output_settings: dict,
    ):
        self.predictor = predictor
        self.input_ctx_manager = input_ctx_manager
        self._input_settings = input_settings
        self.output_ctx_manager = output_ctx_manager
        self._output_settings = output_settings

        # Log predictor version
        predictor_version = "not defined"
        if hasattr(self.predictor, "__version__"):
            predictor_version = self.predictor.__version__
        self.predictor_version = predictor_version
        logger.info(f"predictor({self.predictor.__class__.__name__}).__version__: {self.predictor_version}")

    def _prepare_sns_notification_data(self, info: dict) -> Tuple[str, str]:
        """Get the SNS Topic ARN and RequestId from info if defined"""
        sns_topic_arn = None
        request_id = None
        if REQUEST_UNIQUE_ID_FIELDNAME in info:
            sns_topic_arn = None
            if REQUEST_SNS_TOPIC_ARN_FIELDNAME in info:
                sns_topic_arn = info[REQUEST_SNS_TOPIC_ARN_FIELDNAME]
            elif REQUESTS_COMPLETE_SNS_TOPIC_ARN:
                sns_topic_arn = REQUESTS_COMPLETE_SNS_TOPIC_ARN

            if sns_topic_arn:
                logger.debug(f"using sns_topic_arn: {sns_topic_arn}")
                request_id = info[REQUEST_UNIQUE_ID_FIELDNAME]
        return sns_topic_arn, request_id

    @staticmethod
    def _handle_sns_notifications(notifications: Union[dict, None]) -> int:
        """handle sns notifications if defined"""
        published_message_count = 0
        if notifications:
            logger.debug("sending SNS notifications...")
            for sns_topic_arn, request_ids in notifications.items():
                logger.info(f"sending request_ids to SNS_TOPIC_ARN({sns_topic_arn}) ...")
                for request_ids_json in serialize_json_and_chunk_by_bytes(request_ids, max_bytes=SNS_MAX_MESSAGE_SIZE_BYTES):
                    try:
                        SNS.publish(TargetArn=sns_topic_arn, Message=json.dumps({"default": request_ids_json}), MessageStructure="json")
                        published_message_count += 1
                    except SNS.exceptions.NotFoundException as e:
                        logger.error(f"(NotFoundException) Unable to publish to given SNS_TOPIC_ARN({sns_topic_arn}: {e.args}")
                    except ClientError as e:
                        logger.error(f"(NotFoundException) Unable to publish to given SNS_TOPIC_ARN({sns_topic_arn}: {e.args}")
        return published_message_count

    def execute(self, inputs: Union[list, None] = None) -> Counter:
        """
        Run Predictor.predict() on the given input calling 'preprocess_input' and 'postprocess_output' methods if defined in Predictor class.

        inputs may be None if 'SQSRecordS3InputImageCtxManager' is used.
        (SQSRecordS3InputImageCtxManager will pull messages from the sqs queue)

        .. note::

            On INPUT error, the request information is returned with the 'errors' field added.

        :returns: Summary of timings
        """
        summary_results = Counter()
        sns_notifications = defaultdict(list)
        meta = {"input_settings": self._input_settings, "output_settings": self._output_settings, "request_info": None}
        with self.input_ctx_manager(**self._input_settings) as input_ctxmgr, self.output_ctx_manager(**self._output_settings) as output_ctxmgr:
            for record, info in input_ctxmgr.get_records(inputs):  # process records as they become available
                if PREDICTOR_RESULTS_KEYNAME in info:
                    info.pop(PREDICTOR_RESULTS_KEYNAME)  # to assure that None does not overwrite actual result
                if hasattr(record, "any") and not record.any():  # or not record:  # support both numpy empty array and None
                    # handle error case
                    info["result"] = None
                    record_results = info
                    logger.error(f"Unable to process image request error info will be returned: results={record_results}")
                    summary_results["errors"] += 1
                else:
                    record_in_error = False
                    if "download_time" in info:
                        summary_results["total_download_duration"] += info["download_time"]
                    meta["request_info"] = info
                    if hasattr(self.predictor, "preprocess_input"):
                        preprocess_start = time.time()
                        record = self.predictor.preprocess_input(record, meta)
                        preprocess_end = time.time()
                        preprocess_duration = round(preprocess_end - preprocess_start, 4)
                        logger.info(f"preprocess_duration: {preprocess_duration}")
                        summary_results["total_preprocess_duration"] += preprocess_duration

                    predict_start = time.time()
                    try:
                        logger.debug(f"calling self.predictor.predict(record, meta): meta={meta}")
                        record_results = self.predictor.predict(record, meta)
                        assert isinstance(record_results, dict)
                        # add request data to result record
                        if "request_info" in meta and meta["request_info"]:
                            # remove input_ctxmgr specific keys
                            request_info = {k: v for k, v in meta["request_info"].items() if k not in input_ctxmgr.context_manager_specific_info_keys}
                            record_results.update(request_info)
                            logger.debug(f"Added request info to resulting record_results: {record_results}")

                    except Exception as e:
                        # collect traceback
                        traceback_lines = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                        tb = "".join(traceback_lines)
                        error_message = (
                            f"Exception in ({PREDICTOR_MODULE}) {PREDICTOR_CLASS_NAME}.predict() " f"({e.args})\n:Request: {info}\nTraceback:\n{tb}"
                        )
                        logger.error(error_message)
                        if "errors" not in info:
                            info["errors"] = [error_message]
                        else:
                            if not info["errors"]:
                                info["errors"] = []
                            info["errors"].append(error_message)
                        record_results = info
                        record_in_error = True
                        summary_results["errors"] += 1

                    logger.debug(f"predictor.predict() results: {record_results}")
                    predict_end = time.time()
                    predict_duration = round(predict_end - predict_start, 4)
                    logger.info(f"predict_duration: {predict_duration}")
                    summary_results["total_predict_duration"] += predict_duration
                    summary_results["total_predictions"] += 1

                    if hasattr(self.predictor, "postprocess_output") and not record_in_error:
                        postprocess_start = time.time()
                        record_results = self.predictor.postprocess_output(record_results, meta)
                        logger.debug(f"predictor.postprocess_output() results: {record_results}")
                        postprocess_end = time.time()
                        postprocess_duration = round(postprocess_end - postprocess_start, 4)
                        logger.info(f"postprocess_duration: {postprocess_duration}")
                        summary_results["total_postprocess_duration"] += postprocess_duration

                put_start = time.time()
                response = output_ctxmgr.put_record(record_results)
                logger.debug(f"response: {response}")
                put_end = time.time()
                put_duration = round(put_end - put_start, 4)
                logger.info(f"put_duration: {put_duration}")
                summary_results["total_put_duration"] += put_duration

                # handle SNS reporting
                sns_topic_arn, request_id = self._prepare_sns_notification_data(info)
                if sns_topic_arn and request_id:
                    sns_notifications[sns_topic_arn].append(request_id)
                gc.collect()  # force garbage collection post predict
            context_manager_exit_start = time.time()

        # put time may include operations on output_ctxmgr exit
        # -> update total_put_duration to include output_ctxmgr exit duration
        context_manager_exit_end = time.time()
        context_manager_exit_duration = round(context_manager_exit_end - context_manager_exit_start, 4)
        summary_results["context_manager_exit_duration"] = context_manager_exit_duration
        logger.info(f"(input|output) context_manager_exit_duration: {context_manager_exit_duration}")

        published_sns_message_count = self._handle_sns_notifications(sns_notifications)
        logger.info(f"published_sns_message_count: {published_sns_message_count}")
        total_processing_duration = (
            summary_results["total_preprocess_duration"] + summary_results["total_predict_duration"] + summary_results["total_postprocess_duration"]
        )
        summary_results["total_processing_duration"] = total_processing_duration
        if summary_results["total_predictions"] > 0:
            per_prediction_duration = total_processing_duration / summary_results["total_predictions"]
            summary_results["per_prediction_duration"] = per_prediction_duration

        return summary_results
