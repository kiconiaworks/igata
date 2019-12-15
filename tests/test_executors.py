import json
import logging
import sys
from pathlib import Path
from time import sleep

from dummypredictor.predictors import (
    DummyPredictorNoInputNoOutput,
    DummyPredictorNoInputNoOutputVariableOutput,
    DummyPredictorNoInputNoOutputWithPredictTimeout5s,
)
from igata.cli import execute_prediction
from igata.handlers.aws.input.s3 import S3BucketImageInputCtxManager
from igata.handlers.aws.input.sqs import SQSMessageS3InputImageCtxManager
from igata.handlers.aws.output.dynamodb import DynamodbOutputCtxManager
from igata.handlers.aws.output.sqs import SQSRecordOutputCtxManager
from igata.runners.executors import PredictionExecutor

from .utils import (
    _create_sns_topic,
    _dynamodb_create_table,
    _dynamodb_delete_table,
    _get_queue_url,
    setup_teardown_s3_file,
    setup_teardown_sqs_queue,
    sqs_queue_send_message,
)

# add test root to PATH in order to load dummypredictor
BASE_TEST_DIRECTORY = Path(__file__).absolute().parent
sys.path.append(str(BASE_TEST_DIRECTORY))
sys.path.append(str(BASE_TEST_DIRECTORY.parent))

# reduce logging output from noisy packages
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("pynamodb.connection.base").setLevel(logging.WARNING)


TEST_BUCKETNAME = "test-bucket-local"
TEST_IMAGE_FILENAME = "pacioli-512x512.png"
TEST_IMAGE_FILEPATH = BASE_TEST_DIRECTORY / "data" / "images" / TEST_IMAGE_FILENAME
assert TEST_IMAGE_FILEPATH.exists()

TEST_IMAGE_S3URI = f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}"

TEST_SQS_OUTPUT_QUEUENAME = "test-output-queue"
TEST_SQS_INPUT_QUEUENAME = "test-input-queue"

TEST_SNS_TOPIC_NAME = "sns-test-topic"
TEST_SNS_TOPIC_ARN = _create_sns_topic(topic_name=TEST_SNS_TOPIC_NAME)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_with_predictor_noinput_nooutput():
    predictor = DummyPredictorNoInputNoOutput()

    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SQSRecordOutputCtxManager,
        output_settings=output_settings,
    )

    s3uri_inputs = [TEST_IMAGE_S3URI]
    execute_summary = executor.execute(s3uri_inputs)
    assert execute_summary


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_prediction_multiple_inputs():
    s3uris = [
        f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}",
        f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}",
        f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}",
    ]

    predictor = DummyPredictorNoInputNoOutput()

    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    execute_summary = execute_prediction(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SQSRecordOutputCtxManager,
        output_settings=output_settings,
        inputs=s3uris,
    )
    assert execute_summary


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_prediction_bad_inputs():
    s3uris = [f"s3://{TEST_BUCKETNAME}/a.jpg", f"s3://nobucket/{TEST_IMAGE_FILENAME}", f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}"]

    predictor = DummyPredictorNoInputNoOutput()

    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    execute_summary = execute_prediction(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SQSRecordOutputCtxManager,
        output_settings=output_settings,
        inputs=s3uris,
    )
    assert execute_summary
    assert execute_summary["errors"] == 2


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_log_predictor_version():
    """Confirm that the predictor.__version__ value is properly handled"""
    predictor = DummyPredictorNoInputNoOutput()
    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SQSRecordOutputCtxManager,
        output_settings=output_settings,
    )
    default_version = "0.1.0"
    assert executor.predictor_version == default_version

    assigned_version = "0.5.1"
    predictor.__version__ = assigned_version
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SQSRecordOutputCtxManager,
        output_settings=output_settings,
    )
    assert executor.predictor_version == assigned_version


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_INPUT_QUEUENAME)
def test_executor_requests_with_sns():
    requests = [{"request_id": "r-11111", "s3_uri": f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}", "sns_topic_arn": TEST_SNS_TOPIC_ARN}]

    predictor = DummyPredictorNoInputNoOutputVariableOutput(
        result={"request_id": "r-11111", "result": [{"prediction": 0.11}], "sns_topic_arn": TEST_SNS_TOPIC_ARN, "s3_uri": "s3://bucket/key.png"}
    )

    queue_url = _get_queue_url(TEST_SQS_INPUT_QUEUENAME)
    sqs_queue_send_message(queue_name=TEST_SQS_INPUT_QUEUENAME, message_body=json.dumps(requests))

    requests_tablename = "test-executor_requests_table"
    results_tablename = "test-executor_results_table"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        results_table = _dynamodb_create_table(results_tablename, results_fields)
        input_settings = {"sqs_queue_url": queue_url}
        output_settings = {"results_tablename": results_tablename, "requests_tablename": requests_tablename}
        execute_summary = execute_prediction(
            predictor=predictor,
            input_ctx_manager=SQSMessageS3InputImageCtxManager,
            input_settings=input_settings,
            output_ctx_manager=DynamodbOutputCtxManager,
            output_settings=output_settings,
        )
        assert execute_summary
        assert execute_summary["errors"] == 0
    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_INPUT_QUEUENAME)
def test_executor_requests_with_invalid_sns():
    requests = [{"request_id": "r-11111", "s3_uri": f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}", "sns_topic_arn": TEST_SNS_TOPIC_ARN + "invalid"}]

    predictor = DummyPredictorNoInputNoOutputVariableOutput(
        result={"request_id": "r-11111", "result": [{"prediction": 0.11}], "sns_topic_arn": TEST_SNS_TOPIC_ARN, "s3_uri": "s3://bucket/key.png"}
    )

    queue_url = _get_queue_url(TEST_SQS_INPUT_QUEUENAME)
    sqs_queue_send_message(queue_name=TEST_SQS_INPUT_QUEUENAME, message_body=json.dumps(requests))

    requests_tablename = "test-executor_requests_table"
    results_tablename = "test-executor_results_table"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        results_table = _dynamodb_create_table(results_tablename, results_fields)
        input_settings = {"sqs_queue_url": queue_url}
        output_settings = {"results_tablename": results_tablename, "requests_tablename": requests_tablename}
        execute_summary = execute_prediction(
            predictor=predictor,
            input_ctx_manager=SQSMessageS3InputImageCtxManager,
            input_settings=input_settings,
            output_ctx_manager=DynamodbOutputCtxManager,
            output_settings=output_settings,
        )
        assert execute_summary
        assert execute_summary["errors"] == 0
    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_INPUT_QUEUENAME)
def test_executor_requests_with_meta():
    """Test that meta data from the initial request can be included in the prediction result output"""
    request = {
        "request_id": "r-11111",
        "s3_uri": f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}",
        "sns_topic_arn": TEST_SNS_TOPIC_ARN + "invalid",
        "collection_id": "collection:1234",
        "additional-request-key": "somekey",
        "result": None,
    }
    request_keys = list(request.keys())
    requests = [request]

    predictor = DummyPredictorNoInputNoOutputVariableOutput(
        result={"request_id": "r-11111", "result": [{"prediction": 0.11}], "sns_topic_arn": TEST_SNS_TOPIC_ARN, "s3_uri": "s3://bucket/key.png"}
    )

    queue_url = _get_queue_url(TEST_SQS_INPUT_QUEUENAME)
    sqs_queue_send_message(queue_name=TEST_SQS_INPUT_QUEUENAME, message_body=json.dumps(requests))

    requests_tablename = "test-executor_requests_table"
    results_tablename = "test-executor_results_table"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        results_table = _dynamodb_create_table(results_tablename, results_fields)
        input_settings = {"sqs_queue_url": queue_url}
        output_settings = {
            "results_tablename": results_tablename,
            "requests_tablename": requests_tablename,
            "results_additional_parent_keys": request_keys,  # must be added to include additional values in output
        }
        execute_summary = execute_prediction(
            predictor=predictor,
            input_ctx_manager=SQSMessageS3InputImageCtxManager,
            input_settings=input_settings,
            output_ctx_manager=DynamodbOutputCtxManager,
            output_settings=output_settings,
        )
        assert execute_summary
        assert execute_summary["errors"] == 0
        # check results in results_table
        requests_response = request_table.scan()
        assert len(requests_response["Items"]) == 1
        results_response = results_table.scan()
        assert len(results_response["Items"]) == 1
        result_item = results_response["Items"][0]

        # confirm that request request keys are included in the result output
        for expected_request_key in request_keys:
            assert expected_request_key in result_item, result_item

    except Exception as e:
        raise e

    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_context_manager_exit_duration():
    class SleepExitOutputCtxManager(SQSRecordOutputCtxManager):
        def __exit__(self, *args, **kwargs):
            super().__exit__(*args, **kwargs)
            sleep(1)

    predictor = DummyPredictorNoInputNoOutput()

    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SleepExitOutputCtxManager,
        output_settings=output_settings,
    )

    s3uri_inputs = [TEST_IMAGE_S3URI]
    execute_summary = executor.execute(s3uri_inputs)
    assert execute_summary

    assert "context_manager_exit_duration" in execute_summary
    assert execute_summary["context_manager_exit_duration"] >= 1.0


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_INPUT_QUEUENAME)
def test_executor_inputctxmgr_is_valid_handling():
    """Test that meta data from the initial request is referenced for the 'is_valid' key and does not call predict() when meta/info is_valid is False"""
    request = {
        "request_id": "r-11111",
        "s3_uri": f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}",
        "sns_topic_arn": TEST_SNS_TOPIC_ARN + "invalid",
        "collection_id": "collection:1234",
        "additional-request-key": "somekey",
        "result": None,
    }
    request_keys = list(request.keys())
    requests = [request]

    predictor = DummyPredictorNoInputNoOutputVariableOutput(
        result={"request_id": "r-11111", "result": [{"prediction": 0.11}], "sns_topic_arn": TEST_SNS_TOPIC_ARN, "s3_uri": "s3://bucket/key.png"}
    )

    queue_url = _get_queue_url(TEST_SQS_INPUT_QUEUENAME)
    sqs_queue_send_message(queue_name=TEST_SQS_INPUT_QUEUENAME, message_body=json.dumps(requests))

    requests_tablename = "test-executor_requests_table"
    results_tablename = "test-executor_results_table"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        results_table = _dynamodb_create_table(results_tablename, results_fields)
        input_settings = {"sqs_queue_url": queue_url}
        output_settings = {
            "results_tablename": results_tablename,
            "requests_tablename": requests_tablename,
            "results_additional_parent_keys": request_keys,  # must be added to include additional values in output
        }
        execute_summary = execute_prediction(
            predictor=predictor,
            input_ctx_manager=SQSMessageS3InputImageCtxManager,
            input_settings=input_settings,
            output_ctx_manager=DynamodbOutputCtxManager,
            output_settings=output_settings,
        )
        assert execute_summary
        assert execute_summary["errors"] == 0
        # check results in results_table
        requests_response = request_table.scan()
        assert len(requests_response["Items"]) == 1
        results_response = results_table.scan()
        assert len(results_response["Items"]) == 1
        result_item = results_response["Items"][0]

        # confirm that request request keys are included in the result output
        for expected_request_key in request_keys:
            assert expected_request_key in result_item, result_item

    except Exception as e:
        raise e

    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_predictor_with__set_predict_timeout():
    class SleepExitOutputCtxManager(SQSRecordOutputCtxManager):
        def __exit__(self, *args, **kwargs):
            super().__exit__(*args, **kwargs)
            sleep(1)

    predictor = DummyPredictorNoInputNoOutputWithPredictTimeout5s()

    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=S3BucketImageInputCtxManager,
        input_settings={},
        output_ctx_manager=SleepExitOutputCtxManager,
        output_settings=output_settings,
    )

    s3uri_inputs = [TEST_IMAGE_S3URI]
    execute_summary = executor.execute(s3uri_inputs)
    assert execute_summary

    assert "context_manager_exit_duration" in execute_summary
    assert execute_summary["context_manager_exit_duration"] >= 1.0
    assert "errors" in execute_summary
    assert execute_summary["errors"] == 1
