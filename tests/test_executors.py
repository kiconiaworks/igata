import json
import logging
import sys
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple
from unittest import TestCase

import pandas
from dummypredictor.mixins import DummyMixin
from dummypredictor.predictors import (
    DummyInPandasDataFrameOutPandasCSVPredictor,
    DummyPredictorNoInputNoOutput,
    DummyPredictorNoInputNoOutputVariableOutput,
    DummyPredictorNoInputNoOutputWithPredictTimeout5s,
    DummyPredictorOptionalInValidStaticMethods,
    DummyPredictorOptionalValidStaticMethods,
)
from igata.cli import execute_prediction
from igata.handlers.aws.input import InputCtxManagerBase
from igata.handlers.aws.input.s3 import S3BucketImageInputCtxManager
from igata.handlers.aws.input.sqs import SQSMessageS3InputImageCtxManager
from igata.handlers.aws.output import OutputCtxManagerBase
from igata.handlers.aws.output.dynamodb import DynamodbOutputCtxManager
from igata.handlers.aws.output.mixins.dyanamodb import DynamodbRequestUpdateMixIn
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


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_executor_predictor_with_outputctxmgrmixin():
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
        output_ctxmgr_mixins=[DummyMixin],
    )

    assert DummyMixin in executor.output_ctx_manager.__bases__
    with executor.get_output_ctx_manager_instance() as output_ctxmgr:
        assert hasattr(output_ctxmgr, "mixin_method")
        assert output_ctxmgr.mixin_method()


class DummyInputCtxManagerWithOptionalStaticMethods(InputCtxManagerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_pandas_read_csv_kwargs = kwargs.get("get_pandas_read_csv_kwargs", None)

    def get_records(self, *args, **kwargs):

        yield self.get_pandas_read_csv_kwargs(True), {}

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass


class DummyOutputCtxManagerWithOptionalStaticMethods(OutputCtxManagerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "get_pandas_to_csv_kwargs_func" in kwargs:
            self.get_pandas_to_csv_kwargs_func = kwargs.get("get_pandas_to_csv_kwargs")
        if "get_additional_dynamodb_request_update_attributes" in kwargs:
            self.get_additional_dynamodb_request_update_attributes = kwargs.get("get_additional_dynamodb_request_update_attributes")

    def put_records(self, record, **kwargs):
        """Define method to put data to the desired output target"""
        pass

    def put_record(self, record, *args, **kwargs) -> int:
        """
        Cache record result until RESULT_RECORD_CHUNK_SIZE is met or exceeded,
        then call the sub-class defined put_records() method to process records.
        """
        new_record = []
        for result in record:
            self._record_results.append(result)
        return True

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def required_kwargs(cls) -> Tuple:
        """Define the required instantiation kwarg argument names"""
        return tuple()


def test_executor__optional_predictor_inputctxmgr_staticmethods__valid():
    predictor = DummyPredictorOptionalValidStaticMethods()
    output_settings = {}
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=DummyInputCtxManagerWithOptionalStaticMethods,
        input_settings={},
        output_ctx_manager=DummyOutputCtxManagerWithOptionalStaticMethods,
        output_settings=output_settings,
    )
    execute_summary = executor.execute()
    assert execute_summary  # simple test to confirm basic functionality


def make_student_columns() -> Tuple[List[str], Dict[str, str]]:
    student_columns = ["student_id"]
    student_column_dtypes = {"student_id": "str"}

    qs = [(1, 18), (2, 29), (3, 24), (4, 16), (5, 12)]
    for i, m in qs:
        for j in range(1, m + 1):
            q_col = f"q{i}-{j}_cd"
            student_columns.append(q_col)
            student_column_dtypes[q_col] = "float"
    return student_columns, student_column_dtypes


def make_company_columns() -> Tuple[List[str], Dict[str, str]]:
    company_columns = ["company_id", "employee_id"]
    company_column_dtypes = {"company_id": "str", "employee_id": "str"}
    qs = [(1, 18), (2, 29), (3, 24), (4, 16), (5, 14)]
    for i, m in qs:
        for j in range(1, m + 1):
            q_col = f"q{i}-{j}_cd"
            company_columns.append(q_col)
            company_column_dtypes[q_col] = "float"
    return company_columns, company_column_dtypes


#
#
# class SurveyRecordPredictorTestCase(TestCase):
#     def setUp(self) -> None:
#         # self.maxDiff = None
#         self.students_csv_filepath = BASE_TEST_DIRECTORY / "student_data.csv.gz"
#         self.companies_csv_filepath = BASE_TEST_DIRECTORY / "company_data.csv.gz"
#
#         self.students_colnames, self.students_dtypes = make_student_columns()
#         self.students_input_df = csv_dataframe_load(self.students_csv_filepath, names=self.students_colnames, dtypes=self.students_dtypes)
#
#         self.companies_colnames, self.companies_dtypes = make_company_columns()
#         self.companies_input_df = csv_dataframe_load(self.companies_csv_filepath, names=self.companies_colnames, dtypes=self.companies_dtypes)
#
#         self.input_record = {
#             "job_id": "f02e2c3a-8de1-49af-9d0d-6220c0021999",
#             "job_index": 1,
#             "students_1_1_20190809182030.csv.gz__dataframe": self.students_input_df,
#             "companies_1_1_20190809182030.csv.gz__dataframe": self.companies_input_df,
#             "timeout_seconds": 2100,
#         }
#         self.input_meta = {"s3uri_keys": ["students_1_1_20190809182030.csv.gz", "companies_1_1_20190809182030.csv.gz"], "is_valid": True}
#
#     def test_surveyrecordpredictor__valid_input(self):
#         predictor = DummyInPandasDataFrameOutPandasCSVPredictor()
#         results = predictor.predict(self.input_record, self.input_meta)
#         self.assertEqual(len(results), 1)
#
#         result = results[0]
#         expected_result_keys = ("filename", "gzip", "dataframe", "to_csv_kwargs", "job_id")
#         for expected_key in expected_result_keys:
#             self.assertIn(expected_key, result)
#
#         result_df = result["dataframe"]
#         self.assertTrue(isinstance(result_df, pandas.DataFrame))
#
#     def test_surveyrecordpredictor__students_csv_read(self):
#         predictor = DummyInPandasDataFrameOutPandasCSVPredictor()
#         csv_read_kwargs = predictor.get_read_csv_kwargs("students_1_1_20190809182030.csv.gz")
#
#         expected_kwargs = {"header": None, "sep": ",", "encoding": "utf8", "names": self.students_colnames, "dtype": self.students_dtypes}
#         self.assertDictEqual(csv_read_kwargs, expected_kwargs)
#
#         expected_df = self.students_input_df
#         expected_dict = expected_df.to_dict()
#
#         actual_df = pandas.read_csv(self.students_csv_filepath, **csv_read_kwargs)
#         actual_dict = actual_df.to_dict()
#
#         self.assertEqual(set(actual_dict.keys()), set(expected_dict.keys()))
#
#         # This function is intended to compare two DataFrames and output any differences.
#         # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.testing.assert_frame_equal.html
#         result = pandas.testing.assert_frame_equal(actual_df, expected_df)
#         self.assertFalse(result, result)
