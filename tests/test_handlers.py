import datetime
import json
import logging
import sys
from pathlib import Path

import boto3
from igata import settings
from igata.handlers import INPUT_CONTEXT_MANAGERS, OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS, OUTPUT_CONTEXT_MANAGERS
from igata.handlers.aws.input.s3 import S3BucketImageInputCtxManager
from igata.handlers.aws.input.sqs import SQSRecordS3InputImageCtxManager
from igata.handlers.aws.output.dynamodb import DynamodbOutputCtxManager, prepare_record
from igata.handlers.aws.output.s3 import S3BucketCsvFileOutputCtxManager
from igata.handlers.aws.output.sqs import SQSRecordOutputCtxManager

from .utils import (
    _create_sqs_queue,
    _delete_sqs_queue,
    _dynamodb_create_table,
    _dynamodb_delete_table,
    _get_dynamodb_table_resource,
    _get_queue_url,
    setup_teardown_s3_bucket,
    setup_teardown_s3_file,
    setup_teardown_sqs_queue,
    sqs_queue_get_attributes,
    sqs_queue_send_message,
)

# add test root to PATH in order to load dummypredictor
BASE_TEST_DIRECTORY = Path(__file__).absolute().parent
sys.path.append(str(BASE_TEST_DIRECTORY))


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(levelname)s] (%(name)s) %(funcName)s: %(message)s")

logger = logging.getLogger(__name__)

# reduce logging output from noisy packages
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("pynamodb.connection.base").setLevel(logging.WARNING)

S3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)

TEST_INPUT_SQS_QUEUENAME = "input-test-queue"
TEST_SQS_OUTPUT_QUEUENAME = "output-test-queue"
TEST_BUCKETNAME = "test-bucket-local"
TEST_OUTPUT_BUCKETNAME = "test-output-bucket-local"
TEST_IMAGE_FILENAME = "pacioli-512x512.png"
TEST_IMAGE_FILEPATH = BASE_TEST_DIRECTORY / "data" / "images" / TEST_IMAGE_FILENAME
assert TEST_IMAGE_FILEPATH.exists()

TEST_IMAGE_S3URI = f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}"


class DummyException(Exception):
    pass


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
def test_input_handler_s3bucketimageinputctxmanager():
    image_found = False

    s3uris = [TEST_IMAGE_S3URI]

    input_settings = {}
    with S3BucketImageInputCtxManager(**input_settings) as s3images:
        for image, info in s3images.get_records(s3uris):
            assert image.any()
            assert info
            image_found = True
    assert image_found


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
def test_input_handler_sqsrecords3inputimagectxmanager():
    image_found = False

    request = {
        "s3_uri": TEST_IMAGE_S3URI,
        "collection_id": "events:1234:photographers:5678",
        "image_id": None,  # populated below
        "request_id": "request:{request_id}",
    }

    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    for i in range(10):
        records = []
        # add 2 requests and send
        for message_request_count in range(2):
            request["image_id"] = f"images:{message_request_count}"
            request["request_id"] = request["request_id"].format(request_id=i)
            records.append(request)
        assert len(records) == 2

        # add dummy records to input queue
        sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=records)

    input_settings = {"sqs_queue_url": queue_url, "max_processing_requests": 2}
    expected_keys = ("s3_uri", "collection_id", "image_id", "request_id")

    expected_count = 2  # defined by 'max_processing_requests'
    with SQSRecordS3InputImageCtxManager(**input_settings) as s3images:
        actual_count = 0
        for image, info in s3images.get_records():
            assert image.any()
            assert info
            assert all(k in info for k in expected_keys)
            image_found = True
            actual_count += 1
    assert image_found
    assert actual_count == expected_count


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
def test_input_handler_sqsrecordss3inputimagectxmanager_single_record():
    """Assure that a single record is properly handled and returned"""
    try:
        _delete_sqs_queue(TEST_INPUT_SQS_QUEUENAME)
    except Exception as e:
        logger.exception(e)

    records = [
        {
            "s3_uri": TEST_IMAGE_S3URI,
            "collection_id": "collection:testmissingresult5",
            "image_id": "images:06c72dc811ecbc8b7114f69d0a18afc9",
            "sns_topic_arn": None,
            "request_id": "c7d4d01c-9351-5dbe-8266-0fd14fab2d50",
            "state": "queued",
            "created_at_timestamp": 1559094119,
            "updated_at_timestamp": 1559094118,
            "result": None,
            "errors": None,
        }
    ]
    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)

    # add dummy records to input queue
    sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=records)
    input_settings = {"sqs_queue_url": queue_url, "max_processing_requests": 2}
    expected_keys = ("s3_uri", "collection_id", "image_id", "request_id")

    expected_count = 1  # defined by 'max_processing_requests'
    with SQSRecordS3InputImageCtxManager(**input_settings) as s3images:
        actual_count = 0
        for image, info in s3images.get_records():
            assert image.any()
            assert info
            assert all(k in info for k in expected_keys)
            image_found = True
            actual_count += 1
    assert image_found
    assert actual_count == expected_count


@setup_teardown_s3_bucket(bucket=TEST_OUTPUT_BUCKETNAME)
def test_output_handler_s3bucketcsvfileoutputctxmanager():
    record_one = [1, "value"]
    record_two = [2, "another"]
    nested_dict_record = {"values": {"a": 1, "b": 2}, "other": "hi"}
    records = (record_one, record_two, nested_dict_record)

    output_settings = {"output_s3_bucket": TEST_OUTPUT_BUCKETNAME, "csv_fieldnames": ("other",), "output_headers": False}
    with S3BucketCsvFileOutputCtxManager(**output_settings) as csvs3bucket:
        csvs3bucket.put_records(records)
        output_key = csvs3bucket.key

    # check that file is in bucket
    response = S3.get_object(Bucket=TEST_OUTPUT_BUCKETNAME, Key=output_key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    lines = response["Body"].read().decode("utf8").strip().split("\n")
    print(lines)
    assert len(lines) == 3


def test_output_handler_s3bucketcsvfileoutputctxmanager_required_envars():
    expected_required = ("output_s3_bucket", "csv_fieldnames")
    assert all(f in S3BucketCsvFileOutputCtxManager.required_kwargs() for f in expected_required)
    mgr = S3BucketCsvFileOutputCtxManager(output_s3_bucket="test_bucket1", csv_fieldnames=["a", "b", "c"])

    expected_envars = [f"OUTPUT_CTXMGR_{e.upper()}" for e in S3BucketCsvFileOutputCtxManager.required_kwargs()]
    for expected_envar in expected_envars:
        assert expected_envar in OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[str(mgr)]


@setup_teardown_sqs_queue(queue_name=TEST_SQS_OUTPUT_QUEUENAME)
def test_output_handler_sqsrecordoutputctxmanager():

    dict_sample = {"other": 1}
    list_sample = [{"other": "value"}, {"more": "values"}]
    queue_url = _get_queue_url(TEST_SQS_OUTPUT_QUEUENAME)
    output_settings = {"sqs_queue_url": queue_url}
    with SQSRecordOutputCtxManager(**output_settings) as sqs_output:
        summary = sqs_output.put_records(dict_sample)
        assert summary["sent_messages"] == 1
        summary = sqs_output.put_records(list_sample)
        assert summary["sent_messages"] == 2


def test_output_handler_sqsrecordoutputctxmanager_required_envars():
    expected_required = ("sqs_queue_url",)
    assert all(f in SQSRecordOutputCtxManager.required_kwargs() for f in expected_required)
    mgr = SQSRecordOutputCtxManager(sqs_queue_url="http://x.com/queue/test_bucket1")

    expected_envars = [f"OUTPUT_CTXMGR_{e.upper()}" for e in SQSRecordOutputCtxManager.required_kwargs()]
    for expected_envar in expected_envars:
        assert expected_envar in OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[str(mgr)]


def test_output_handler_dynamodboutputctxmanager():
    requests_tablename = "txessutsasitsassdsxz-srequests"
    results_tablename = "texsstisissdsdsstxz-sresults"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}

    now = datetime.datetime.now()
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        _ = _dynamodb_create_table(results_tablename, results_fields)
        collection_id = "c1:i3:p3"
        request_item = {
            "s3_uri": "s3://bucket/key",
            "collection_id": collection_id,
            "image_id": "image:33",
            "request_id": "rid222",
            "created_at_timestamp": int(now.timestamp()),
            "state": "queued",
            "result": None,
        }
        request_table.put_item(Item=request_item)
        request_item_no_result = {
            "s3_uri": "s3://bucket/key2",
            "collection_id": collection_id,
            "image_id": "image:332",
            "request_id": "rid223",
            "created_at_timestamp": int(now.timestamp()),
            "state": "queued",
            "result": None,
        }
        request_table.put_item(Item=request_item_no_result)

        result = [
            {
                "numbers": {"digit_fc": "09232", "digit_ltsm": "09282"},
                "position": {"x1": 1, "y1": 2, "x2": 88, "y2": 13},
                "detection_score": 0.77,
                "selection_score": 0.22,
                "is_valid": False,
                "guest_runner_score": 0.77,
            }
        ]
        result_json = json.dumps(result)

        db_item_with_result = {
            "s3_uri": "s3://bucket/key",
            "request_id": "rid222",
            "created_at_timestamp": int(now.timestamp()),
            "result": result,
            "errors": None,
        }
        db_item_no_result = {
            "s3_uri": "s3://bucket/key2",
            "request_id": "rid223",
            "created_at_timestamp": int(now.timestamp()),
            "result": None,
            "errors": None,
        }
        output_settings = {"results_tablename": results_tablename, "requests_tablename": requests_tablename}
        db_items = [db_item_with_result, db_item_no_result]
        with DynamodbOutputCtxManager(**output_settings) as dynamodb:
            summary = dynamodb.put_records(db_items)
            assert summary

        # check that value was updated
        result = request_table.get_item(Key={"request_id": request_item["request_id"]})
        result_item = result["Item"]
        assert result_item
        assert "state" in result_item
        assert result_item["state"] == "processed"
        assert result_item["result"] == result_json
        assert "collection_id" in result_item
        assert result_item["collection_id"] == collection_id

        result = request_table.get_item(Key={"request_id": request_item_no_result["request_id"]})
        result_item_no_result = result["Item"]
        assert result_item_no_result
        assert result_item_no_result["state"] == "processed"
        assert any(result_item_no_result["result"] == null_value for null_value in (None, "[]", []))
        assert result_item_no_result["collection_id"] == collection_id

    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


def test_output_handler_dynamodboutputctxmanager_put_record():
    requests_tablename = "txessutsasitsassdsxz-srequests"
    results_tablename = "texsstisissdsdsstxz-sresults"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}

    now = datetime.datetime.now()
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        _ = _dynamodb_create_table(results_tablename, results_fields)
        collection_id = "c1:i3:p3"
        result = [
            {
                "numbers": {"digit_fc": "09232", "digit_ltsm": "09282"},
                "position": {"x1": 1, "y1": 2, "x2": 88, "y2": 13},
                "detection_score": 0.77,
                "selection_score": 0.22,
                "is_valid": False,
                "guest_runner_score": 0.77,
            }
        ]
        result_json = json.dumps(result)

        initial_request_id = None
        db_result_items = []
        RESULT_RECORD_CHUNK_SIZE = 15
        for i in range(RESULT_RECORD_CHUNK_SIZE + 5):
            s3_uri = f"s3://bucket/key{i}"
            request_id = f"rid{i}"
            image_id = f"image:{i}"
            if not initial_request_id:
                initial_request_id = request_id
            request_item = {
                "s3_uri": s3_uri,
                "collection_id": collection_id,
                "image_id": image_id,
                "request_id": request_id,
                "created_at_timestamp": int(now.timestamp()),
                "state": "queued",
                "result": None,
            }
            request_table.put_item(Item=request_item)
            item = {"s3_uri": s3_uri, "request_id": request_id, "result": result, "errors": None}
            db_result_items.append(item)

        output_settings = {"results_tablename": results_tablename, "requests_tablename": requests_tablename}
        with DynamodbOutputCtxManager(**output_settings) as dynamodb:
            for count, item in enumerate(db_result_items, 1):
                _ = dynamodb.put_record(item)
                if count >= RESULT_RECORD_CHUNK_SIZE:
                    count = count - RESULT_RECORD_CHUNK_SIZE
                assert len(dynamodb._record_results) == count

        # check that value was updated
        result = request_table.get_item(Key={"request_id": initial_request_id})
        result_item = result["Item"]
        assert result_item
        assert "state" in result_item
        assert result_item["state"] == "processed"
        assert result_item["result"] == result_json
        assert "collection_id" in result_item
        assert result_item["collection_id"] == collection_id

    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)


def test_output_handler_dynamodboutputctxmanager_prepare_record():
    result = [{"a": 1, "b": "other"}]
    result_json = json.dumps(result)
    record = {"first": 123, "second": "2nd", "result": result}
    prepared_record, original_nested = prepare_record(record)

    assert "result" in original_nested

    # check that nested record was converted to json
    assert prepared_record["result"] == result_json

    # check that all keys exist in prepared_record
    assert all(original_key in prepared_record for original_key in record.keys())

    # check that non-nested keys are NOT included in original_nested
    assert "first" not in original_nested
    assert "second" not in original_nested


def test_registered_context_managers():
    supported_input_context_managers = ("S3BucketImageInputCtxManager", "SQSRecordS3InputImageCtxManager")
    assert all(configured in supported_input_context_managers for configured in INPUT_CONTEXT_MANAGERS)

    supported_output_context_managers = ("S3BucketCsvFileOutputCtxManager", "SQSRecordOutputCtxManager", "DynamodbOutputCtxManager")
    assert all(configured in supported_output_context_managers for configured in OUTPUT_CONTEXT_MANAGERS)


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
def test_input_handler_sqsrecords3inputimagectxmanager_no_delete_sqs_messages_on_exception():
    image_found = False

    request = {
        "s3_uri": TEST_IMAGE_S3URI,
        "collection_id": "events:1234:photographers:5678",
        "image_id": None,  # populated below
        "request_id": "request:{request_id}",
    }

    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME, purge=True)
    sqs_message_count = 10
    records_per_message = 2
    for i in range(sqs_message_count):
        records = []
        # add 2 requests and send
        for message_request_count in range(records_per_message):
            request["image_id"] = f"images:{message_request_count}"
            request["request_id"] = request["request_id"].format(request_id=i)
            records.append(request)
        assert len(records) == records_per_message

        # add dummy records to input queue
        sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=records)

    # confirm that messages are in queue
    response = sqs_queue_get_attributes(queue_name=TEST_INPUT_SQS_QUEUENAME)
    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == sqs_message_count

    desired_processing_requests = sqs_message_count * records_per_message
    input_settings = {"sqs_queue_url": queue_url, "max_processing_requests": desired_processing_requests}
    expected_keys = ("s3_uri", "collection_id", "image_id", "request_id")

    expected_count = desired_processing_requests  # defined by 'max_processing_requests'
    try:
        with SQSRecordS3InputImageCtxManager(**input_settings) as s3images:
            actual_count = 0
            for image, info in s3images.get_records():
                assert image.any()
                assert info
                assert all(k in info for k in expected_keys)
                actual_count += 1
                if actual_count >= desired_processing_requests - 2:
                    raise DummyException
    except DummyException:
        pass

    # confirm that messages are NOT deleted and still available in queue
    # --> Messages returned to QUEUE
    response = sqs_queue_get_attributes(queue_name=TEST_INPUT_SQS_QUEUENAME)
    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == sqs_message_count


def test_output_handler_dynamodboutputctxmanager_duplicate_record_overwrite():
    requests_tablename = "txessutsasitsassdsxz-srequests"
    results_tablename = "texsstisissdsdsstxz-sresults"
    requests_fields = {"request_id": ("S", "HASH")}

    results_fields = {"hashkey": ("S", "HASH"), "s3_uri": ("S", "RANGE")}

    now = datetime.datetime.now()
    try:
        request_table = _dynamodb_create_table(requests_tablename, requests_fields)
        _ = _dynamodb_create_table(results_tablename, results_fields)
        collection_id = "c1:i3:p3"
        request_item = {
            "s3_uri": "s3://bucket/key",
            "collection_id": collection_id,
            "image_id": "image:33",
            "request_id": "rid222",
            "created_at_timestamp": int(now.timestamp()),
            "state": "queued",
            "result": None,
        }
        request_table.put_item(Item=request_item)
        request_item_no_result = {
            "s3_uri": "s3://bucket/key2",
            "collection_id": collection_id,
            "image_id": "image:332",
            "request_id": "rid223",
            "created_at_timestamp": int(now.timestamp()),
            "state": "queued",
            "result": None,
        }
        request_table.put_item(Item=request_item_no_result)

        result = [
            {
                "numbers": {"digit_fc": "09232", "digit_ltsm": "09282"},
                "position": {"x1": 1, "y1": 2, "x2": 88, "y2": 13},
                "detection_score": 0.77,
                "selection_score": 0.22,
                "is_valid": False,
                "guest_runner_score": 0.77,
            }
        ]

        db_item_with_result = {
            "s3_uri": "s3://bucket/key",
            "request_id": "rid222",
            "created_at_timestamp": int(now.timestamp()),
            "result": result,
            "errors": None,
        }
        db_item_no_result = {
            "s3_uri": "s3://bucket/key2",
            "request_id": "rid223",
            "created_at_timestamp": int(now.timestamp()),
            "result": None,
            "errors": None,
        }

        results_table = _get_dynamodb_table_resource(results_tablename)
        initial_results_record_count = results_table.item_count
        assert initial_results_record_count == 0

        output_settings = {"results_tablename": results_tablename, "requests_tablename": requests_tablename}
        db_items = [db_item_with_result, db_item_no_result]
        expected_results_record_count = 1  # db_item_no_result not saved as result
        with DynamodbOutputCtxManager(**output_settings) as dynamodb:
            summary = dynamodb.put_records(db_items)
            assert summary

        # check the results table values
        results_table = _get_dynamodb_table_resource(results_tablename)
        results_record_count = results_table.item_count

        request_record_count = request_table.item_count

        # duplicate request put_records
        with DynamodbOutputCtxManager(**output_settings) as dynamodb:
            summary = dynamodb.put_records(db_items)
            assert summary

        results_table = _get_dynamodb_table_resource(results_tablename)
        post_duplicate_results_record_count = results_table.item_count
        assert post_duplicate_results_record_count == results_record_count
        assert post_duplicate_results_record_count == expected_results_record_count

        request_table = _get_dynamodb_table_resource(requests_tablename)
        post_duplicate_request_record_count = request_table.item_count
        assert post_duplicate_request_record_count == request_record_count

    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)
