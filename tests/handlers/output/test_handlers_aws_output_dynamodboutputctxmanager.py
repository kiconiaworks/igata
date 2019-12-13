import datetime
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

import boto3
from igata import settings
from igata.handlers.aws.output.dynamodb import DynamodbOutputCtxManager, prepare_record
from tests.utils import _dynamodb_create_table, _dynamodb_delete_table, _get_dynamodb_table_resource

# add test root to PATH in order to load dummypredictor
BASE_TEST_DIRECTORY = Path(__file__).absolute().parent.parent.parent
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
        # flattened result
        expected_result = {
            "position__y1": Decimal("2"),
            "numbers__digit_ltsm": "09282",
            "position__x1": Decimal("1"),
            "position__y2": Decimal("13"),
            "guest_runner_score": Decimal("0.77"),
            "position__x2": Decimal("88"),
            "s3_uri": "s3://bucket/key",
            "numbers__digit_fc": "09232",
            "is_valid": False,
            "selection_score": Decimal("0.22"),
            "detection_score": Decimal("0.77"),
            "request_id": "rid222",
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

        response = results_table.scan()
        result_items = response["Items"]
        assert len(result_items) == expected_results_record_count
        initial_result_item = result_items[0]

        for key, value in expected_result.items():
            if key in ("created_at_timestamp", "hashkey"):
                continue
            assert initial_result_item[key] == value

        request_record_count = request_table.item_count

        # duplicate request put_records
        with DynamodbOutputCtxManager(**output_settings) as dynamodb:
            summary = dynamodb.put_records(db_items)
            assert summary

        results_table = _get_dynamodb_table_resource(results_tablename)
        post_duplicate_results_record_count = results_table.item_count
        assert post_duplicate_results_record_count == results_record_count
        assert post_duplicate_results_record_count == expected_results_record_count

        response = results_table.scan()
        result_items = response["Items"]
        assert len(result_items) == expected_results_record_count
        duplicate_result_item = result_items[0]
        assert duplicate_result_item == initial_result_item

        for key, value in expected_result.items():
            if key in ("created_at_timestamp", "hashkey"):
                continue
            assert duplicate_result_item[key] == value

        request_table = _get_dynamodb_table_resource(requests_tablename)
        post_duplicate_request_record_count = request_table.item_count
        assert post_duplicate_request_record_count == request_record_count

    except Exception as e:
        raise e
    finally:
        _dynamodb_delete_table(requests_tablename)
        _dynamodb_delete_table(results_tablename)
