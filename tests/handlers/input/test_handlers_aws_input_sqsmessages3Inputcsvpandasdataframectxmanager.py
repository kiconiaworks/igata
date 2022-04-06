import logging
import sys
from pathlib import Path

import boto3
import pandas
from igata import settings
from igata.handlers.aws.input.sqs import SQSMessageS3InputCSVPandasDataFrameCtxManager
from tests.utils import _create_sqs_queue, _delete_sqs_queue, _upload_to_s3, setup_teardown_s3_file, sqs_queue_send_message

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

SAMPLE_CSV_FILEPATH = Path(__file__).parent.parent.parent / "data" / "sample.csv"
SAMPLE_CSVGZ_FILEPATH = Path(__file__).parent.parent.parent / "data" / "sample.csv.gz"


TEST_IMAGE_S3URI = f"s3://{TEST_BUCKETNAME}/{TEST_IMAGE_FILENAME}"


class DummyException(Exception):
    pass


@setup_teardown_s3_file(local_filepath=SAMPLE_CSV_FILEPATH, bucket=TEST_BUCKETNAME, key=SAMPLE_CSV_FILEPATH.name)
def test_input_handler_sqsmessages3inputcsvpandasdataframectxmanager():
    test_s3uri_1 = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSV_FILEPATH.name}"
    request = {"s3_uri_key1": test_s3uri_1, "collection_id": "events:1234:photographers:5678", "request_id": "request:{request_id}"}

    _delete_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    for i in range(10):
        # update request_id
        request["request_id"] = request["request_id"].format(request_id=i)

        # add dummy records to input queue
        sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=request)

    input_settings = {"sqs_queue_url": queue_url, "max_processing_requests": 2, "s3uri_keys": ["s3_uri_key1"]}

    expected_count = 2  # defined by 'max_processing_requests'
    with SQSMessageS3InputCSVPandasDataFrameCtxManager(**input_settings) as dataframeinput:
        actual_count = 0
        for dfrecords, info in dataframeinput.get_records():
            assert isinstance(dfrecords, dict)
            assert "s3uri_keys" in info
            assert "sqs_queue_url" in info
            assert "max_processing_requests" in info
            assert "is_valid" in info
            assert info["is_valid"] is True
            for df in dfrecords.values():
                assert isinstance(df, pandas.DataFrame)

            actual_count += 1
    assert actual_count == expected_count


@setup_teardown_s3_file(local_filepath=SAMPLE_CSV_FILEPATH, bucket=TEST_BUCKETNAME, key=SAMPLE_CSV_FILEPATH.name)
def test_input_handler_sqsmessages3inputcsvpandasdataframectxmanager_multiple_s3urikeys():
    _upload_to_s3(SAMPLE_CSVGZ_FILEPATH, TEST_BUCKETNAME, SAMPLE_CSVGZ_FILEPATH.name)
    test_s3uri_1 = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSV_FILEPATH.name}"
    test_s3uri_2 = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSVGZ_FILEPATH.name}"
    request = {
        "s3_uri_key1": test_s3uri_1,
        "s3_uri_key2": test_s3uri_2,
        "collection_id": "events:1234:photographers:5678",
        "request_id": "request:{request_id}",
    }

    _delete_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    for i in range(10):

        # update request_id
        request["request_id"] = request["request_id"].format(request_id=i)

        # add dummy records to input queue
        sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=request)

    input_settings = {"sqs_queue_url": queue_url, "max_processing_requests": 2, "s3uri_keys": ["s3_uri_key1", "s3_uri_key2"]}

    expected_count = 2  # defined by 'max_processing_requests'
    with SQSMessageS3InputCSVPandasDataFrameCtxManager(**input_settings) as dataframeinput:
        actual_count = 0
        for dfrecords, info in dataframeinput.get_records():
            assert isinstance(dfrecords, dict)
            assert "s3uri_keys" in info
            assert "sqs_queue_url" in info
            assert "max_processing_requests" in info
            assert "is_valid" in info
            assert info["is_valid"] is True
            for df in dfrecords.values():
                assert isinstance(df, pandas.DataFrame)

            actual_count += 1
    assert actual_count == expected_count


@setup_teardown_s3_file(local_filepath=SAMPLE_CSV_FILEPATH, bucket=TEST_BUCKETNAME, key=SAMPLE_CSV_FILEPATH.name)
def test_input_handler_sqsmessages3inputcsvpandasdataframectxmanager_multiple_multilayered_s3urikeys():
    _upload_to_s3(SAMPLE_CSVGZ_FILEPATH, TEST_BUCKETNAME, SAMPLE_CSVGZ_FILEPATH.name)
    test_s3uri_1 = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSV_FILEPATH.name}"
    test_s3uri_2 = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSVGZ_FILEPATH.name}"
    request = {
        "s3_uri_keys_container": {
            "key1": test_s3uri_1,
            "key2": test_s3uri_2,
        },
        "collection_id": "events:1234:photographers:5678",
        "request_id": "request:{request_id}",
    }

    _delete_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    queue_url = _create_sqs_queue(queue_name=TEST_INPUT_SQS_QUEUENAME)
    for i in range(10):

        # update request_id
        request["request_id"] = request["request_id"].format(request_id=i)

        # add dummy records to input queue
        sqs_queue_send_message(queue_name=TEST_INPUT_SQS_QUEUENAME, message_body=request)

    input_settings = {
        "sqs_queue_url": queue_url,
        "max_processing_requests": 2,
        "s3uri_keys": ["s3_uri_keys_container.key1", "s3_uri_keys_container.key2"],
    }

    expected_count = 2  # defined by 'max_processing_requests'
    with SQSMessageS3InputCSVPandasDataFrameCtxManager(**input_settings) as dataframeinput:
        actual_count = 0
        for dfrecords, info in dataframeinput.get_records():
            assert isinstance(dfrecords, dict)
            assert "s3uri_keys" in info
            assert "sqs_queue_url" in info
            assert "max_processing_requests" in info
            assert "is_valid" in info
            assert info["is_valid"] is True
            for df in dfrecords.values():
                assert isinstance(df, pandas.DataFrame)

            actual_count += 1
    assert actual_count == expected_count
