import logging
import sys
from pathlib import Path
from time import sleep

import boto3
from igata import settings
from igata.handlers.aws.input.sqs import SQSMessageS3InputImageCtxManager
from tests.utils import (
    _create_sqs_queue,
    _delete_sqs_queue,
    setup_teardown_s3_file,
    setup_teardown_sqs_queue,
    sqs_queue_get_attributes,
    sqs_queue_send_message,
)

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


@setup_teardown_s3_file(local_filepath=TEST_IMAGE_FILEPATH, bucket=TEST_BUCKETNAME, key=TEST_IMAGE_FILENAME)
def test_input_handler_sqsmessages3inputimagectxmanager():
    image_found = False

    request = {
        "s3_uri": TEST_IMAGE_S3URI,
        "collection_id": "events:1234:photographers:5678",
        "image_id": None,  # populated below
        "request_id": "request:{request_id}",
    }

    try:
        _delete_sqs_queue(TEST_INPUT_SQS_QUEUENAME)
    except Exception as e:
        logger.exception(e)

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
    with SQSMessageS3InputImageCtxManager(**input_settings) as s3images:
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
    with SQSMessageS3InputImageCtxManager(**input_settings) as s3images:
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
def test_input_handler_sqsmessages3inputimagectxmanager_no_delete_sqs_messages_on_exception():
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

    try:
        with SQSMessageS3InputImageCtxManager(**input_settings) as s3images:
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

    assert actual_count > 0
    # confirm that messages are NOT deleted and still available in queue
    # --> Messages returned to QUEUE
    sleep(settings.SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)  # wait for messages to become visible
    response = sqs_queue_get_attributes(queue_name=TEST_INPUT_SQS_QUEUENAME)
    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == sqs_message_count
