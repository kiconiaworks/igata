import csv
import logging
import sys
from pathlib import Path

import boto3
from igata import settings
from igata.handlers.aws.input.s3 import S3BucketCSVInputCtxManager
from tests.utils import (
    _create_sqs_queue,
    _delete_sqs_queue,
    _upload_to_s3,
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


@setup_teardown_s3_file(local_filepath=SAMPLE_CSV_FILEPATH, bucket=TEST_BUCKETNAME, key=SAMPLE_CSV_FILEPATH.name)
def test_input_handler_s3bucketcsvinputctxmanager():
    sample_csv_s3uri = f"s3://{TEST_BUCKETNAME}/{SAMPLE_CSV_FILEPATH.name}"
    s3uris = [sample_csv_s3uri]

    expected_lines = []
    with SAMPLE_CSV_FILEPATH.open("r", encoding="utf8") as expected_csv:
        headers = False
        for line in csv.reader(expected_csv):
            if not headers:
                headers = True
                continue
            expected_lines.append(line)

    lines = []
    input_settings = {}
    with S3BucketCSVInputCtxManager(**input_settings) as s3images:
        for csvreader, info in s3images.get_records(s3uris):
            assert csvreader
            assert info
            lines = [line for line in csvreader]
    assert lines
    assert len(lines) == len(expected_lines), expected_lines
