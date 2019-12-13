import logging
import sys
from pathlib import Path

import boto3
from igata import settings
from igata.handlers import OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS
from igata.handlers.aws.output.sqs import SQSRecordOutputCtxManager
from tests.utils import _get_queue_url, setup_teardown_sqs_queue

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
