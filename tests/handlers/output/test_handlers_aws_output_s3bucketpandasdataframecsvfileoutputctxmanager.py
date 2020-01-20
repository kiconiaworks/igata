import gzip
import logging
import sys
from pathlib import Path
from uuid import uuid4

import boto3
import pandas
from igata import settings
from igata.handlers import OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS
from igata.handlers.aws.output.s3 import S3BucketPandasDataFrameCsvFileOutputCtxManager
from tests.utils import setup_teardown_dyanmodb_table, setup_teardown_s3_bucket

# add test root to PATH in order to load dummypredictor
BASE_TEST_DIRECTORY = Path(__file__).absolute().parent.parent.parent
sys.path.append(str(BASE_TEST_DIRECTORY))
sys.path.append(str(BASE_TEST_DIRECTORY.parent))


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(levelname)s] (%(name)s) %(funcName)s: %(message)s")

logger = logging.getLogger(__name__)

# reduce logging output from noisy packages
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
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


def create_sample_dataframe() -> pandas.DataFrame:
    raw_data = {
        "first_name": ["Jason", "Molly", "Tina", "Jake", "Amy"],
        "last_name": ["Miller", "Jacobson", "Ali", "Milner", "Cooze"],
        "age": [42, 52, 36, 24, 73],
        "preTestScore": [4, 24, 31, 2, 3],
        "postTestScore": [25, 94, 57, 62, 70],
    }
    df = pandas.DataFrame(raw_data, columns=["first_name", "last_name", "age", "preTestScore", "postTestScore"])
    return df


@setup_teardown_s3_bucket(bucket=TEST_OUTPUT_BUCKETNAME)
def test_output_handler_s3bucketpandasdataframecsvfileoutputctxmanager__no_tocsvkwargs():
    job_id = str(uuid4())
    sample_df = create_sample_dataframe()
    record = {"job_id": job_id, "filename": "outputfilename.csv", "dataframe": sample_df, "is_valid": True}

    output_settings = {"output_s3_bucket": TEST_OUTPUT_BUCKETNAME, "results_keyname": "result", "output_s3_prefix": "prefix/"}
    all_outputs = []
    with S3BucketPandasDataFrameCsvFileOutputCtxManager(**output_settings) as pandascsvoutputmgr:
        outputs = pandascsvoutputmgr.put_record(record)
        all_outputs.append(outputs)

    for output_info in all_outputs:
        # check that file(s) in bucket
        response = S3.get_object(**output_info)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        lines = response["Body"].read().decode("utf8").strip().split("\n")
        assert len(lines) == 5, lines


def test_output_handler_s3bucketpandasdataframecsvfileoutputctxmanager_required_envars():
    expected_required = ("output_s3_bucket", "output_s3_prefix", "results_keyname")
    assert all(f in S3BucketPandasDataFrameCsvFileOutputCtxManager.required_kwargs() for f in expected_required)
    mgr = S3BucketPandasDataFrameCsvFileOutputCtxManager(output_s3_bucket="test_bucket1", results_keyname="result", output_s3_prefix="test")

    expected_envars = [f"OUTPUT_CTXMGR_{e.upper()}" for e in S3BucketPandasDataFrameCsvFileOutputCtxManager.required_kwargs()]
    for expected_envar in expected_envars:
        assert expected_envar in OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[str(mgr)]


@setup_teardown_s3_bucket(bucket=TEST_OUTPUT_BUCKETNAME)
def test_output_handler_s3bucketpandasdataframecsvfileoutputctxmanager__with_gzip_compression():
    job_id = str(uuid4())
    sample_df = create_sample_dataframe()
    record = {"job_id": job_id, "filename": "outputfilename.csv.gz", "dataframe": sample_df, "is_valid": True}

    output_settings = {"output_s3_bucket": TEST_OUTPUT_BUCKETNAME, "results_keyname": "result", "output_s3_prefix": "prefix/"}
    all_outputs = []
    with S3BucketPandasDataFrameCsvFileOutputCtxManager(**output_settings) as pandascsvoutputmgr:
        outputs = pandascsvoutputmgr.put_record(record)
        all_outputs.append(outputs)

    for output_info in all_outputs:
        # check that file(s) in bucket
        response = S3.get_object(**output_info)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        with gzip.GzipFile(fileobj=response["Body"], mode="r") as gz:
            lines = gz.read().decode("utf8").strip().split("\n")
            assert len(lines) == 5, lines


@setup_teardown_s3_bucket(bucket=TEST_OUTPUT_BUCKETNAME)
@setup_teardown_dyanmodb_table(tablename="test_requests_table", fields={"job_id": ("S", "HASH")})
def test_output_handler_s3bucketpandasdataframecsvfileoutputctxmanager__with_force_gzip_compression(*args, **kwargs):
    job_id = str(uuid4())
    sample_df = create_sample_dataframe()
    record = {"job_id": job_id, "filename": "outputfilename.csv", "dataframe": sample_df, "is_valid": True, "result_s3_uris": []}

    # create entry in dynamodb to be updated
    table = kwargs.get("dynamodb_table")
    table.put_item(
        Item={
            "job_id": job_id,
            "predictor_status": "pending",
            "updated_timestamp": 22,
            "completed_timestamp": 11,
            "result_s3_uris": "[]",
            "errors": "[]",
        }
    )

    output_settings = {
        "output_s3_bucket": TEST_OUTPUT_BUCKETNAME,
        "results_keyname": "result",
        "output_s3_prefix": "prefix/",
        "force_gzip_compression": True,
    }
    all_outputs = []
    with S3BucketPandasDataFrameCsvFileOutputCtxManager(**output_settings) as pandascsvoutputmgr:
        outputs = pandascsvoutputmgr.put_record(record)
        all_outputs.append(outputs)

    for output_info in all_outputs:
        assert output_info["Key"].endswith(".gz")
        # check that file(s) in bucket
        response = S3.get_object(**output_info)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        data = gzip.decompress(response["Body"].read())
        lines = data.decode("utf8").strip().split("\n")
        assert len(lines) == 5, lines
