"""
Project/package-wide settings
"""
import datetime
import logging
import os
from distutils.util import strtobool

logger = logging.getLogger(__name__)

JST = datetime.timezone(datetime.timedelta(hours=+9), "JST")

MAX_POST_IMAGES = 50


VALID_INPUT_CONTEXT_MANAGER_NAMES = (
    "S3BucketImageInputCtxManager",
    "SQSMessageS3InputImageCtxManager",
    "SQSMessageS3InputCSVPandasDataFrameCtxManager",
)

DEFAULT_INPUT_CONTEXT_MANAGER_NAME = "SQSMessageS3InputCSVPandasDataFrameCtxManager"
INPUT_CONTEXT_MANAGER_NAME = os.getenv("INPUT_CONTEXT_MANAGER", DEFAULT_INPUT_CONTEXT_MANAGER_NAME)
if INPUT_CONTEXT_MANAGER_NAME not in VALID_INPUT_CONTEXT_MANAGER_NAMES:
    logger.warning(f"Invalid INPUT_CONTEXT_MANAGER({INPUT_CONTEXT_MANAGER_NAME}), using default: {DEFAULT_INPUT_CONTEXT_MANAGER_NAME}")
    INPUT_CONTEXT_MANAGER_NAME = DEFAULT_INPUT_CONTEXT_MANAGER_NAME

VALID_OUTPUT_CONTEXT_MANAGER_NAMES = ("S3BucketPandasDataFrameCsvFileOutputCtxManager", "SQSRecordOutputCtxManager", "DynamodbOutputCtxManager")

DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME = "S3BucketPandasDataFrameCsvFileOutputCtxManager"
OUTPUT_CONTEXT_MANAGER_NAME = os.getenv("OUTPUT_CONTEXT_MANAGER", DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME)
if OUTPUT_CONTEXT_MANAGER_NAME not in VALID_OUTPUT_CONTEXT_MANAGER_NAMES:
    logger.warning(f"Invalid OUTPUT_CONTEXT_MANAGER_NAME({OUTPUT_CONTEXT_MANAGER_NAME}), using default: {DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME}")
    OUTPUT_CONTEXT_MANAGER_NAME = DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME


PREDICTOR_MODULE = os.getenv("PREDICTOR_MODULE", None)
DEFAULT_PREDICTOR_CLASS_NAME = "Predictor"
PREDICTOR_CLASS_NAME = os.getenv("PREDICTOR_CLASS_NAME", DEFAULT_PREDICTOR_CLASS_NAME)

DEFAULT_INSTANCE_ON_AWS = "True"
INSTANCE_ON_AWS = strtobool(os.getenv("INSTANCE_ON_AWS", DEFAULT_INSTANCE_ON_AWS))

DEFAULT_AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING = "True"
AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING = strtobool(os.getenv("AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING", DEFAULT_AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING))


# aws boto3 configurations
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")

DEFAULT_S3_ENDPOINT = f"https://s3.{AWS_REGION}.amazonaws.com"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", DEFAULT_S3_ENDPOINT)

DEFAULT_SQS_ENDPOINT = f"https://sqs.{AWS_REGION}.amazonaws.com"
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT", DEFAULT_SQS_ENDPOINT)

DEFAULT_SNS_ENDPOINT = f"https://sns.{AWS_REGION}.amazonaws.com"
SNS_ENDPOINT = os.getenv("SNS_ENDPOINT", DEFAULT_SNS_ENDPOINT)

DEFAULT_DYNAMODB_ENDPOINT = f"https://dynamodb.{AWS_REGION}.amazonaws.com"
DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT", DEFAULT_DYNAMODB_ENDPOINT)

DEFAULT_DYNAMODB_DECIMAL_PRECISION_DIGITS = "6"
DYNAMODB_DECIMAL_PRECISION_DIGITS = int(os.getenv("DYNAMODB_DECIMAL_PRECISION_DIGITS", DEFAULT_DYNAMODB_DECIMAL_PRECISION_DIGITS))

DEFAULT_SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION = "5"
SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION = int(
    os.getenv("SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION", DEFAULT_SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION)
)

DOWNLOAD_WORKERS = int(os.getenv("DOWNLOAD_WORKERS", 8))

# comma separated
DEFAULT_S3URI_KEYS = "s3_uri"
REQUEST_S3URI_KEYS = os.getenv("REQUEST_S3URI_KEYS", DEFAULT_S3URI_KEYS)
if REQUEST_S3URI_KEYS:
    REQUEST_S3URI_KEYS = REQUEST_S3URI_KEYS.split(",")

DEFAULT_MAX_PROCESSING_REQUESTS = 50
MAX_PROCESSING_REQUESTS = int(os.getenv("MAX_PROCESSING_REQUESTS", str(DEFAULT_MAX_PROCESSING_REQUESTS)))
logger.info(f"(ENVAR) MAX_PROCESSING_REQUESTS: {MAX_PROCESSING_REQUESTS}")

DEFAULT_MAX_PER_REQUEST_PROCESSING_SECONDS = 60
MAX_PER_REQUEST_PROCESSING_SECONDS = int(os.getenv("MAX_PER_REQUEST_PROCESSING_SECONDS", str(DEFAULT_MAX_PER_REQUEST_PROCESSING_SECONDS)))
logger.info(f"MAX_PER_REQUEST_PROCESSING_SECONDS: {MAX_PER_REQUEST_PROCESSING_SECONDS}")

# dynamodb settings
DYNAMODB_REQUESTS_TABLENAME = os.getenv("DYNAMODB_REQUESTS_TABLENAME", "test_requests_table")

DYNAMODB_RESULTS_ADDITIONAL_PARENT_FIELDS = os.getenv(
    "RESULTS_ADDITIONAL_PARENT_FIELDS", "request_id,s3_uri"
)  # comma separated field to include from parent
DYNAMODB_RESULTS_SORTKEY_KEYNAME = os.getenv("RESULTS_SORTKEY_KEYNAME", "s3_uri")

DYNAMODB_DEFAULT_RESULTS_KEYNAME = "result"
DYNAMODB_REQUESTS_TABLE_RESULTS_KEYNAME = os.getenv("REQUESTS_TABLE_RESULTS_KEYNAME", DYNAMODB_DEFAULT_RESULTS_KEYNAME)
DYNAMODB_REQUESTS_TABLE_HASHKEY_KEYNAME = os.getenv("REQUESTS_TABLE_HASHKEY_KEYNAME", "request_id")

# fields dependent on api implementation
DYNAMODB_RESULTS_TABLE_STATE_FIELDNAME = "state"
DYNAMODB_RESULTS_ERROR_STATE = "error"
DYNAMODB_RESULTS_PROCESSED_STATE = "completed"

# INPUT CSV Settings
DEFAULT_INPUT_CSV_ENCODING = "utf8"
INPUT_CSV_ENCODING = os.getenv("INPUT_CSV_ENCODING", DEFAULT_INPUT_CSV_ENCODING)

DEFAULT_INPUT_CSV_DELIMITER = ","
INPUT_CSV_DELIMITER = os.getenv("INPUT_CSV_DELIMITER", DEFAULT_INPUT_CSV_DELIMITER)

DEFAULT_INPUT_CSV_HEADER_LINES = None  # a header may span more than 1 line
INPUT_CSV_HEADER_LINES = os.getenv("INPUT_CSV_HEADER_LINES", DEFAULT_INPUT_CSV_HEADER_LINES)
if INPUT_CSV_HEADER_LINES:
    INPUT_CSV_HEADER_LINES = int(INPUT_CSV_HEADER_LINES)

DEFAULT_OUTPUT_CSV_ENCODING = os.getenv("DEFAULT_OUTPUT_CSV_ENCODING", "utf8")
DEFAULT_OUTPUT_CSV_DELIMITER = os.getenv("DEFAULT_OUTPUT_CSV_DELIMITER", ",")

# - csv reader settings
DEFAULT_INPUT_CSV_READER_DIALECT = "excel"
INPUT_CSV_READER_DIALECT = os.getenv("INPUT_CSV_READER_DIALECT", DEFAULT_INPUT_CSV_READER_DIALECT)

# Tuple of available handlers.aws.output.mixins
VALID_OUTPUT_CTXMGR_MIXINS = ("DynamodbRequestUpdateMixIn",)
DEFAULT_OUTPUT_CTXMGR_MIXINS = None
OUTPUT_CTXMGR_MIXINS = os.getenv("OUTPUT_CTXMGR_MIXINS", DEFAULT_OUTPUT_CTXMGR_MIXINS)
if OUTPUT_CTXMGR_MIXINS:
    OUTPUT_CTXMGR_MIXINS = OUTPUT_CTXMGR_MIXINS.split(",")
    for output_mixin in OUTPUT_CTXMGR_MIXINS:
        if output_mixin not in VALID_OUTPUT_CTXMGR_MIXINS:
            raise ValueError(f"Invalid value for OUTPUT_CTXMGR_MIXINS, must be comma separated list of: {VALID_OUTPUT_CTXMGR_MIXINS}")
