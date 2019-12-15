from collections import defaultdict

from .aws.input.s3 import S3BucketCSVInputCtxManager, S3BucketImageInputCtxManager
from .aws.input.sqs import (
    SQSMessagePassthroughCtxManager,
    SQSMessageS3InputCSVPandasDataFrameCtxManager,
    SQSMessageS3InputCSVReaderCtxManager,
    SQSMessageS3InputImageCtxManager,
)
from .aws.output.dynamodb import DynamodbOutputCtxManager
from .aws.output.mixins.dyanamodb import DynamodbRequestUpdateMixIn
from .aws.output.s3 import S3BucketPandasDataFrameCsvFileOutputCtxManager
from .aws.output.sqs import SQSRecordOutputCtxManager

INPUT_CONTEXT_MANAGERS = {
    "S3BucketImageInputCtxManager": S3BucketImageInputCtxManager,
    "S3BucketCSVInputCtxManager": S3BucketCSVInputCtxManager,
    "SQSMessageS3InputImageCtxManager": SQSMessageS3InputImageCtxManager,
    "SQSMessageS3InputCSVCtxManager": SQSMessageS3InputCSVReaderCtxManager,
    "SQSMessagePassthroughCtxManager": SQSMessagePassthroughCtxManager,
    "SQSMessageS3InputCSVPandasDataFrameCtxManager": SQSMessageS3InputCSVPandasDataFrameCtxManager,
}
DEFAULT_INPUT_CONTEXT_MANAGER_NAME = "S3BucketImageInputCtxManager"

OUTPUT_CONTEXT_MANAGERS = {
    "S3BucketPandasDataFrameCsvFileOutputCtxManager": S3BucketPandasDataFrameCsvFileOutputCtxManager,
    "SQSRecordOutputCtxManager": SQSRecordOutputCtxManager,
    "DynamodbOutputCtxManager": DynamodbOutputCtxManager,
}
DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME = "SQSRecordOutputCtxManager"

# collect required parameters expected to be given as Environment variables
INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS = defaultdict(list)
INPUT_CTXMGR_ENVAR_PREFIX = "INPUT_CTXMGR_"
input_ctxmgr_envar_prefix = INPUT_CTXMGR_ENVAR_PREFIX
for name, ctxmgr_class in INPUT_CONTEXT_MANAGERS.items():
    for required_arguement_name in ctxmgr_class.required_kwargs():
        required_envar_name = f"{input_ctxmgr_envar_prefix}{required_arguement_name.upper()}"
        INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[name].append(required_envar_name)

OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS = defaultdict(list)
OUTPUT_CTXMGR_ENVAR_PREFIX = "OUTPUT_CTXMGR_"
output_ctxmgr_envar_prefix = OUTPUT_CTXMGR_ENVAR_PREFIX
for name, ctxmgr_class in OUTPUT_CONTEXT_MANAGERS.items():
    for required_arguement_name in ctxmgr_class.required_kwargs():
        required_envar_name = f"{output_ctxmgr_envar_prefix}{required_arguement_name.upper()}"
        OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[name].append(required_envar_name)

OUTPUT_CONTEXT_MANAGER_MIXINS = {"DynamodbRequestUpdateMixIn": DynamodbRequestUpdateMixIn}
