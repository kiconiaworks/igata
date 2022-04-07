from collections import defaultdict

from .aws.input.s3 import S3BucketImageInputCtxManager
from .aws.input.sqs import SQSMessageS3InputCSVPandasDataFrameCtxManager, SQSMessageS3InputImageCtxManager
from .aws.output.aframax import AframaxRecordOutputCtxManager
from .aws.output.dynamodb import DynamodbOutputCtxManager
from .aws.output.s3 import S3BucketPandasDataFrameCsvFileOutputCtxManager
from .aws.output.sqs import SQSRecordOutputCtxManager

INPUT_CONTEXT_MANAGERS = {
    "S3BucketImageInputCtxManager": S3BucketImageInputCtxManager,
    "SQSMessageS3InputImageCtxManager": SQSMessageS3InputImageCtxManager,
    "SQSMessageS3InputCSVPandasDataFrameCtxManager": SQSMessageS3InputCSVPandasDataFrameCtxManager,
}


OUTPUT_CONTEXT_MANAGERS = {
    "S3BucketPandasDataFrameCsvFileOutputCtxManager": S3BucketPandasDataFrameCsvFileOutputCtxManager,
    "SQSRecordOutputCtxManager": SQSRecordOutputCtxManager,
    "DynamodbOutputCtxManager": DynamodbOutputCtxManager,
    "AframaxRecordOutputCtxManager": AframaxRecordOutputCtxManager,
}

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
