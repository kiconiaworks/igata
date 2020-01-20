from igata.handlers import INPUT_CONTEXT_MANAGERS


class DummyException(Exception):
    pass


def test_registered_input_context_managers():
    supported_input_context_managers = (
        "S3BucketImageInputCtxManager",
        "SQSMessageS3InputImageCtxManager",
        "SQSMessageS3InputCSVPandasDataFrameCtxManager",
    )
    assert all(configured in supported_input_context_managers for configured in INPUT_CONTEXT_MANAGERS)
