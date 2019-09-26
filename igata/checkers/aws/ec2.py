from ...utils import requests_retry_session

INSTANCE_TYPE_URL = "http://169.254.169.254/latest/meta-data/instance-type"


def get_instance_type() -> str:
    """
    Obtain instance type

    AWS meta-data docs:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html#instancedata-data-retrieval

    """
    return requests_retry_session().get(INSTANCE_TYPE_URL).text
