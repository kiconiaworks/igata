import rx
import rx.operators as ops
from requests import Response

from ...utils import requests_retry_session
from .observers import CheckerMessage

INSTANCE_ACTION_URL = "http://169.254.169.254/latest/meta-data/spot/instance-action"
SPOT_INSTANCE_CHECKER_TYPE = "spot_instance"


def access_spot_instance_state(_) -> Response:
    """
    Obtain the spot instance state

    AWS spot meta-data docs:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html

    Response Example:

        {"action": "stop", "time": "2017-09-18T08:22:00Z"}

    """
    return requests_retry_session().get(INSTANCE_ACTION_URL)


def spot_instance_state_response_to_checker_message(resp: Response) -> CheckerMessage:
    """
    Handle the access_spot_instance_state response
    """
    message = resp.json()
    return CheckerMessage(SPOT_INSTANCE_CHECKER_TYPE, f'"spot/instance-action": {message}')


def spot_instance_check_observable(interval_seconds: float = 1.0) -> rx.Observable:
    """
    スポットインスタンスのステータスをinterval秒ごとにurlを叩いてチェックしに行くobservableを返す関数
    :param interval_seconds: interval seconds
    """
    return rx.interval(interval_seconds).pipe(
        ops.map(access_spot_instance_state),
        ops.filter(lambda resp: resp.status_code != 404),
        ops.map(spot_instance_state_response_to_checker_message),
        ops.distinct_until_changed(),
    )
