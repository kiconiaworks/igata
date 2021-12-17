from time import sleep
from unittest import mock

from igata.checkers.aws.ec2 import INSTANCE_TYPE_URL, get_instance_type
from igata.checkers.aws.spot import SpotInstanceValueObserver, INSTANCE_ACTION_URL


def test_spotinstancevalueobserver(requests_mock):
    with mock.patch("igata.checkers.aws.spot.logger.info") as mock_method:
        thread = SpotInstanceValueObserver(interval_seconds=0.5)
        thread.start()
        requests_mock.get(INSTANCE_ACTION_URL, json={"action": "terminate", "time": "2017-09-18T08:22:00Z"})
        sleep(1)
        thread.terminate()
        thread.join()
        mock_method.assert_called_once()


def test_get_instance_type(requests_mock):
    requests_mock.get(INSTANCE_TYPE_URL, text="c5.large")
    assert get_instance_type() == "c5.large"
