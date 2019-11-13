from unittest import mock

import rx
import rx.operators as ops
from igata.checkers.aws.ec2 import INSTANCE_TYPE_URL, get_instance_type
from igata.checkers.aws.observers import CheckerMessage, CheckersObserver
from igata.checkers.aws.spot import INSTANCE_ACTION_URL, spot_instance_check_observable
from rx.concurrency import CurrentThreadScheduler


def test_spot_instance_checker_through_404(requests_mock):
    requests_mock.get(INSTANCE_ACTION_URL, text="test", status_code=404)

    o = spot_instance_check_observable(0.1).pipe(ops.merge(rx.timer(0.5)), ops.first())

    def on_next(x):
        assert x == 0

    o.subscribe(on_next=on_next, scheduler=CurrentThreadScheduler())


def test_spot_instance_checker_terminate(requests_mock):
    body = {"action": "terminate", "time": "2017-09-18T08:22:00Z"}
    requests_mock.get(INSTANCE_ACTION_URL, json=body)

    o = spot_instance_check_observable(0.1).pipe(ops.merge(rx.timer(0.5)), ops.first())

    def on_next(x):
        assert isinstance(x, CheckerMessage)
        assert x.checker_type == "spot_instance"
        assert x.body == f'"spot/instance-action": {body}'

    o.subscribe(on_next=on_next, scheduler=CurrentThreadScheduler())


def test_spot_instance_checker_observer(requests_mock):
    with mock.patch("igata.checkers.aws.observers.logger.info") as mock_method:
        requests_mock.get(INSTANCE_ACTION_URL, json={"action": "terminate", "time": "2017-09-18T08:22:00Z"})

        o = spot_instance_check_observable(0.1).pipe(ops.merge(rx.timer(0.3)), ops.take(2), ops.filter(lambda x: isinstance(x, CheckerMessage)))

        o.subscribe(CheckersObserver(), scheduler=CurrentThreadScheduler())
        mock_method.assert_called_once()


def test_get_instance_type(requests_mock):
    requests_mock.get(INSTANCE_TYPE_URL, text="c5.large")

    assert get_instance_type() == "c5.large"
