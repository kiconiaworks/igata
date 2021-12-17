import logging

from time import sleep
from threading import Thread
from typing import NamedTuple
from requests import Response

from ...utils import requests_retry_session


logger = logging.getLogger("checkers")

INSTANCE_ACTION_URL = "http://169.254.169.254/latest/meta-data/spot/instance-action"
SPOT_INSTANCE_CHECKER_TYPE = "spot_instance"


def access_spot_instance_state() -> Response:
    """
    Obtain the spot instance state

    AWS spot meta-data docs:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html

    Response Example:

        {"action": "stop", "time": "2017-09-18T08:22:00Z"}

    """
    retry_session = requests_retry_session()
    response = retry_session.get(INSTANCE_ACTION_URL)
    return response


class CheckerMessage(NamedTuple):
    """Checker message"""

    checker_type: str
    body: str


class SpotInstanceValueObserver(Thread):
    """Thread used to monitor and log when a change in state is detected"""

    def __init__(self, interval_seconds: float = 1.0):
        Thread.__init__(self)
        self.internal_seconds = interval_seconds
        self.last_instance_state = None
        self.started = False
        self._terminate = False

    def terminate(self):
        """Terminate the thread if started"""
        self._terminate = True
        if self.started:
            self.join()

    def run(self):
        """Start thread"""
        self.started = True
        while True:
            response = access_spot_instance_state()
            if response.status_code != 404:
                result = response.json()
                message = CheckerMessage(SPOT_INSTANCE_CHECKER_TYPE, f'"spot/instance-action": {result}')
                if message.checker_type == SPOT_INSTANCE_CHECKER_TYPE and self.last_instance_state != message.body:
                    logger.info(f'Observer Change Detected checker_type("{message.checker_type}"): {message.body}')
                self.last_instance_state = message.body
            if self._terminate:
                break
            sleep(self.internal_seconds)
