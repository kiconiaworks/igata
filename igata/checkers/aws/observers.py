import logging
from typing import NamedTuple

from rx.core import Observer

logger = logging.getLogger("checkers")


class CheckerMessage(NamedTuple):
    """Checker message"""

    checker_type: str
    body: str


class CheckersObserver(Observer):
    """Checker observer"""

    def on_next(self, value: CheckerMessage) -> None:
        """
        Perform action for each element in the observable sequence.
        """
        logger.info(f'Observer Change Detected checker_type("{value.checker_type}"): {value.body}')

    def on_error(self, error: Exception) -> None:
        """
        Perform action upon exceptional termination of the observable sequence.
        """
        super().on_error(error)
