from abc import abstractmethod
from typing import Any, List, Optional, Type


class PostPredictHookMixInBase:
    """Defines required method for integrating a PostPredictHook"""

    @abstractmethod
    def post_predict_hook(self, record: Any, response: Any, meta: Optional[dict] = None) -> Any:
        """Hook for providing igata.handlers.aws.mixins for additional post processing. (Intended for signaling, db updates, etc.)"""
        pass
