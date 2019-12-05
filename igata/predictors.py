from abc import abstractmethod
from typing import Any, Optional, Union


class PredictorBase:
    """Class to subclass to define a predictor to be wrapped and run by the igata.runners.executors.PredictionExecutor"""

    __version__ = "0.1.0"

    def pre_predict_hook(self, record: Any, info: Optional[dict] = None) -> None:
        """Hook for providing igata.handlers.aws.mixins for additional pre processing. (Intended for signaling, db updates, etc.)"""
        pass

    def preprocess_input(self, input_record, meta: Union[dict, None] = None):
        """
        (Optional)
        If defined, this method will be called on the input_record provided,
        with the results of this method used as input to the `predict` method.
        """
        return input_record

    @abstractmethod
    def predict(self, input_record, meta: Union[dict, None] = None) -> Union[list, dict]:
        """
        (Required) method must be defined by user
        Takes the input_record and provides a result prediction based on the given input_record.
        The result is typically expected to be JSON parsable

        If `preprocess_outputs` is not defined, these results will be passed to the defined OutputCtxManager
        """
        prediction_result = {}
        return prediction_result

    def postprocess_output(self, prediction_result, meta: Union[dict, None] = None) -> Union[list, dict]:
        """
        (Optional)
        If defined, this method will be called on the result of the `predict` method.
        With transformed results passed to the defined OutputCtxManager
        """
        return prediction_result

    def post_predict_hook(self, record: Any, response: Any, meta: Optional[dict] = None) -> None:
        """Hook for providing igata.handlers.aws.mixins for additional post processing. (Intended for signaling, db updates, etc.)"""
        pass
