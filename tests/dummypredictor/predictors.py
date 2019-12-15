from time import sleep

from igata.predictors import PredictorBase


class DummyPredictorNoInputNoOutput(PredictorBase):
    def predict(self, inputs, meta):
        result = {"result": 0.222, "class": "car", "is_valid": True}
        return result


class DummyPredictorNoInputNoOutputVariableOutput(PredictorBase):
    def __init__(self, *args, **kwargs):
        default_result = {"result": 0.222, "class": "car", "is_valid": True}
        self.result = kwargs.get("result", default_result)

    def predict(self, input, meta=None):
        return self.result


class DummyPredictorNoOutput(PredictorBase):
    def preprocess_input(self, record, meta=None):
        return {}

    def predict(self, record, meta):
        return record


class DummyPredictorNoInputNoOutputWithPredictTimeout5s(PredictorBase):
    def predict(self, inputs, meta):
        self.set_predict_timeout(3)
        sleep(10)
        result = {"result": 0.222, "class": "car", "is_valid": True}
        return result
