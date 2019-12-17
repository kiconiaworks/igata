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


class DummyPredictorOptionalValidStaticMethods(PredictorBase):
    @staticmethod
    def get_pandas_read_csv_kwargs(self):
        return {"x": 1}

    def predict(self, inputs, meta):
        return {"result": 0.222, "class": "car", "is_valid": True}

    @staticmethod
    def get_pandas_to_csv_kwargs(self):
        return {"y": 2}

    @staticmethod
    def set_additional_dynamodb_request_update_attributes(self):
        return {"v": True}


class DummyPredictorOptionalInValidStaticMethods(PredictorBase):
    def get_pandas_read_csv_kwargs(self):
        return {"x": 1}

    def predict(self, inputs, meta):
        return {"result": 0.222, "class": "car", "is_valid": True}

    def get_pandas_to_csv_kwargs(self):
        return {"y": 2}

    def set_additional_dynamodb_request_update_attributes(self):
        return {"v": True}


class DummyInPandasDataFrameOutPandasCSVPredictor(PredictorBase):
    def predict(self, inputs, meta):
        raise NotImplementedError
