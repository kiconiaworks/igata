class PredictTimeoutError(Exception):
    """Raised when a Predictor calls .set_predictor_timeout and the Predictor.predict() method call timeout expires."""

    pass
