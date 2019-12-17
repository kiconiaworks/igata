from abc import abstractmethod
from typing import Any, Tuple


class InputCtxManagerBase:
    """To define input sources subclass this class"""

    context_manager_specific_info_keys = []

    def __init__(self, *args, **kwargs):
        required_kwargs = self.required_kwargs()
        missing = []
        for required_kwarg_name in required_kwargs:
            if required_kwarg_name not in kwargs:
                missing.append(required_kwarg_name)
        if missing:
            raise TypeError(f"{self.__class__.__name__} Required Fields Missing: {missing}")

    @abstractmethod
    def get_records(self, *args, **kwargs) -> Tuple[Any, dict]:
        """Define to get records from the desired data source."""
        record = None
        info = {"download_time": None}  # Should contain download_time
        return record, info

    @abstractmethod
    def __enter__(self):
        self

    @abstractmethod
    def __exit__(self, exception_type, exception_value, traceback):
        pass

    @classmethod
    @abstractmethod
    def required_kwargs(cls) -> Tuple:
        """Define the required instantiation kwarg argument names"""
        return tuple()

    def __str__(self):
        return self.__class__.__name__
