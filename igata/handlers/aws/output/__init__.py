import logging
import os
from abc import abstractmethod
from typing import Tuple, Union

RESULT_RECORD_CHUNK_SIZE = int(os.getenv("RESULT_RECORD_CHUNK_SIZE", "15"))


logger = logging.getLogger(__name__)


class OutputCtxManagerBase:
    """For defining output targets subclass this class"""

    results_additional_parent_keys: Union[list, tuple, None] = None  # defined if request info should be included in result output

    def __init__(self, *args, **kwargs):
        required_kwargs = self.required_kwargs()
        missing = []
        for required in required_kwargs:
            if required not in kwargs:
                missing.append(required)
        if missing:
            raise TypeError(f"{self.__class__.__name__} Required Fields Missing: {missing}")
        self._record_results = []

    @abstractmethod
    def put_records(self, *args, **kwargs):
        """Define method to put data to the desired output target"""
        pass

    def put_record(self, record, *args, **kwargs) -> int:
        """
        Cache record result until RESULT_RECORD_CHUNK_SIZE is met or exceeded,
        then call the sub-class defined put_records() method to process records.
        """
        put_records_count = 0
        self._record_results.append(record)
        if len(self._record_results) >= RESULT_RECORD_CHUNK_SIZE:
            logger.debug(f"len(self._record_results) >= RESULT_RECORD_CHUNK_SIZE({RESULT_RECORD_CHUNK_SIZE}): calling self.put_records")

            self.put_records(self._record_results, *args, **kwargs)
            put_records_count = len(self._record_results)
            self._record_results = []
        return put_records_count

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self):
        pass

    @classmethod
    @abstractmethod
    def required_kwargs(cls) -> Tuple:
        """Define the required instantiation kwarg argument names"""
        return tuple()

    def __str__(self):
        return self.__class__.__name__
