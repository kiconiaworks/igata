import importlib
import logging
import os
import sys
from collections import Counter
from pathlib import Path
from typing import List, Type

from . import __version__, settings
from .checkers.aws.ec2 import get_instance_type
from .checkers.aws.spot import SpotInstanceValueObserver
from .defaults import DEFAULT_MODEL_VERSION
from .handlers import (
    INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS,
    INPUT_CONTEXT_MANAGERS,
    INPUT_CTXMGR_ENVAR_PREFIX,
    OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS,
    OUTPUT_CONTEXT_MANAGERS,
    OUTPUT_CTXMGR_ENVAR_PREFIX,
)
from .handlers.aws.input import InputCtxManagerBase
from .handlers.aws.output import OutputCtxManagerBase
from .predictors import PredictorBase
from .runners.executors import PredictionExecutor

MODEL_VERSION = os.getenv("MODEL_VERSION", DEFAULT_MODEL_VERSION)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
if hasattr(logging, LOG_LEVEL):
    log_level = getattr(logging, LOG_LEVEL)
else:
    log_level = logging.INFO

logging.basicConfig(stream=sys.stdout, level=log_level, format="%(asctime)s [%(levelname)s] (%(name)s) %(funcName)s: %(message)s")

# reduce logging output from noisy packages
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("pynamodb.connection.base").setLevel(logging.WARNING)

logger = logging.getLogger("cliexecutor")


def execute_prediction(predictor, input_ctx_manager, input_settings, output_ctx_manager, output_settings, inputs: List[str] = None) -> Counter:
    """Run prediction using the user-defined predictor object and defined input/output context managers."""
    executor = PredictionExecutor(
        predictor=predictor,
        input_ctx_manager=input_ctx_manager,
        input_settings=input_settings,
        output_ctx_manager=output_ctx_manager,
        output_settings=output_settings,
    )
    summary = executor.execute(inputs)  # results are handled by method defined in selected output_ctx_manager
    return summary


def find_predictor_class(module_name: str, predictor_class_name: str) -> Type[PredictorBase]:
    """
    Load the class defined by the predictor_class_name from the given module.

    :param module_name: In format 'package.module'
    :param predictor_class_name: class in the given module that subclasses PredictorBase
    """
    logger.info(f"Importing module({module_name})...")
    module = importlib.import_module(module_name)
    predictor_class = getattr(module, predictor_class_name)
    logger.info(f"Loading ({predictor_class_name}) class from module({module_name})...")
    if not issubclass(predictor_class, PredictorBase):
        raise ValueError(f'Given "from {module_name} import {predictor_class_name}" {predictor_class_name} is not a subclass of PredictorBase!')
    return predictor_class


def collect_ctxmgr_settings(input_ctx_manager: Type[InputCtxManagerBase], output_ctx_manager: Type[OutputCtxManagerBase]) -> dict:
    """Collect context manager settings from environment variables"""
    assert input_ctx_manager
    assert output_ctx_manager
    ctxmgr_settings = {"input_settings": {}, "output_settings": {}}
    logger.debug(f"INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS: {INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS}")
    logger.debug(f"OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS: {OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS}")
    input_ctx_manager_key = input_ctx_manager.__name__
    output_ctx_manager_key = output_ctx_manager.__name__
    logger.info(f"({input_ctx_manager_key}) REQUIRED_ENVARS: {INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[input_ctx_manager_key]}")
    logger.info(f"({output_ctx_manager_key}) REQUIRED_ENVARS: {OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[output_ctx_manager_key]}")
    missing_envars = []
    for expected_envar in INPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[input_ctx_manager_key]:
        envar_value = os.getenv(expected_envar, None)
        logger.debug(f"ENVAR({expected_envar}): {envar_value}")
        if not envar_value:
            missing_envars.append(expected_envar)
        else:
            # remove prefix and lowercase
            # INPUT_CTXMGR_REQUIRED_ARGUMENT -> required_argument
            argument_name = expected_envar.replace(INPUT_CTXMGR_ENVAR_PREFIX, "").lower()
            ctxmgr_settings["input_settings"][argument_name] = envar_value.strip()

    for expected_envar in OUTPUT_CONTEXT_MANAGER_REQUIRED_ENVARS[output_ctx_manager_key]:
        envar_value = os.getenv(expected_envar, None)
        logger.debug(f"ENVAR({expected_envar}): {envar_value}")
        if not envar_value:
            missing_envars.append(expected_envar)
        else:
            # remove prefix and lowercase
            # OUTPUT_CTXMGR_REQUIRED_ARGUMENT -> required_argument
            argument_name = expected_envar.replace(OUTPUT_CTXMGR_ENVAR_PREFIX, "").lower()
            ctxmgr_settings["output_settings"][argument_name] = envar_value.strip()

    if missing_envars:
        for missing_envar in missing_envars:
            logger.error(f"Required EnvironmentVariable not set: {missing_envar}")
        raise ValueError(f"Required EnvironmentVariable(s) not set: {missing_envars}")
    return ctxmgr_settings


def input_contenxt_manager(value) -> Type[InputCtxManagerBase]:
    """argparse type for converting string into the ContextManger class"""
    ctxmgr = INPUT_CONTEXT_MANAGERS.get(value, None)
    if not ctxmgr:
        raise ValueError(f"{value} not in: {INPUT_CONTEXT_MANAGERS.keys()}")
    return ctxmgr


def output_context_manager(value) -> Type[OutputCtxManagerBase]:
    """argparse type for converting string into the ContextManger class"""
    ctxmgr = OUTPUT_CONTEXT_MANAGERS.get(value, None)
    if not ctxmgr:
        raise ValueError(f"{value} not in: {OUTPUT_CONTEXT_MANAGERS.keys()}")
    return ctxmgr


def path_type(value) -> Path:
    """argparse type for converting string into a pathlib.Path object"""
    p = Path(value)
    if not p.exists():
        raise ValueError(f"Given Path not found: {value}")
    return p


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-m",
        "--module-name",
        dest="module_name",
        default=settings.PREDICTOR_MODULE,
        help=f"The dotted path to the module containing the Predictor class [DEFAULT={settings.PREDICTOR_MODULE}]",
    )
    parser.add_argument(
        "-p",
        "--predictor-class-name",
        dest="predictor_class_name",
        default=settings.PREDICTOR_CLASS_NAME,
        help=f"Path to the root package [DEFAULT={settings.PREDICTOR_CLASS_NAME}]",
    )
    parser.add_argument("--model-version", dest="model_version", default=MODEL_VERSION, help="Version of model used for prediction")
    parser.add_argument(
        "-i",
        "--input-ctx-manager",
        dest="input_ctx_manager",
        type=input_contenxt_manager,
        default=INPUT_CONTEXT_MANAGERS[settings.INPUT_CONTEXT_MANAGER_NAME],
        help=f"InputCtxManager ({INPUT_CONTEXT_MANAGERS.keys()}) [DEFAULT={settings.INPUT_CONTEXT_MANAGER_NAME}]",
    )
    parser.add_argument(
        "-o",
        "--output-ctx-manager",
        dest="output_ctx_manager",
        type=output_context_manager,
        default=OUTPUT_CONTEXT_MANAGERS[settings.OUTPUT_CONTEXT_MANAGER_NAME],
        help=f"Result OutputCtxManager ({OUTPUT_CONTEXT_MANAGERS.keys()}) [DEFAULT={settings.OUTPUT_CONTEXT_MANAGER_NAME}]",
    )
    parser.add_argument(
        "inputs",
        type=str,
        nargs="?",
        default=None,
        help='If given, input values separated by command, ",", will be split and given to executor.execute(inputs)',
    )
    parser.add_argument(
        "--spot-instance",
        dest="is_spot_instance",
        action="store_true",
        help='When given identifies environment as using SPOT-INSTANCES, "meta-data/spot/instance-action" data will be logged',
    )
    parser.add_argument("--debug", dest="debug", action="store_true")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.info(f"igata version: {__version__}")
    logger.info(f"Using InputCtxManager: {str(args.input_ctx_manager)}")
    logger.info(f"Using OutputCtxManager: {str(args.output_ctx_manager)}")
    logger.debug(f"> Available INPUT_CONTENXT_MANAGERS: {INPUT_CONTEXT_MANAGERS.keys()}")
    logger.debug(f"> Available OUTPUT_CONTEXT_MANAGERS: {OUTPUT_CONTEXT_MANAGERS.keys()}")

    external_predictor_class = find_predictor_class(args.module_name, args.predictor_class_name)
    external_predictor = external_predictor_class()
    ctxmgr_settings = collect_ctxmgr_settings(args.input_ctx_manager, args.output_ctx_manager)
    for setting in ctxmgr_settings:
        logger.info(f"{setting}: {ctxmgr_settings[setting]}")

    instance_observer = None
    if settings.INSTANCE_ON_AWS:
        logger.info(f"instance_type: {get_instance_type()}")  # Assumes being run on AWS EC2 instance
        if args.is_spot_instance or settings.AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING:
            logger.info("Start spot_instance_observable monitoring...")
            instance_observer = SpotInstanceValueObserver(interval_seconds=1.0)
    elif args.is_spot_instance or settings.AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING:
        logger.warning(
            '"--spot-instance" flag or AWS_ENABLE_SPOTINSTANCE_STATE_LOGGING envar given, '
            "but INSTANCE_ON_AWS == False, logging NOT performed!"
        )

    if instance_observer:
        instance_observer.start()

    input_values = None
    if args.inputs:
        input_values = [v.strip() for v in args.inputs.split(",")]

    summary = execute_prediction(
        predictor=external_predictor,
        input_ctx_manager=args.input_ctx_manager,
        input_settings=ctxmgr_settings["input_settings"],
        output_ctx_manager=args.output_ctx_manager,
        output_settings=ctxmgr_settings["output_settings"],
        inputs=input_values,
    )
    logger.info(f"execution summary: {summary}")
    if instance_observer:
        instance_observer.terminate()
