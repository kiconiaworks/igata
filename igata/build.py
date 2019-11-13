"""
Build the docker image for the given package using the defined input/output context managers
"""
from pathlib import Path

from .handlers import DEFAULT_INPUT_CONTEXT_MANAGER_NAME, DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME, INPUT_CONTEXT_MANAGERS, OUTPUT_CONTEXT_MANAGERS


def build_image(input_ctx_manager, output_ctx_manager, package_filepath):
    """Build docker image for target package including a Predictor"""
    # https://docker-py.readthedocs.io/en/stable/images.html
    raise NotImplementedError()


if __name__ == "__main__":
    import argparse

    def pathtype(value):
        """argparse type function for handling pathlib.Path"""
        p = Path(value)
        if not p.exists():
            raise ValueError(f"Given path not found: {value}")
        return p

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-ctx-manager",
        "-i",
        dest="input_ctx_manager",
        type=str,
        default=DEFAULT_INPUT_CONTEXT_MANAGER_NAME,
        choices=[k for k in INPUT_CONTEXT_MANAGERS.keys()],
        help=f"Input Context Manager to use with Predictor [DEFAULT={DEFAULT_INPUT_CONTEXT_MANAGER_NAME}]",
    )
    parser.add_argument(
        "--output-ctx-manager",
        "-o",
        dest="output_ctx_manager",
        type=str,
        default=DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME,
        choices=[k for k in OUTPUT_CONTEXT_MANAGERS.keys()],
        help=f"Output Context Manager to use with Predictor [DEFAULT={DEFAULT_OUTPUT_CONTEXT_MANAGER_NAME}]",
    )
    parser.add_argument("--package-filepath", "-p", dest="package_filepath", type=pathtype, required=True, help="Path to the root package directory")

    # pipenv run python -m igata.build --input-ctx-manager YYYY --output-ctx-manager XXXX --package RELPATH_TO_PACKAGE
    args = parser.parse_args()
    build_image(args.input_ctx_manager, args.output_ctx_maanager, args.package_filepath)
