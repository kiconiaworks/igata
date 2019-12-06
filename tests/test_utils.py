from pathlib import Path
from uuid import UUID

import pandas
import pytest
from igata.utils import flatten, generate_request_id, prepare_csv_dataframe, prepare_csv_reader

from .utils import setup_teardown_s3_file

SAMPLE_CSV_FILEPATH = Path(__file__).parent / "data" / "sample.csv"
SAMPLE_CSVGZ_FILEPATH = Path(__file__).parent / "data" / "sample.csv.gz"


def test_generate_request_id():

    hashable_values = ("somevalue", 999)
    result = generate_request_id(*hashable_values)
    assert result

    # confirm result is valid UUID
    try:
        valid_uuid = UUID(result)
    except ValueError:
        raise pytest.fail(f"result is not a valid UUID: {result}")

    exception_raised = True
    with pytest.raises(ValueError) as verror:
        nonhashable_values = ({"k": [1, 2, 3]}, 1, "other")
        result = generate_request_id(*nonhashable_values)
        exception_raised = False

    assert exception_raised


def test_flatten():

    nested_dict = {"key1": {"other": "other1"}, "key2": "value2"}
    expected = {"key1__other": "other1", "key2": "value2"}
    actual = dict(flatten(nested_dict))
    assert actual == expected, f"actual({actual}) != expected({expected})"


@setup_teardown_s3_file(SAMPLE_CSV_FILEPATH, bucket="igata-testbucket-localstack", key=SAMPLE_CSV_FILEPATH.name)
def test_prepare_csv_reader_csv():
    _, csvreader, download_time, error_message = prepare_csv_reader(bucket="igata-testbucket-localstack", key=SAMPLE_CSV_FILEPATH.name)

    assert error_message is None
    first_line = next(csvreader)
    assert first_line == {"a": "1", "b": "2", "c": "3"}

    second_line = next(csvreader)
    assert second_line == {"a": "4", "b": "5", "c": "6"}


@setup_teardown_s3_file(SAMPLE_CSVGZ_FILEPATH, bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)
def test_prepare_csv_reader_csvgz():
    _, csvreader, download_time, error_message = prepare_csv_reader(bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)

    assert error_message is None
    first_line = next(csvreader)
    assert first_line == {"a": "1", "b": "2", "c": "3"}

    second_line = next(csvreader)
    assert second_line == {"a": "4", "b": "5", "c": "6"}


@setup_teardown_s3_file(SAMPLE_CSV_FILEPATH, bucket="igata-testbucket-localstack", key="badext.zip")
def test_prepare_csv_reader_invalidext():
    _, csvreader, download_time, error_message = prepare_csv_reader(bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)
    assert csvreader is None
    assert error_message


@setup_teardown_s3_file(SAMPLE_CSV_FILEPATH, bucket="igata-testbucket-localstack", key=SAMPLE_CSV_FILEPATH.name)
def test_prepare_csv_dataframe_csv():
    _, df, download_time, error_message = prepare_csv_dataframe(bucket="igata-testbucket-localstack", key=SAMPLE_CSV_FILEPATH.name)
    assert isinstance(df, pandas.DataFrame)
    dfdict = df.to_dict()
    expected = {"a": {0: 1, 1: 4}, "b": {0: 2, 1: 5}, "c": {0: 3, 1: 6}}
    assert dfdict == expected


@setup_teardown_s3_file(SAMPLE_CSVGZ_FILEPATH, bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)
def test_prepare_csv_dataframe_csvgz():
    _, df, download_time, error_message = prepare_csv_dataframe(bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)
    assert isinstance(df, pandas.DataFrame)
    dfdict = df.to_dict()
    expected = {"a": {0: 1, 1: 4}, "b": {0: 2, 1: 5}, "c": {0: 3, 1: 6}}
    assert dfdict == expected


@setup_teardown_s3_file(SAMPLE_CSVGZ_FILEPATH, bucket="igata-testbucket-localstack", key=SAMPLE_CSVGZ_FILEPATH.name)
def test_prepare_csv_dataframe_csv_doesnotexist():
    _, df, download_time, error_message = prepare_csv_dataframe(bucket="igata-testbucket-localstack", key=SAMPLE_CSV_FILEPATH.name)
    assert df is None
    assert error_message
