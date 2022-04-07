import csv
import datetime
import json
import logging
import os
import time
import urllib
from collections.abc import Hashable
from decimal import Decimal
from gzip import GzipFile
from hashlib import md5
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Generator, List, Optional, Tuple, Union
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from uuid import NAMESPACE_URL, uuid5

import boto3
import imageio
import numpy as np
import pandas
import requests
from botocore.errorfactory import ClientError
from igata import settings
from requests.adapters import HTTPAdapter
from retry.api import retry_call
from urllib3 import Retry

logger = logging.getLogger("cliexecutor")


# for generating UUID for request_id
UUID_NAMESPACE_DNS_NAME = os.getenv("UUID_NAMESPACE_DNS_NAME", "my-api.com")

S3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)


def default_json_encoder(obj):
    """
    Serialize for objects that cannot be serialized by the default json encoder

    Usage:

        json_bytes = json.dumps(myobj, default=default_json_encoder)

    """
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object cannot be serialized: {obj}")


def flatten(nested_object, keystring="", allow_null_strings=True, separator="__") -> Generator[tuple, None, None]:
    """
    Flatten a nested dictionary into a flat/single-level key, value tuple.

    Usage:
        nested_object = {
            'key1': {'other': 'other1'},
            'key2': 'value2'
        }
        for key_value in flatten(nested_object):
            print(key_value)  # ('key1__other': 'other1') ...

    .. note::

        Results can be converted to dictionary using:

            flattened_dict = dict(flatten(nested_object))

    """
    if isinstance(nested_object, dict):
        keystring = f"{keystring}{separator}" if keystring else keystring
        for key in nested_object:
            updated_keystring = f"{keystring}{key}"
            yield from flatten(nested_object[key], updated_keystring, allow_null_strings, separator)
    elif isinstance(nested_object, list):
        for list_element in nested_object:
            yield from flatten(list_element, keystring, allow_null_strings, separator)
    else:
        if not allow_null_strings:
            if nested_object != "":
                yield keystring, nested_object
        else:
            yield keystring, nested_object


def prepare_images(bucket, key) -> Tuple[Tuple[str, str], np.array, float, Optional[str]]:
    """
    Read the given s3 key into a numpy array.from retry.api import retry_call
    """
    error_message = None
    key = unquote(key)
    url = S3.generate_presigned_url(ClientMethod="get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600, HttpMethod="GET")

    start = time.time()
    try:
        image = retry_call(imageio.imread, fargs=[url], tries=10)[:, :, :3]
    except HTTPError as e:
        logger.exception(e)
        error_message = f"Exception while processing image(s3://{bucket}/{key}): ({e.code}) {e.reason}"
        logger.error(error_message)
        image = np.array([])
    except ValueError as e:
        logger.exception(e)
        error_message = f"Exception while processing image(s3://{bucket}/{key}): {e.args}"
        logger.error(error_message)
        image = np.array([])
    end = time.time()
    download_time = end - start

    return (bucket, key), image, download_time, error_message


def _download_s3_file(bucket: str, key: str) -> dict:
    """Download file from S3"""
    url = S3.generate_presigned_url(ClientMethod="get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600, HttpMethod="GET")
    logger.info(f"downloading ({url})...")
    response = requests_retry_session().get(url)
    return response


def prepare_csv_reader(
    bucket: str,
    key: str,
    encoding: str = settings.INPUT_CSV_ENCODING,
    delimiter: str = settings.INPUT_CSV_DELIMITER,
    reader: Union[csv.reader, csv.DictReader] = csv.DictReader,
    dialect: str = settings.INPUT_CSV_READER_DIALECT,
) -> Tuple[Tuple[str, str], Union[csv.reader, csv.DictReader, None], float, Optional[str]]:
    """
    Read the given s3 key into a numpy array.from retry.api import retry_call
    reader = csv.DictReader(StringIO(text))
    """
    error_message = None
    csvreader = None
    key = unquote(key)
    if key.lower().endswith((".csv", ".gz")):
        start = time.time()
        try:
            response = _download_s3_file(bucket, key)
        except HTTPError as e:
            logger.exception(e)
            error_message = f"Exception while processing csv(s3://{bucket}/{key}): ({e.code}) {e.reason}"
            logger.error(error_message)

        except ValueError as e:
            logger.exception(e)
            error_message = f"Exception while processing csv(s3://{bucket}/{key}): {e.args}"
            logger.error(error_message)

        if 200 <= response.status_code <= 299:
            if key.lower().endswith(".gz"):
                data = GzipFile(fileobj=BytesIO(response.content)).read().decode(encoding)
                csvreader = reader(StringIO(data), dialect=dialect, delimiter=delimiter)
            elif key.lower().endswith(".csv"):
                data = response.text
                csvreader = reader(StringIO(data), dialect=dialect, delimiter=delimiter)

        else:
            error_message = f"({response.status_code}) error downloading data"
    else:
        error_message = f"unsupported CSV file extension: s3://{bucket}/{key}"

    end = time.time()
    download_time = end - start

    return (bucket, key), csvreader, download_time, error_message


def prepare_csv_dataframe(
    bucket: str, key: str, read_csv_kwargs: Optional[dict] = None
) -> Tuple[Tuple[str, str], Optional[pandas.DataFrame], float, Optional[str]]:
    """Read CSV from s3 and return a dataframe"""
    df = None
    error_message = None
    response = None
    start = time.time()
    try:
        response = _download_s3_file(bucket, key)
    except HTTPError as e:
        logger.exception(e)
        error_message = f"Exception while processing csv(s3://{bucket}/{key}): ({e.code}) {e.reason}"
        logger.error(error_message)

    if response:
        if 200 <= response.status_code <= 299:
            filename = Path(key.split("/")[-1])
            data = BytesIO(response.content)
            data.name = filename.name

            if not read_csv_kwargs:
                # set defaults
                read_csv_kwargs = {
                    "sep": settings.DEFAULT_INPUT_CSV_DELIMITER,
                    "encoding": settings.DEFAULT_INPUT_CSV_ENCODING,
                    "header": settings.DEFAULT_INPUT_CSV_HEADER_LINES,
                }

            # - determine compression
            ext = filename.suffix.lower()
            compression_ext_mapping = {".zip": "zip", ".gz": "gzip", ".xz": "xz", ".bz2": "bz2"}
            compression = compression_ext_mapping.get(ext, None)
            if compression and "compression" not in read_csv_kwargs:
                read_csv_kwargs["compression"] = compression

            logger.debug(f"read_csv_kwargs={read_csv_kwargs}")
            try:
                df = pandas.read_csv(data, **read_csv_kwargs)
            except Exception as e:
                logger.exception(e)
                error_message = f"Exception Occurred while calling pandas.read_csv(): {e.args}"
        else:
            error_message = f"Invalid response.status_code while processing csv(s3://{bucket}/{key}): status_code={response.status_code}"
            logger.error(error_message)
    else:
        error_message = f"response not defined, download failed for: s3://{bucket}/{key}"
        logger.error("response not defined!")

    end = time.time()
    download_time = end - start

    return (bucket, key), df, download_time, error_message


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Parse s3 uri (s3://bucket/key) to (bucket, key)
    """
    result = urlparse(uri)
    bucket = result.netloc
    key = result.path[1:]  # removes leading slash
    return bucket, key


def generate_request_id(*values, uuid_namespace_dns_name=UUID_NAMESPACE_DNS_NAME) -> str:
    """
    Generate the UUID string for given values

    .. note::

        values are sorted to ensure key reproducibility

    """
    if not all(isinstance(v, Hashable) for v in values):
        raise ValueError(f"Given value not hashable, values: {values}")
    unique_key = md5(".".join(value for value in sorted(str(v) for v in values)).encode("utf8")).hexdigest()
    hash_url = urllib.parse.quote_plus(f"http://{uuid_namespace_dns_name}/{unique_key}")
    value = str(uuid5(namespace=NAMESPACE_URL, name=hash_url))
    return value


def serialize_json_and_chunk_by_bytes(items: List[Union[dict, str]], max_bytes: int = 2048) -> Generator[str, None, None]:
    """
    Serialize items into JSON and yield by the resulting
    """
    is_initial = True
    last_json_str = None
    chunked_items = []
    logger.debug(f"chunk_processing items incoming: {len(items)}")
    for item in items:
        if chunked_items:
            json_str = json.dumps(chunked_items)
            json_bytes = json_str.encode("utf8")

            if is_initial and len(json_bytes) > max_bytes:
                raise ValueError(f"Single item > max_bytes({max_bytes}: {json_bytes}")

            elif len(json_bytes) > max_bytes:
                yield last_json_str
                chunked_items = chunked_items[-1:]  # remove items yielded in last_json_str

            last_json_str = json_str
        chunked_items.append(item)
        is_initial = False

    if chunked_items:
        json_str = json.dumps(chunked_items)
        encoded = json_str.encode("utf8")
        if len(encoded) >= max_bytes:
            json_str = json.dumps(chunked_items[:-1])
            yield json_str  # make sure to send last one!
            json_str = json.dumps(chunked_items[-1:])
            yield json_str  # make sure to send last one!
        else:
            yield json_str  # make sure to send last one!


def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    """
    request retry sessions
    :param retries:
    :param backoff_factor:
    :param status_forcelist:
    :param session:
    :return:
    """
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def s3_key_exists(bucket: str, key: str) -> bool:
    """Check if given bucket, key exists"""
    exists = False
    try:
        S3.head_object(Bucket=bucket, Key=key)
        exists = True
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            logger.error(f"s3 key does not exist: s3://{bucket}/{key}")
        else:
            logger.exception(e)
            logger.error(f"Unknown ClientError: {e.args}")

    return exists


def get_item_dot_separated(target_dictionary: dict, key: Hashable, default: Any = None, raise_key_error: bool = True) -> Any:
    """
    ２階層やそれよりも深い階層の辞書アイテムにアクセスする関数。DOT-SEPARATEDな辞書キーを使用してアクセスできる。
    :param target_dictionary: アイテムを取り出したい辞書。
    :param key: DOT-SEPARATEDなStringか、または単にHashable。
    :param default: 辞書キーが見つからなかった場合に返すデフォルト値。
    :param raise_key_error: 辞書キーが見つからなかった場合にkey_errorを返すかどうか。
    :return: 辞書キーを解決できた場合には該当の辞書アイテム、解決できずraise_key_errorでない場合はデフォルト値を返す。
    >>> target_dict = {'a1': {'b': {'c1': 100, 'c2': 200, 'c3': 'TARGET'}, 'd': 1000},
    ...                'a2': {'b': {'c1': 100, 'c2': 200, 'c3': 'DUMMY'}, 'd': 1000},
    ...                100: 100,
    ...                200: {300: 300}}
    >>> get_item_dot_separated(target_dict, 'a1.b.c3', None)
    'TARGET'
    >>> get_item_dot_separated(target_dict, 'a2.b.c3', None)
    'DUMMY'
    >>> get_item_dot_separated(target_dict, 'a.b.c3', None)
    Traceback (most recent call last):
    ...
    KeyError: 'a.b.c3 at a'
    >>> get_item_dot_separated(target_dict, 'a1.b.c4', None)
    Traceback (most recent call last):
    ...
    KeyError: 'a1.b.c4 at c4'
    >>> get_item_dot_separated(target_dict, 'a1.b', None)
    {'c1': 100, 'c2': 200, 'c3': 'TARGET'}
    >>> get_item_dot_separated(target_dict, 100, None)
    100
    >>> get_item_dot_separated(target_dict, '200.300', None) # not work with integer key
    Traceback (most recent call last):
    ...
    KeyError: '200.300 at 200'
    >>> get_item_dot_separated(target_dict, 'a.b.c3', 'DEFAULT', raise_key_error=False)
    'DEFAULT'
    """
    if not isinstance(key, str):
        if key not in target_dictionary.keys() and raise_key_error:
            raise KeyError(key)
        return target_dictionary.get(key, default)

    if "." not in key:
        if key not in target_dictionary.keys() and raise_key_error:
            raise KeyError(key)
        return target_dictionary.get(key, default)

    dot_separated_keys = key.split(".")
    layer = target_dictionary
    for key_at_layer in dot_separated_keys:
        if key_at_layer not in layer.keys():
            if raise_key_error:
                raise KeyError(f"{key} at {key_at_layer}")
            return default
        layer = layer[key_at_layer]
    return layer
