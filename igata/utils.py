import datetime
import json
import logging
import os
import time
import urllib
from collections import Hashable
from decimal import Decimal
from hashlib import md5
from typing import Generator, List, Tuple, Union
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from uuid import NAMESPACE_URL, uuid5

import boto3
import imageio
import numpy as np
import requests
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


def prepare_images(bucket, key) -> Tuple[Tuple[str, str], np.array, float, str]:
    """
    Read the given s3 key into a numpy array.from retry.api import retry_call
    """
    error = None
    key = unquote(key)
    url = S3.generate_presigned_url(ClientMethod="get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600, HttpMethod="GET")

    start = time.time()
    try:
        image = retry_call(imageio.imread, fargs=[url], tries=10)[:, :, :3]
    except HTTPError as e:
        logger.exception(e)
        logger.error(f"Exception while processing image(s3://{bucket}/{key}): ({e.code}) {e.reason}")
        image = np.array([])
    except ValueError as e:
        logger.exception(e)
        logger.error(f"Exception while processing image(s3://{bucket}/{key}): {e.args}")
        image = np.array([])
    end = time.time()
    download_time = end - start

    return (bucket, key), image, download_time, error


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
