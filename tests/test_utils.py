from uuid import UUID

import pytest
from igata.utils import flatten, generate_request_id


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
