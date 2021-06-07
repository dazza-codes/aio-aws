"""
Test S3URI
"""
import datetime
import inspect
import pickle
from pathlib import Path

import boto3
import botocore
import botocore.exceptions
import pytest
from moto import mock_s3

from aio_aws.s3_uri import bucket_validate
from aio_aws.s3_uri import LOGGER
from aio_aws.s3_uri import S3URI


@pytest.fixture
def s3_uri(s3_uri_str) -> S3URI:
    return S3URI.parse_s3_uri(s3_uri_str)


def test_s3_uri():
    assert inspect.isclass(S3URI)


def test_s3_uri_accepts_s3_uri(s3_uri):
    assert isinstance(s3_uri, S3URI)


def test_s3_uri_as_str(s3_uri_str):
    s3_uri = S3URI.parse_s3_uri(s3_uri_str)
    assert isinstance(s3_uri, S3URI)
    assert str(s3_uri) == s3_uri_str


def test_pickle_s3_uri(s3_uri):
    assert pickle.loads(pickle.dumps(s3_uri))


def test_hash_s3_uri(s3_uri):
    assert hash(s3_uri)


def test_s3_uri_repr(s3_uri):
    assert S3URI.__name__ in repr(s3_uri)
    assert s3_uri.s3_uri in repr(s3_uri)


def test_s3_uri_str(s3_uri):
    assert S3URI.__name__ not in str(s3_uri)
    assert s3_uri.s3_uri == str(s3_uri)


def test_s3_uri_ordering():
    s3_uri_a = S3URI.parse_s3_uri("s3://s3-bucket/path/a")
    s3_uri_b = S3URI.parse_s3_uri("s3://s3-bucket/path/b")
    s3_uri_1 = S3URI.parse_s3_uri("s3://s3-bucket/path/1")
    s3_uri_9 = S3URI.parse_s3_uri("s3://s3-bucket/path/9")
    sorted_uris = sorted([s3_uri_9, s3_uri_b, s3_uri_1, s3_uri_a])
    assert sorted_uris == [s3_uri_1, s3_uri_9, s3_uri_a, s3_uri_b]


def test_s3_uri_has_bucket_str(s3_uri, s3_bucket_name):
    assert isinstance(s3_uri.bucket, str)
    assert s3_uri.bucket == s3_bucket_name


def test_s3_uri_has_key_str(s3_uri, s3_key):
    assert isinstance(s3_uri.key, str)
    assert s3_uri.key == s3_key


def test_s3_uri_has_key_path_str(s3_uri, s3_key_path):
    assert isinstance(s3_uri.key_path, str)
    assert s3_uri.key_path == s3_key_path
    assert s3_uri.key_path in s3_uri.key
    assert s3_uri.key_path != s3_uri.key


def test_s3_uri_has_key_file_str(s3_uri, s3_key_file):
    assert isinstance(s3_uri.key_file, str)
    assert s3_uri.key_file == s3_key_file
    assert s3_uri.key_file in s3_uri.key
    assert s3_uri.key_file != s3_uri.key


def test_s3_uri_separates_key_path_and_key_file(s3_uri, s3_key_path, s3_key_file):
    assert s3_uri.key_file not in s3_uri.key_path


def test_s3_uri_has_no_key(s3_bucket_name):
    uri = f"s3://{s3_bucket_name}"
    s3_uri = S3URI.parse_s3_uri(uri)
    assert isinstance(s3_uri, S3URI)
    assert s3_uri.bucket == s3_bucket_name
    assert s3_uri.key == ""
    assert s3_uri.key_path == ""
    assert s3_uri.key_file == ""


def test_s3_uri_has_no_file(s3_bucket_name, s3_key_path):
    uri = f"s3://{s3_bucket_name}/{s3_key_path}"
    s3_uri = S3URI.parse_s3_uri(uri)
    assert isinstance(s3_uri, S3URI)
    assert s3_uri.bucket == s3_bucket_name
    assert s3_uri.key == s3_key_path
    assert s3_uri.key_path == s3_key_path
    assert s3_uri.key_file == ""


def test_s3_uri_has_glob_pattern(s3_uri, s3_key_path):
    glob_pattern = s3_uri.glob_pattern()
    assert isinstance(glob_pattern, str)
    assert glob_pattern == f"{s3_key_path}/**/*"


def test_s3_uri_has_glob_file_pattern(s3_uri, s3_key_path, s3_key_file):
    glob_file_pattern = s3_uri.glob_file_pattern()
    assert isinstance(glob_file_pattern, str)
    file_stem = Path(s3_key_file).stem
    assert glob_file_pattern == f"{s3_key_path}/**/{file_stem}*.*"


def test_s3_head_request_for_success(s3_uri_object, mocker):
    # s3_uri_object fixture exists
    s3_uri = S3URI(s3_uri_object.s3_uri)

    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    s3_head = s3_uri.s3_head_request()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 1
    assert boto3.resource.call_count == 0
    assert isinstance(s3_head, dict)
    assert list(s3_head.keys()) == [
        "ResponseMetadata",
        "LastModified",
        "ContentLength",
        "ETag",
        "Metadata",
    ]


@mock_s3
def test_s3_head_request_for_missing_bucket(s3_uri, mocker):
    # there is no s3 bucket
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    mocker.spy(LOGGER, "warning")
    s3_head = s3_uri.s3_head_request()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 1
    assert boto3.resource.call_count == 0
    LOGGER.warning.assert_called_once_with(
        "Missing object, %s for %s", "NoSuchBucket", s3_uri.s3_uri
    )
    assert s3_head == {}


def test_s3_head_request_for_missing_key(s3_bucket, s3_uri, mocker):
    # with a mock s3 bucket
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    mocker.spy(LOGGER, "warning")
    s3_head = s3_uri.s3_head_request()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 1
    assert boto3.resource.call_count == 0
    LOGGER.warning.assert_called_once_with(
        "Missing object, %s for %s", "404", s3_uri.s3_uri
    )
    assert s3_head == {}
    assert s3_bucket


@pytest.mark.skip("It's not easy to mock bad credentials")
def test_s3_head_request_for_bad_credentials(s3_uri, mocker, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "bad")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "bad")
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    mocker.spy(LOGGER, "error")
    s3_head = s3_uri.s3_head_request()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 1
    assert boto3.resource.call_count == 0
    LOGGER.error.assert_called_once_with("Access denied for %s", s3_uri.s3_uri)
    assert s3_head == {}


def test_s3_exists_for_success(s3_uri_object, mocker):
    # s3_uri_object fixture exists
    s3_uri = S3URI(s3_uri_object.s3_uri)
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    s3_uri_exists = s3_uri.s3_exists()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 0
    assert boto3.resource.call_count == 1
    assert s3_uri_exists is True


def test_s3_exists_for_failure(s3_bucket, s3_uri, mocker):
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    mocker.spy(LOGGER, "warning")
    s3_uri_exists = s3_uri.s3_exists()
    # the s3 client is used once to check the s3 object exists
    assert boto3.client.call_count == 0
    assert boto3.resource.call_count == 1
    LOGGER.warning.assert_called_once_with(
        "Missing object, %s for %s", "404", s3_uri.s3_uri
    )
    assert s3_uri_exists is False


def test_s3_object_summary_for_success(s3_uri_object, mocker):
    # s3_uri_object fixture exists
    s3_uri = S3URI(s3_uri_object.s3_uri)

    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    summary = s3_uri.s3_object_summary()
    # the s3 resource is used once to check the s3 object exists
    assert boto3.client.call_count == 0
    assert boto3.resource.call_count == 1

    assert summary.__class__.__name__ == "s3.ObjectSummary"
    assert summary.bucket_name == s3_uri.bucket
    assert summary.key == s3_uri.key
    assert isinstance(summary.last_modified, datetime.datetime)


def test_s3_object_summary_for_failure(s3_bucket, s3_uri, mocker):
    mocker.spy(boto3, "client")
    mocker.spy(boto3, "resource")
    summary = s3_uri.s3_object_summary()
    # the s3 resource is used once to check the s3 object exists
    assert boto3.client.call_count == 0
    assert boto3.resource.call_count == 1

    assert summary.__class__.__name__ == "s3.ObjectSummary"
    assert summary.bucket_name == s3_uri.bucket
    assert summary.key == s3_uri.key
    with pytest.raises(botocore.exceptions.ClientError):
        assert isinstance(summary.last_modified, datetime.datetime)


def test_s3_file_derivatives(s3_temp_file, s3_temp_objects):
    s3_uri = S3URI.parse_s3_uri(s3_temp_file.s3_uri)
    file_path = Path(s3_uri.key_file)
    s3_objects = list(s3_uri.s3_derivatives())
    s3_derivative_keys = set([obj.key for obj in s3_objects])
    assert s3_derivative_keys
    for key in s3_derivative_keys:
        assert key != s3_uri.key
        assert file_path.stem in Path(key).stem
    # derivative keys are in anything in key-path/**/stem*.*
    s3_temp_keys = [obj.key for obj in s3_temp_objects]
    key_intersect = set(s3_temp_keys).intersection(s3_derivative_keys)
    assert s3_derivative_keys == key_intersect


def test_s3_objects(s3_bucket_name, s3_temp_objects):
    s3_uri = S3URI.parse_s3_uri(f"s3://{s3_bucket_name}")
    s3_objects = list(s3_uri.s3_objects())
    s3_keys = sorted([obj.key for obj in s3_objects])
    s3_temp_keys = sorted([obj.key for obj in s3_temp_objects])
    assert s3_keys == s3_temp_keys


def test_s3_objects_prefix(s3_bucket_name, s3_temp_objects, s3_temp_dir):
    # create a mock s3 bucket with a couple of files in it
    s3_uri = S3URI.parse_s3_uri(f"s3://{s3_bucket_name}")
    s3_prefix = s3_temp_dir.split("/")[0]
    s3_objects = list(s3_uri.s3_objects(prefix=s3_prefix))
    s3_keys = sorted([obj.key for obj in s3_objects])
    s3_temp_keys = sorted([obj.key for obj in s3_temp_objects])
    assert s3_keys == s3_temp_keys


def test_s3_objects_glob(s3_bucket_name, s3_temp_objects):
    s3_uri = S3URI.parse_s3_uri(f"s3://{s3_bucket_name}")
    s3_objects = list(s3_uri.s3_objects(glob_pattern="**/*.tif"))
    s3_keys = sorted([obj.key for obj in s3_objects])
    s3_temp_keys = sorted([obj.key for obj in s3_temp_objects])
    s3_temp_tifs = [key for key in s3_temp_keys if key.endswith(".tif")]
    assert s3_keys == s3_temp_tifs


def test_s3_objects_glob_hundreds(s3_bucket_name, s3_temp_1000s_objects):
    # create a mock s3 bucket with hundreds of files in it; this
    # should exceed the default MaxKeys limit on a filter because
    # the implementation in S3URI.s3_objects is unlimited.
    s3_uri = S3URI.parse_s3_uri(f"s3://{s3_bucket_name}")
    s3_objects = list(s3_uri.s3_objects(glob_pattern="**/*.tif"))
    s3_keys = sorted([obj.key for obj in s3_objects])
    s3_temp_keys = sorted([obj.key for obj in s3_temp_1000s_objects])
    s3_temp_tifs = [key for key in s3_temp_keys if key.endswith(".tif")]
    assert s3_keys == s3_temp_tifs


#
# Errors
#


def test_s3_uri_has_no_schema():
    with pytest.raises(ValueError) as err:
        S3URI.parse_s3_uri("bucket/key")
    assert "The s3_uri is invalid" in err.value.args[0]


def test_s3_uri_has_no_paths():
    with pytest.raises(ValueError) as err:
        S3URI.parse_s3_uri("s3://")
    assert "The s3_uri is invalid" in err.value.args[0]


def test_s3_file_init_fails_with_invalid_bucket():
    with pytest.raises(ValueError) as err:
        # the '_' character is not allowed
        S3URI.parse_s3_uri("s3://money_buckets/more_money")
    assert "The s3_uri is invalid" in err.value.args[0]


def test_s3_invalid_bucket():
    bucket_name = "money_buckets"
    with pytest.raises(ValueError) as err:
        # the '_' character is not allowed
        bucket_validate("money_buckets")
    assert "The bucket_name is invalid" in err.value.args[0]
    assert bucket_name in err.value.args[0]