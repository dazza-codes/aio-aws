# Copyright 2020 Darren Weber
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List

import aiobotocore
import aiobotocore.config
import botocore.exceptions
import pytest

from notes.aio_aws.aio_aws import response_success
from notes.aio_aws.aio_aws_s3 import aio_s3_bucket_access
from notes.aio_aws.aio_aws_s3 import aio_s3_bucket_head
from notes.aio_aws.aio_aws_s3 import aio_s3_buckets_access
from notes.aio_aws.aio_aws_s3 import aio_s3_buckets_list
from notes.aio_aws.aio_aws_s3 import aio_s3_object_head
from notes.aio_aws.aio_aws_s3 import S3Parts


@pytest.fixture
def aio_s3_bucket_name() -> str:
    """A valid S3 bucket name
    :return: str for the bucket component of 's3://{bucket}/{key}'
    """
    return "aio-moto-bucket"


@pytest.fixture
def aio_s3_key_path() -> str:
    """A valid S3 key name that is not a file name, it's like a directory
    The key component of 's3://{bucket}/{key}' that is composed of '{key_path}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return "aio_s3_key_path"


@pytest.fixture
def aio_s3_key_file() -> str:
    """A valid S3 key name that is also a file name
    The key component of 's3://{bucket}/{key}' that is composed of '{key_file}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return "aio_s3_file_test.txt"


@pytest.fixture
def aio_s3_key(aio_s3_key_path, aio_s3_key_file) -> str:
    """A valid S3 key composed of a key_path and a key_file
    The key component of 's3://{bucket}/{key}' that is composed of '{key_path}/{key_file}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return f"{aio_s3_key_path}/{aio_s3_key_file}"


@pytest.fixture
def aio_s3_uri(aio_s3_bucket_name, aio_s3_key) -> str:
    """A valid S3 URI comprised of 's3://{bucket}/{key}'
    :return: str
    """
    return f"s3://{aio_s3_bucket_name}/{aio_s3_key}"


@pytest.fixture
def aio_s3_object_text() -> str:
    """s3 object data: 's3 test object text\n'"""
    return "aio-s3 test object text\n"


@pytest.fixture
async def aio_s3_bucket(aio_s3_uri, aio_aws_s3_client) -> str:
    s3_parts = S3Parts.parse_s3_uri(aio_s3_uri)
    resp = await aio_aws_s3_client.create_bucket(
        Bucket=s3_parts.bucket, ACL="public-read-write"
    )
    assert response_success(resp)
    head = await aio_aws_s3_client.head_bucket(Bucket=s3_parts.bucket)
    assert response_success(head)

    return s3_parts.bucket


@pytest.fixture
async def aio_s3_buckets(aio_s3_bucket_name, aio_aws_s3_client) -> List[str]:
    bucket_names = []
    for i in range(20):
        bucket_name = f"{aio_s3_bucket_name}_{i:02d}"
        resp = await aio_aws_s3_client.create_bucket(Bucket=bucket_name)
        assert response_success(resp)
        head = await aio_aws_s3_client.head_bucket(Bucket=bucket_name)
        assert response_success(head)
        bucket_names.append(bucket_name)

    return bucket_names


@pytest.fixture
async def aio_s3_object_uri(
    aio_s3_uri, aio_s3_object_text, aio_s3_bucket, aio_aws_s3_client
) -> str:
    """s3 object data is PUT to the aio_s3_uri"""
    s3_parts = S3Parts.parse_s3_uri(aio_s3_uri)
    resp = await aio_aws_s3_client.put_object(
        Bucket=s3_parts.bucket,
        Key=s3_parts.key,
        Body=aio_s3_object_text,
        ACL="public-read-write",
    )
    assert response_success(resp)
    resp = await aio_aws_s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)
    assert response_success(resp)

    return aio_s3_uri


def test_s3_parts(aio_s3_uri):
    s3_parts = S3Parts.parse_s3_uri(aio_s3_uri)
    assert s3_parts.s3_uri == aio_s3_uri


def test_s3_parts_with_bad_uri():
    with pytest.raises(ValueError) as err:
        S3Parts.parse_s3_uri("file://tmp.txt")
    assert S3Parts.PROTOCOL in err.value.args[0]


@pytest.mark.asyncio
async def test_aio_s3_list_bucket(aio_aws_s3_client, aio_s3_bucket):
    resp = await aio_aws_s3_client.list_buckets()
    assert response_success(resp)
    bucket_names = [b["Name"] for b in resp["Buckets"]]
    assert bucket_names == [aio_s3_bucket]


@pytest.mark.asyncio
async def test_aio_s3_list_buckets(aio_aws_s3_client, aio_s3_buckets):
    resp = await aio_aws_s3_client.list_buckets()
    assert response_success(resp)
    bucket_names = [b["Name"] for b in resp["Buckets"]]
    assert bucket_names == aio_s3_buckets


@pytest.mark.asyncio
async def test_aio_s3_bucket_head(aio_s3_bucket, aio_aws_s3_client):
    resp = await aio_aws_s3_client.head_bucket(Bucket=aio_s3_bucket)
    assert response_success(resp)

    head = await aio_s3_bucket_head(aio_s3_bucket, s3_client=aio_aws_s3_client)
    assert response_success(head)
    assert resp == head


@pytest.mark.asyncio
async def test_aio_s3_bucket_access(aio_s3_bucket, aio_aws_s3_client):
    bucket_name, access = await aio_s3_bucket_access(aio_s3_bucket, s3_client=aio_aws_s3_client)
    assert bucket_name == aio_s3_bucket
    assert access is True


@pytest.mark.asyncio
async def test_aio_s3_bucket_access_with_missing_bucket(aio_s3_bucket, aio_aws_s3_client):
    bucket = "missing_bucket"
    bucket_name, access = await aio_s3_bucket_access(bucket, s3_client=aio_aws_s3_client)
    assert bucket_name == bucket
    assert access is False


@pytest.mark.asyncio
async def test_aio_s3_buckets_list(aio_s3_buckets, aio_aws_s3_client):
    resp = await aio_s3_buckets_list(s3_client=aio_aws_s3_client)
    assert response_success(resp)
    buckets = [bucket["Name"] for bucket in resp["Buckets"]]
    assert buckets == aio_s3_buckets


@pytest.mark.skip("Problems with test loop vs. code loop for some reason - why?")
@pytest.mark.asyncio
async def test_aio_s3_buckets_access(aio_s3_buckets, aio_aws_s3_client):
    result = await aio_s3_buckets_access(aio_s3_buckets, s3_client=aio_aws_s3_client)
    assert result
    for bucket in aio_s3_buckets:
        assert result[bucket] is True


@pytest.mark.asyncio
async def test_aio_s3_object_head(
    aio_s3_uri, aio_s3_bucket, aio_s3_object_text, aio_aws_s3_client
):
    s3_parts = S3Parts.parse_s3_uri(aio_s3_uri)
    resp = await aio_aws_s3_client.put_object(
        Bucket=s3_parts.bucket,
        Key=s3_parts.key,
        Body=aio_s3_object_text,
        ACL="public-read-write",
    )
    assert response_success(resp)
    resp = await aio_aws_s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)
    assert response_success(resp)

    head = await aio_s3_object_head(aio_s3_uri, s3_client=aio_aws_s3_client)
    assert response_success(head)
    assert resp == head


@pytest.mark.asyncio
async def test_aio_s3_object_head_for_missing_object(aio_s3_bucket, aio_aws_s3_client):
    with pytest.raises(botocore.exceptions.ClientError) as err:
        s3_uri = f"s3://{aio_s3_bucket}/missing_key"
        await aio_s3_object_head(s3_uri, s3_client=aio_aws_s3_client)

    msg = err.value.args[0]
    assert "HeadObject operation" in msg
    assert "404" in msg


@pytest.mark.skip("https://github.com/aio-libs/aiobotocore/issues/781")
@pytest.mark.asyncio
async def test_aio_s3_bucket_head_too_many_requests():

    session = aiobotocore.get_session()
    aioconfig = aiobotocore.config.AioConfig(max_pool_connections=1)
    session.set_default_client_config(aioconfig)
    session.set_credentials("fake_AWS_ACCESS_KEY_ID", "fake_AWS_SECRET_ACCESS_KEY")

    async with session.create_client("s3") as client:

        # TODO: HOW TO ADD HTTP STUBBER HERE, similar to:
        #   https://botocore.amazonaws.com/v1/documentation/api/latest/reference/stubber.html

        with pytest.raises(botocore.exceptions.ClientError) as err:
            await client.head_bucket(Bucket="missing-bucket")

        msg = err.value.args[0]
        assert "HeadBucket operation" in msg
        assert "403" in msg
        assert "Forbidden" in msg
