# Copyright 2019-2021 Darren Weber
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

from contextlib import asynccontextmanager
from typing import List

import aiobotocore
import aiobotocore.client
import aiobotocore.config
import pytest

from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_s3 import S3Parts
from aio_aws.utils import response_success


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
async def aio_s3_bucket(aio_s3_uri, aio_aws_s3_client, aws_region) -> str:
    s3_parts = S3Parts.parse_s3_uri(aio_s3_uri)
    resp = await aio_aws_s3_client.create_bucket(
        Bucket=s3_parts.bucket,
        ACL="public-read-write",
        CreateBucketConfiguration={"LocationConstraint": aws_region},
    )
    assert response_success(resp)
    head = await aio_aws_s3_client.head_bucket(Bucket=s3_parts.bucket)
    assert response_success(head)

    return s3_parts.bucket


@pytest.fixture
async def aio_s3_buckets(
    aio_s3_bucket_name, aio_aws_s3_client, aws_region
) -> List[str]:
    bucket_names = []
    for i in range(20):
        bucket_name = f"{aio_s3_bucket_name}_{i:02d}"
        resp = await aio_aws_s3_client.create_bucket(
            Bucket=bucket_name,
            ACL="public-read-write",
            CreateBucketConfiguration={"LocationConstraint": aws_region},
        )
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


@pytest.fixture
def s3_config(aio_aws_session, aio_aws_s3_server) -> AioAWSConfig:
    class TestS3Config(AioAWSConfig):
        session = aio_aws_session

        @asynccontextmanager
        async def create_client(self, service: str) -> aiobotocore.client.AioBaseClient:
            async with aio_aws_session.create_client(
                "s3", endpoint_url=aio_aws_s3_server
            ) as client:
                yield client

    config = TestS3Config(
        min_pause=0.2,
        max_pause=0.6,
        min_jitter=0.1,
        max_jitter=0.2,
    )

    yield config
