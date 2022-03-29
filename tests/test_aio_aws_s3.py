# Copyright 2019-2022 Darren Weber
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
from typing import Dict

import aiobotocore
import aiobotocore.client
import aiobotocore.config
import aiobotocore.session
import botocore.exceptions
import pytest

from aio_aws.aio_aws_s3 import aio_s3_bucket_access
from aio_aws.aio_aws_s3 import aio_s3_bucket_head
from aio_aws.aio_aws_s3 import aio_s3_buckets_access
from aio_aws.aio_aws_s3 import aio_s3_buckets_list
from aio_aws.aio_aws_s3 import aio_s3_object_head
from aio_aws.s3_uri import S3URI
from aio_aws.utils import response_success


def valid_head_response(resp: Dict, head: Dict) -> bool:
    resp_meta = resp["ResponseMetadata"]
    resp_headers = resp_meta["HTTPHeaders"]
    resp_headers.pop("x-amzn-requestid", None)

    head_meta = head["ResponseMetadata"]
    head_headers = resp_meta["HTTPHeaders"]
    head_headers.pop("x-amzn-requestid", None)

    assert resp_meta["HTTPStatusCode"] == 200
    assert head_meta["HTTPStatusCode"] == 200
    assert resp_headers == head_headers
    return True


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
async def test_aio_s3_bucket_head(aio_s3_bucket, aio_aws_s3_client, s3_config):
    resp = await aio_aws_s3_client.head_bucket(Bucket=aio_s3_bucket)
    assert response_success(resp)

    head = await aio_s3_bucket_head(
        aio_s3_bucket, config=s3_config, s3_client=aio_aws_s3_client
    )
    assert response_success(head)
    assert valid_head_response(resp=resp, head=head)


@pytest.mark.asyncio
async def test_aio_s3_bucket_access(aio_s3_bucket, aio_aws_s3_client, s3_config):
    bucket_name, access = await aio_s3_bucket_access(
        aio_s3_bucket, config=s3_config, s3_client=aio_aws_s3_client
    )
    assert bucket_name == aio_s3_bucket
    assert access is True


@pytest.mark.asyncio
async def test_aio_s3_bucket_access_with_missing_bucket(
    aio_s3_bucket, aio_aws_s3_client, s3_config
):
    bucket = "missing_bucket"
    bucket_name, access = await aio_s3_bucket_access(
        bucket, config=s3_config, s3_client=aio_aws_s3_client
    )
    assert bucket_name == bucket
    assert access is False


@pytest.mark.asyncio
async def test_aio_s3_buckets_list(aio_s3_buckets, aio_aws_s3_client, s3_config):
    resp = await aio_s3_buckets_list(config=s3_config, s3_client=aio_aws_s3_client)
    assert response_success(resp)
    buckets = [bucket["Name"] for bucket in resp["Buckets"]]
    assert buckets == aio_s3_buckets


@pytest.mark.asyncio
async def test_aio_s3_buckets_access(aio_s3_buckets, aio_aws_s3_client, s3_config):
    result = await aio_s3_buckets_access(
        aio_s3_buckets, config=s3_config, s3_client=aio_aws_s3_client
    )
    assert result
    for bucket in aio_s3_buckets:
        assert result[bucket] is True


@pytest.mark.asyncio
async def test_aio_s3_object_head(
    aio_s3_uri, aio_s3_bucket, aio_s3_object_text, aio_aws_s3_client, s3_config
):
    s3_uri = S3URI.parse_s3_uri(aio_s3_uri)
    resp = await aio_aws_s3_client.put_object(
        Bucket=s3_uri.bucket,
        Key=s3_uri.key,
        Body=aio_s3_object_text,
        ACL="public-read-write",
    )
    assert response_success(resp)
    resp = await aio_aws_s3_client.head_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
    assert response_success(resp)

    head = await aio_s3_object_head(
        aio_s3_uri, config=s3_config, s3_client=aio_aws_s3_client
    )
    assert response_success(head)
    assert valid_head_response(resp=resp, head=head)


@pytest.mark.asyncio
async def test_aio_s3_object_head_for_missing_object(
    aio_s3_bucket, aio_aws_s3_client, s3_config
):
    with pytest.raises(botocore.exceptions.ClientError) as err:
        s3_uri = f"s3://{aio_s3_bucket}/missing_key"
        await aio_s3_object_head(s3_uri, config=s3_config, s3_client=aio_aws_s3_client)

    msg = err.value.args[0]
    assert "HeadObject operation" in msg
    assert "404" in msg


@pytest.mark.asyncio
async def test_aio_s3_bucket_head_not_authorized():

    session = aiobotocore.session.get_session()
    aio_config = aiobotocore.config.AioConfig(max_pool_connections=1)
    session.set_default_client_config(aio_config)
    session.set_credentials("fake_AWS_ACCESS_KEY_ID", "fake_AWS_SECRET_ACCESS_KEY")

    async with session.create_client("s3") as client:
        with pytest.raises(botocore.exceptions.ClientError) as err:
            await client.head_bucket(Bucket="missing-bucket")

    msg = err.value.args[0]
    assert "HeadBucket operation" in msg
    assert "403" in msg
    assert "Forbidden" in msg


@pytest.mark.skip("https://github.com/aio-libs/aiobotocore/issues/781")
@pytest.mark.asyncio
async def test_aio_s3_bucket_head_too_many_requests():

    session = aiobotocore.session.get_session()
    aio_config = aiobotocore.config.AioConfig(max_pool_connections=1)
    session.set_default_client_config(aio_config)
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
