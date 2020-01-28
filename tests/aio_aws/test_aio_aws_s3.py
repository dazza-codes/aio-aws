
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

import pytest

from notes.aio_aws.aio_aws import response_success


@pytest.fixture
def aio_s3_bucket_name() -> str:
    return "aio_moto_bucket"


@pytest.fixture
async def aio_s3_bucket(aio_s3_bucket_name, aio_aws_s3_client) -> str:
    resp = await aio_aws_s3_client.create_bucket(Bucket=aio_s3_bucket_name)
    assert response_success(resp)
    head = await aio_aws_s3_client.head_bucket(Bucket=aio_s3_bucket_name)
    assert response_success(head)
    return aio_s3_bucket_name


@pytest.mark.asyncio
async def test_aio_aws_bucket_access(aio_aws_s3_client, aio_s3_bucket):
    resp = await aio_aws_s3_client.list_buckets()
    assert response_success(resp)
    bucket_names = [b["Name"] for b in resp["Buckets"]]
    assert bucket_names == [aio_s3_bucket]
