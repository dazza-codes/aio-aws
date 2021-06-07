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
import uuid
from typing import List

import pytest

from aio_aws.utils import response_success
from aio_aws.aio_aws_s3 import aws_s3_buckets_access


@pytest.fixture
def s3_bucket_name() -> str:
    return "moto_bucket" + str(uuid.uuid4())


@pytest.fixture
def s3_bucket(s3_bucket_name, aws_s3_client, aws_region) -> str:
    resp = aws_s3_client.create_bucket(
        Bucket=s3_bucket_name,
        ACL="public-read-write",
        CreateBucketConfiguration={"LocationConstraint": aws_region},
    )
    assert response_success(resp)

    # Ensure the bucket exists
    exists_waiter = aws_s3_client.get_waiter("bucket_exists")
    exists_waiter.wait(Bucket=s3_bucket_name)

    head = aws_s3_client.head_bucket(Bucket=s3_bucket_name)
    assert response_success(head)
    return s3_bucket_name


@pytest.fixture
def s3_buckets(s3_bucket_name, aws_s3_client, aws_region) -> List[str]:
    bucket_names = []
    for i in range(20):
        bucket_name = f"{s3_bucket_name}_{i:02d}"
        resp = aws_s3_client.create_bucket(
            Bucket=bucket_name,
            ACL="public-read-write",
            CreateBucketConfiguration={"LocationConstraint": aws_region},
        )
        assert response_success(resp)
        # Ensure the bucket exists
        exists_waiter = aws_s3_client.get_waiter("bucket_exists")
        exists_waiter.wait(Bucket=bucket_name)

        head = aws_s3_client.head_bucket(Bucket=bucket_name)
        assert response_success(head)
        bucket_names.append(bucket_name)
    return bucket_names


def test_s3_list_buckets(aws_s3_client, s3_buckets):
    resp = aws_s3_client.list_buckets()
    assert response_success(resp)
    bucket_names = [b["Name"] for b in resp["Buckets"]]
    assert bucket_names == s3_buckets


def test_s3_buckets_access(aws_s3_client, s3_buckets):
    buckets = aws_s3_buckets_access(aws_s3_client)
    assert isinstance(buckets, dict)
    for bucket in buckets:
        assert buckets[bucket] is True
