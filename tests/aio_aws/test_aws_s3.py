
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
from notes.aio_aws.aio_aws_s3 import run_aws_s3_seq_buckets


@pytest.fixture
def s3_bucket_name() -> str:
    return "moto_bucket"


@pytest.fixture
def s3_bucket(s3_bucket_name, aws_s3_client) -> str:
    resp = aws_s3_client.create_bucket(Bucket=s3_bucket_name)
    assert response_success(resp)
    head = aws_s3_client.head_bucket(Bucket=s3_bucket_name)
    assert response_success(head)
    return s3_bucket_name


def test_aws_sync_bucket_access(aws_s3_client, s3_bucket):
    bucket_access = run_aws_s3_seq_buckets(aws_s3_client)
    assert isinstance(bucket_access, dict)
    assert bucket_access == {"moto_bucket": True}


@pytest.mark.skip
@pytest.mark.s3_live
def test_aws_sync_live_bucket_access():
    bucket_access = run_aws_s3_seq_buckets()
    assert isinstance(bucket_access, dict)
    assert bucket_access


def test_aws_bucket_access(aws_s3_client, s3_bucket):
    resp = aws_s3_client.list_buckets()
    assert response_success(resp)
    bucket_names = [b["Name"] for b in resp["Buckets"]]
    assert bucket_names == [s3_bucket]
