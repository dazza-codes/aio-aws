# Copyright 2019-2023 Darren Weber
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

import aiobotocore
import aiobotocore.client
import aiobotocore.config
import pytest

from aio_aws.aio_aws_config import AioAWSConfig


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
