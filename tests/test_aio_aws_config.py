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

"""
Test the aio_aws.aio_aws_config module
"""
from types import ModuleType

import aiobotocore.client
import aiobotocore.config
import aiobotocore.session
import botocore.client
import botocore.config
import botocore.session
import pytest

from aio_aws.aio_aws_config import MAX_JITTER
from aio_aws.aio_aws_config import MAX_PAUSE
from aio_aws.aio_aws_config import MIN_JITTER
from aio_aws.aio_aws_config import MIN_PAUSE
from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_config import aio_aws_client
from aio_aws.aio_aws_config import aio_aws_default_config
from aio_aws.aio_aws_config import aio_aws_default_session
from aio_aws.aio_aws_config import aio_aws_session
from aio_aws.aio_aws_config import delay
from aio_aws.aio_aws_config import jitter


@pytest.fixture
def aio_config(aio_aws_session, aio_aws_s3_server) -> AioAWSConfig:
    class TestConfig(AioAWSConfig):
        session = aio_aws_session

    config = TestConfig(
        min_pause=0.2,
        max_pause=0.6,
        min_jitter=0.1,
        max_jitter=0.2,
    )
    yield config


def test_aio_aws_package():
    import aio_aws.aio_aws_config

    assert isinstance(aio_aws.aio_aws_config, ModuleType)


@pytest.mark.asyncio
async def test_async_delay():
    min_delay = 0.0
    max_delay = 0.5
    pause = await delay("delay_task", min_delay, max_delay)
    assert isinstance(pause, float)
    assert min_delay <= pause <= max_delay


@pytest.mark.asyncio
async def test_async_jitter():
    min_jitter = 0.0
    max_jitter = 0.5
    pause = await jitter("jitter_task", min_jitter, max_jitter)
    assert isinstance(pause, float)
    assert min_jitter <= pause <= max_jitter


@pytest.mark.skip("Skip slower default delay")
@pytest.mark.asyncio
async def test_async_delay_defaults():
    pause = await delay("delay_task")
    assert isinstance(pause, float)
    assert MIN_PAUSE <= pause <= MAX_PAUSE


@pytest.mark.skip("Skip slower default jitter")
@pytest.mark.asyncio
async def test_async_jitter_defaults():
    pause = await jitter("jitter_task")
    assert isinstance(pause, float)
    assert MIN_JITTER <= pause <= MAX_JITTER


@pytest.mark.asyncio
async def test_aio_aws_config(aio_config, aio_aws_s3_server):

    # see also
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

    client_config = aio_config.default_client_config
    client_config = client_config.merge(
        botocore.client.Config(
            read_timeout=360,
            max_pool_connections=100,
        )
    )

    async with aio_config.create_client(
        "s3", endpoint_url=aio_aws_s3_server, config=client_config
    ) as client:
        assert client
        assert "aiobotocore.client.S3" in str(client)
        assert isinstance(client, aiobotocore.client.AioBaseClient)
        assert isinstance(client, botocore.client.BaseClient)
        config = client.meta.config
        assert isinstance(config, aiobotocore.config.AioConfig)
        assert isinstance(config, botocore.config.Config)
        assert client.meta.config.max_pool_connections == 100
        assert client.meta.config.read_timeout == 360
        assert client.meta.endpoint_url == aio_aws_s3_server


def test_default_config():
    config = aio_aws_default_config()
    assert isinstance(config, aiobotocore.config.AioConfig)
    assert isinstance(config, botocore.config.Config)


def test_default_session():
    session = aio_aws_default_session()
    assert isinstance(session, aiobotocore.session.AioSession)
    assert isinstance(session, botocore.session.Session)
    assert session.user_agent_name == "aio-aws"


def test_aio_aws_session():
    session = aio_aws_session()
    assert isinstance(session, aiobotocore.session.AioSession)
    assert isinstance(session, botocore.session.Session)
    assert session.user_agent_name == "aio-aws"


@pytest.mark.asyncio
async def test_aio_aws_client():
    async with aio_aws_client("s3") as client:
        assert "aiobotocore.client.S3" in str(client)
        assert isinstance(client, aiobotocore.client.AioBaseClient)
        assert isinstance(client, botocore.client.BaseClient)


@pytest.mark.asyncio
async def test_aio_aws_client_configs(aio_aws_s3_server):
    client_config = botocore.client.Config(
        read_timeout=360,
        max_pool_connections=100,
    )
    async with aio_aws_client(
        "s3", endpoint_url=aio_aws_s3_server, config=client_config
    ) as client:
        assert client
        assert "aiobotocore.client.S3" in str(client)
        assert isinstance(client, aiobotocore.client.AioBaseClient)
        assert isinstance(client, botocore.client.BaseClient)
        config = client.meta.config
        assert isinstance(config, aiobotocore.config.AioConfig)
        assert isinstance(config, botocore.config.Config)
        assert client.meta.config.max_pool_connections == 100
        assert client.meta.config.read_timeout == 360
        assert client.meta.endpoint_url == aio_aws_s3_server
