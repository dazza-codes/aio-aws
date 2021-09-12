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
Test Asyncio AWS Client Fixtures

This test suite checks fixtures for aiobotocore clients.

Do _not_ use default moto mock decorators, which incur:
AttributeError: 'AWSResponse' object has no attribute 'raw_headers'
.. seealso:: https://github.com/aio-libs/aiobotocore/issues/755
"""

import os

import pytest
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession

from aio_aws.utils import response_success
from tests.fixtures.aiomoto_fixtures import AioAwsBatchClients
from tests.utils import AWS_ACCESS_KEY_ID
from tests.utils import AWS_REGION
from tests.utils import AWS_SECRET_ACCESS_KEY
from tests.utils import has_moto_mocks


@pytest.mark.asyncio
async def test_aio_aws_session_credentials(aio_aws_session):
    assert isinstance(aio_aws_session, AioSession)
    credentials = await aio_aws_session.get_credentials()
    assert credentials.access_key == AWS_ACCESS_KEY_ID
    assert credentials.secret_key == AWS_SECRET_ACCESS_KEY
    assert os.getenv("AWS_ACCESS_KEY_ID")
    assert os.getenv("AWS_SECRET_ACCESS_KEY")
    assert os.getenv("AWS_ACCESS_KEY_ID") == AWS_ACCESS_KEY_ID
    assert os.getenv("AWS_SECRET_ACCESS_KEY") == AWS_SECRET_ACCESS_KEY


def test_aio_aws_batch_clients(aio_aws_batch_clients):
    clients = aio_aws_batch_clients
    assert isinstance(clients, AioAwsBatchClients)
    assert isinstance(clients.batch, AioBaseClient)
    assert isinstance(clients.ec2, AioBaseClient)
    assert isinstance(clients.ecs, AioBaseClient)
    assert isinstance(clients.iam, AioBaseClient)
    assert isinstance(clients.logs, AioBaseClient)
    assert isinstance(clients.region, str)
    assert clients.region == AWS_REGION


@pytest.mark.asyncio
async def test_aio_aws_batch_client(aio_aws_batch_client):
    client = aio_aws_batch_client
    assert isinstance(client, AioBaseClient)

    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = await client.describe_job_queues()
    assert response_success(resp)
    assert resp.get("jobQueues") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.batch.DescribeJobQueues")


@pytest.mark.asyncio
async def test_aio_aws_ec2_client(aio_aws_ec2_client):
    client = aio_aws_ec2_client
    assert isinstance(client, AioBaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = await client.describe_instances()
    assert response_success(resp)
    assert resp.get("Reservations") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.ec2.DescribeInstances")


@pytest.mark.asyncio
async def test_aio_aws_ecs_client(aio_aws_ecs_client):
    client = aio_aws_ecs_client
    assert isinstance(client, AioBaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = await client.list_task_definitions()
    assert response_success(resp)
    assert resp.get("taskDefinitionArns") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.ecs.ListTaskDefinitions")


@pytest.mark.asyncio
async def test_aio_aws_iam_client(aio_aws_iam_client):
    client = aio_aws_iam_client
    assert isinstance(client, AioBaseClient)
    assert client.meta.config.region_name == "aws-global"  # not AWS_REGION
    assert client.meta.region_name == "aws-global"  # not AWS_REGION

    resp = await client.list_roles()
    assert response_success(resp)
    assert resp.get("Roles") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.iam.ListRoles")


@pytest.mark.asyncio
async def test_aio_aws_logs_client(aio_aws_logs_client):
    client = aio_aws_logs_client
    assert isinstance(client, AioBaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = await client.describe_log_groups()
    assert response_success(resp)
    assert resp.get("logGroups") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.cloudwatch-logs.DescribeLogGroups")


@pytest.mark.asyncio
async def test_aio_aws_s3_client(aio_aws_s3_client):
    client = aio_aws_s3_client
    assert isinstance(client, AioBaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = await client.list_buckets()
    assert response_success(resp)
    assert resp.get("Buckets") == []

    # the event-name mocks are dynamically generated after calling the method;
    # for aio-clients, they should be disabled for aiohttp to hit moto.server.
    assert not has_moto_mocks(client, "before-send.s3.ListBuckets")


@pytest.mark.asyncio
async def test_aio_aws_client(aio_aws_client):
    # aio_aws_client is an async generator
    # aio_aws_client(service_name) yields a client
    async for client in aio_aws_client("s3"):
        assert isinstance(client, AioBaseClient)
        assert client.meta.config.region_name == AWS_REGION
        assert client.meta.region_name == AWS_REGION

        resp = await client.list_buckets()
        assert response_success(resp)
        assert resp.get("Buckets") == []

        # the event-name mocks are dynamically generated after calling the method;
        # for aio-clients, they should be disabled for aiohttp to hit moto.server.
        assert not has_moto_mocks(client, "before-send.s3.ListBuckets")
