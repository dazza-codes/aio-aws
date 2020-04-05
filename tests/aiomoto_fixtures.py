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

"""
AWS test fixtures

This test suite uses a large suite of moto mocks for the AWS batch
infrastructure. These infrastructure mocks are derived from the moto test
suite for testing the batch client. The test infrastructure should be used
according to the moto license. That license overrides any global license
applied to my aio_aws project.

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""

from typing import NamedTuple
from typing import Optional

import aiobotocore.client
import aiobotocore.config
import pytest

from tests.aiomoto_services import MotoService
from tests.utils import AWS_ACCESS_KEY_ID
from tests.utils import AWS_REGION
from tests.utils import AWS_SECRET_ACCESS_KEY


#
# Asyncio AWS Services
#


@pytest.fixture
async def aio_aws_batch_server():
    async with MotoService("batch") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_cloudformation_server():
    async with MotoService("cloudformation") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_ec2_server():
    async with MotoService("ec2") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_ecs_server():
    async with MotoService("ecs") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_iam_server():
    async with MotoService("iam") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_dynamodb2_server():
    async with MotoService("dynamodb2") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_logs_server():
    # cloud watch logs
    async with MotoService("logs") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_s3_server():
    async with MotoService("s3") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_sns_server():
    async with MotoService("sns") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


@pytest.fixture
async def aio_aws_sqs_server():
    async with MotoService("sqs") as svc:
        svc.reset()
        yield svc.endpoint_url
        svc.reset()


#
# Asyncio AWS Clients
#


@pytest.fixture
def aio_aws_session(aws_credentials, aws_region, event_loop):
    # pytest-asyncio provides and manages the `event_loop`
    # and it should be set as the default loop for this session
    assert event_loop  # but it's not == asyncio.get_event_loop() ?

    session = aiobotocore.get_session()
    session.user_agent_name = "aiomoto"

    assert session.get_default_client_config() is None
    aioconfig = aiobotocore.config.AioConfig(max_pool_connections=1, region_name=aws_region)

    # Note: tried to use proxies for the aiobotocore.endpoint, to replace
    #      'https://batch.us-west-2.amazonaws.com/v1/describejobqueues', but
    #      the moto.server does not behave as a proxy server.  Leaving this
    #      here for the record to avoid trying to do it again sometime later.
    # proxies = {
    #     'http': os.getenv("HTTP_PROXY", "http://127.0.0.1:5000/moto-api/"),
    #     'https': os.getenv("HTTPS_PROXY", "http://127.0.0.1:5000/moto-api/"),
    # }
    # assert aioconfig.proxies is None
    # aioconfig.proxies = proxies

    session.set_default_client_config(aioconfig)
    assert session.get_default_client_config() == aioconfig

    session.set_credentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    session.set_debug_logger(logger_name="aiomoto")

    return session


@pytest.fixture
async def aio_aws_client(aio_aws_session):
    async def _get_client(service_name):
        async with MotoService(service_name) as srv:
            async with aio_aws_session.create_client(
                service_name, endpoint_url=srv.endpoint_url
            ) as client:
                yield client

    return _get_client


@pytest.fixture
async def aio_aws_batch_client(aio_aws_session, aio_aws_batch_server):
    async with aio_aws_session.create_client(
        "batch", endpoint_url=aio_aws_batch_server
    ) as client:
        yield client


@pytest.fixture
async def aio_aws_ec2_client(aio_aws_session, aio_aws_ec2_server):
    async with aio_aws_session.create_client("ec2", endpoint_url=aio_aws_ec2_server) as client:
        yield client


@pytest.fixture
async def aio_aws_ecs_client(aio_aws_session, aio_aws_ecs_server):
    async with aio_aws_session.create_client("ecs", endpoint_url=aio_aws_ecs_server) as client:
        yield client


@pytest.fixture
async def aio_aws_iam_client(aio_aws_session, aio_aws_iam_server):
    async with aio_aws_session.create_client("iam", endpoint_url=aio_aws_iam_server) as client:
        client.meta.config.region_name = "aws-global"  # not AWS_REGION
        yield client


@pytest.fixture
async def aio_aws_logs_client(aio_aws_session, aio_aws_logs_server):
    async with aio_aws_session.create_client(
        "logs", endpoint_url=aio_aws_logs_server
    ) as client:
        yield client


@pytest.fixture
async def aio_aws_s3_client(aio_aws_session, aio_aws_s3_server, mocker):
    async with aio_aws_session.create_client("s3", endpoint_url=aio_aws_s3_server) as client:
        # TODO: find a way to apply this method mock only for creating "s3" clients;
        # # ensure the session returns the mock s3 client
        # mocker.patch.object(
        #     aiobotocore.session.AioClientCreator, "create_client",
        #     return_value=client
        # )
        yield client


#
# Batch Infrastructure
#


class AioAwsBatchClients(NamedTuple):
    batch: "aiobotocore.client.Batch"
    ec2: "aiobotocore.client.EC2"
    ecs: "aiobotocore.client.ECS"
    iam: "aiobotocore.client.IAM"
    logs: "aiobotocore.client.CloudWatchLogs"
    region: str


@pytest.fixture
async def aio_aws_batch_clients(
    aio_aws_batch_client,
    aio_aws_ec2_client,
    aio_aws_ecs_client,
    aio_aws_iam_client,
    aio_aws_logs_client,
    aws_region,
):
    return AioAwsBatchClients(
        batch=aio_aws_batch_client,
        ec2=aio_aws_ec2_client,
        ecs=aio_aws_ecs_client,
        iam=aio_aws_iam_client,
        logs=aio_aws_logs_client,
        region=aws_region,
    )


class AioAwsBatchInfrastructure:
    aio_aws_clients: AioAwsBatchClients
    aws_region: str = AWS_REGION
    vpc_id: Optional[str] = None
    subnet_id: Optional[str] = None
    security_group_id: Optional[str] = None
    iam_arn: Optional[str] = None
    compute_env_name: Optional[str] = None
    compute_env_arn: Optional[str] = None
    job_queue_name: Optional[str] = None
    job_queue_arn: Optional[str] = None
    job_definition_name: Optional[str] = None
    job_definition_arn: Optional[str] = None


async def aio_batch_infrastructure(
    aio_aws_batch_clients: AioAwsBatchClients,
    aws_region: str,
    compute_env_name: str,
    job_queue_name: str,
    job_definition_name: str,
) -> AioAwsBatchInfrastructure:
    """
    Create AWS Batch infrastructure, including:
    - VPC with subnet
    - Security group and IAM role
    - Batch compute environment and job queue
    - Batch job job_definition

    This function is not a fixture so that tests can pass the AWS clients to it and then
    continue to use the infrastructure created by it while the client fixtures are in-tact for
    the duration of a test.
    """

    infrastructure = AioAwsBatchInfrastructure()
    infrastructure.aws_region = aws_region
    infrastructure.aio_aws_clients = aio_aws_batch_clients

    resp = await aio_aws_batch_clients.ec2.create_vpc(CidrBlock="172.30.0.0/24")
    vpc_id = resp["Vpc"]["VpcId"]

    resp = await aio_aws_batch_clients.ec2.create_subnet(
        AvailabilityZone=f"{aws_region}a", CidrBlock="172.30.0.0/25", VpcId=vpc_id
    )
    subnet_id = resp["Subnet"]["SubnetId"]

    resp = await aio_aws_batch_clients.ec2.create_security_group(
        Description="moto_test_sg_desc", GroupName="moto_test_sg", VpcId=vpc_id
    )
    sg_id = resp["GroupId"]

    resp = await aio_aws_batch_clients.iam.create_role(
        RoleName="MotoTestRole", AssumeRolePolicyDocument="moto_test_policy"
    )
    iam_arn = resp["Role"]["Arn"]

    resp = await aio_aws_batch_clients.batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="UNMANAGED",
        state="ENABLED",
        serviceRole=iam_arn,
    )
    compute_env_arn = resp["computeEnvironmentArn"]

    resp = await aio_aws_batch_clients.batch.create_job_queue(
        jobQueueName=job_queue_name,
        state="ENABLED",
        priority=123,
        computeEnvironmentOrder=[{"order": 123, "computeEnvironment": compute_env_arn}],
    )
    assert resp["jobQueueName"] == job_queue_name
    assert resp["jobQueueArn"]
    job_queue_arn = resp["jobQueueArn"]

    resp = await aio_aws_batch_clients.batch.register_job_definition(
        jobDefinitionName=job_definition_name,
        type="container",
        containerProperties={
            "image": "busybox",
            "vcpus": 2,
            "memory": 8,
            "command": ["sleep", "10"],  # NOTE: job runs for 10 sec without overrides
        },
    )
    assert resp["jobDefinitionName"] == job_definition_name
    assert resp["jobDefinitionArn"]
    job_definition_arn = resp["jobDefinitionArn"]
    assert resp["revision"]
    assert resp["jobDefinitionArn"].endswith(
        "{0}:{1}".format(resp["jobDefinitionName"], resp["revision"])
    )

    infrastructure.vpc_id = vpc_id
    infrastructure.subnet_id = subnet_id
    infrastructure.security_group_id = sg_id
    infrastructure.iam_arn = iam_arn
    infrastructure.compute_env_name = compute_env_name
    infrastructure.compute_env_arn = compute_env_arn
    infrastructure.job_queue_name = job_queue_name
    infrastructure.job_queue_arn = job_queue_arn
    infrastructure.job_definition_name = job_definition_name
    infrastructure.job_definition_arn = job_definition_arn
    return infrastructure
