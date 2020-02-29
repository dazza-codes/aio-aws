
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
applied to my notes project.

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""
import os
from typing import NamedTuple
from typing import Optional

import boto3
import botocore.waiter
import pytest
from moto import mock_batch
from moto import mock_ec2
from moto import mock_ecs
from moto import mock_iam
from moto import mock_logs
from moto import mock_s3

from tests.aio_aws.utils import AWS_ACCESS_KEY_ID
from tests.aio_aws.utils import AWS_REGION
from tests.aio_aws.utils import AWS_SECRET_ACCESS_KEY

# import moto.settings
# moto.settings.TEST_SERVER_MODE = True

AWS_HOST = "127.0.0.1"
AWS_PORT = "5000"


@pytest.fixture
def aws_host():
    return os.getenv("AWS_HOST", AWS_HOST)


@pytest.fixture
def aws_port():
    return os.getenv("AWS_PORT", AWS_PORT)


@pytest.fixture
def aws_proxy(aws_host, aws_port, monkeypatch):
    # only required if using a moto stand-alone server or similar local stack
    monkeypatch.setenv("HTTP_PROXY", f"http://{aws_host}:{aws_port}")
    monkeypatch.setenv("HTTPS_PROXY", f"http://{aws_host}:{aws_port}")


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "test")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test")


@pytest.fixture
def aws_region():
    return AWS_REGION


@pytest.fixture(scope="session")
def job_queue_name():
    return "moto_test_job_queue"


@pytest.fixture(scope="session")
def job_definition_name():
    return "moto_test_job_definition"


@pytest.fixture(scope="session")
def compute_env_name():
    return "moto_test_compute_env"


#
# AWS Clients
#


@pytest.fixture
def aws_batch_client(aws_region):
    with mock_batch():
        yield boto3.client("batch", region_name=aws_region)


@pytest.fixture
def aws_ec2_client(aws_region):
    with mock_ec2():
        yield boto3.client("ec2", region_name=aws_region)


@pytest.fixture
def aws_ecs_client(aws_region):
    with mock_ecs():
        yield boto3.client("ecs", region_name=aws_region)


@pytest.fixture
def aws_iam_client(aws_region):
    with mock_iam():
        yield boto3.client("iam", region_name=aws_region)


@pytest.fixture
def aws_logs_client(aws_region):
    with mock_logs():
        yield boto3.client("logs", region_name=aws_region)


@pytest.fixture
def aws_s3_client(aws_region):
    with mock_s3():
        yield boto3.client("s3", region_name=aws_region)


class AwsBatchClients(NamedTuple):
    batch: "botocore.client.Batch"
    ec2: "botocore.client.EC2"
    ecs: "botocore.client.ECS"
    iam: "botocore.client.IAM"
    logs: "botocore.client.CloudWatchLogs"
    region: str


@pytest.fixture
def aws_batch_clients(
    aws_batch_client,
    aws_ec2_client,
    aws_ecs_client,
    aws_iam_client,
    aws_logs_client,
    aws_region,
):
    return AwsBatchClients(
        batch=aws_batch_client,
        ec2=aws_ec2_client,
        ecs=aws_ecs_client,
        iam=aws_iam_client,
        logs=aws_logs_client,
        region=aws_region,
    )


#
# Batch Infrastructure
#


class AwsBatchInfrastructure:
    aws_region: str
    aws_clients: AwsBatchClients
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


def batch_infrastructure(
    aws_clients: AwsBatchClients,
    compute_env_name: str,
    job_queue_name: str,
    job_definition_name: str,
) -> AwsBatchInfrastructure:
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

    infrastructure = AwsBatchInfrastructure()
    infrastructure.aws_region = aws_clients.region
    infrastructure.aws_clients = aws_clients

    resp = aws_clients.ec2.create_vpc(CidrBlock="172.30.0.0/24")
    vpc_id = resp["Vpc"]["VpcId"]

    resp = aws_clients.ec2.create_subnet(
        AvailabilityZone=f"{aws_clients.region}a", CidrBlock="172.30.0.0/25", VpcId=vpc_id
    )
    subnet_id = resp["Subnet"]["SubnetId"]

    resp = aws_clients.ec2.create_security_group(
        Description="moto_test_sg_desc", GroupName="moto_test_sg", VpcId=vpc_id
    )
    sg_id = resp["GroupId"]

    resp = aws_clients.iam.create_role(
        RoleName="MotoTestRole", AssumeRolePolicyDocument="moto_test_policy"
    )
    iam_arn = resp["Role"]["Arn"]

    resp = aws_clients.batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="UNMANAGED",
        state="ENABLED",
        serviceRole=iam_arn,
    )
    compute_env_arn = resp["computeEnvironmentArn"]

    resp = aws_clients.batch.create_job_queue(
        jobQueueName=job_queue_name,
        state="ENABLED",
        priority=123,
        computeEnvironmentOrder=[{"order": 123, "computeEnvironment": compute_env_arn}],
    )
    assert resp["jobQueueName"] == job_queue_name
    assert resp["jobQueueArn"]
    job_queue_arn = resp["jobQueueArn"]

    resp = aws_clients.batch.register_job_definition(
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
