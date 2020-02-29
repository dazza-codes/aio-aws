
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
Test AWS Fixtures

This test suite checks fixtures for moto clients.

"""

import os

from botocore.client import BaseClient

from notes.aio_aws.aio_aws import response_success
from tests.aio_aws.aws_fixtures import AwsBatchClients
from tests.aio_aws.utils import AWS_ACCESS_KEY_ID
from tests.aio_aws.utils import AWS_REGION
from tests.aio_aws.utils import AWS_SECRET_ACCESS_KEY
from tests.aio_aws.utils import has_moto_mocks


def test_aws_credentials(aws_credentials):
    assert os.getenv("AWS_ACCESS_KEY_ID")
    assert os.getenv("AWS_SECRET_ACCESS_KEY")
    assert os.getenv("AWS_ACCESS_KEY_ID") == AWS_ACCESS_KEY_ID
    assert os.getenv("AWS_SECRET_ACCESS_KEY") == AWS_SECRET_ACCESS_KEY


def test_aws_clients(aws_batch_clients):
    assert isinstance(aws_batch_clients, AwsBatchClients)
    assert isinstance(aws_batch_clients.batch, BaseClient)
    assert isinstance(aws_batch_clients.ec2, BaseClient)
    assert isinstance(aws_batch_clients.ecs, BaseClient)
    assert isinstance(aws_batch_clients.iam, BaseClient)
    assert isinstance(aws_batch_clients.logs, BaseClient)
    assert isinstance(aws_batch_clients.region, str)
    assert aws_batch_clients.region == AWS_REGION


def test_aws_batch_client(aws_batch_client):
    client = aws_batch_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = client.describe_job_queues()
    assert response_success(resp)
    assert resp.get("jobQueues") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.batch.DescribeJobQueues")


def test_aws_ec2_client(aws_ec2_client):
    client = aws_ec2_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = client.describe_instances()
    assert response_success(resp)
    assert resp.get("Reservations") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.ec2.DescribeInstances")


def test_aws_ecs_client(aws_ecs_client):
    client = aws_ecs_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = client.list_task_definitions()
    assert response_success(resp)
    assert resp.get("taskDefinitionArns") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.ecs.ListTaskDefinitions")


def test_aws_iam_client(aws_iam_client):
    client = aws_iam_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == "aws-global"  # not AWS_REGION
    assert client.meta.region_name == "aws-global"  # not AWS_REGION

    resp = client.list_roles()
    assert response_success(resp)
    assert resp.get("Roles") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.iam.ListRoles")


def test_aws_logs_client(aws_logs_client):
    client = aws_logs_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = client.describe_log_groups()
    assert response_success(resp)
    assert resp.get("logGroups") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.cloudwatch-logs.DescribeLogGroups")


def test_aws_s3_client(aws_s3_client):
    client = aws_s3_client
    assert isinstance(client, BaseClient)
    assert client.meta.config.region_name == AWS_REGION
    assert client.meta.region_name == AWS_REGION

    resp = client.list_buckets()
    assert response_success(resp)
    assert resp.get("Buckets") == []

    # the event-name mocks are dynamically generated after calling the method
    assert has_moto_mocks(client, "before-send.s3.ListBuckets")
