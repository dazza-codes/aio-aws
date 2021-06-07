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
Test Asyncio AWS Batch

This test suite uses a large suite of moto mocks for the AWS batch
infrastructure. These infrastructure mocks are derived from the moto test
suite for testing the batch client. The test infrastructure should be used
according to the moto license (Apache-2.0).

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""

import pytest

from aio_aws.utils import response_success
from tests.fixtures.aws_fixtures import AwsBatchClients
from tests.fixtures.aws_fixtures import AwsBatchInfrastructure
from tests.fixtures.aws_fixtures import batch_infrastructure


@pytest.fixture
def aws_batch_infrastructure(
    aws_batch_clients: AwsBatchClients,
    compute_env_name: str,
    job_queue_name: str,
    job_definition_name: str,
) -> AwsBatchInfrastructure:
    aws_region = aws_batch_clients.region
    aws_resources = batch_infrastructure(
        aws_batch_clients, compute_env_name, job_queue_name, job_definition_name
    )
    return aws_resources


def test_aws_batch_infrastructure(
    aws_batch_infrastructure: AwsBatchInfrastructure,
):
    infrastructure = aws_batch_infrastructure
    assert infrastructure
    assert infrastructure.vpc_id
    assert infrastructure.subnet_id
    assert infrastructure.security_group_id
    assert infrastructure.iam_arn
    assert infrastructure.compute_env_name
    assert infrastructure.compute_env_arn
    assert infrastructure.job_queue_name
    assert infrastructure.job_queue_arn
    assert infrastructure.job_definition_name
    assert infrastructure.job_definition_arn


def test_batch_job_definitions(
    aws_batch_infrastructure: AwsBatchInfrastructure,
):
    aws_resources = aws_batch_infrastructure
    aws_region = aws_resources.aws_region
    job_definition_name = aws_resources.job_definition_name

    assert aws_resources
    assert aws_resources.job_definition_arn
    assert f"arn:aws:batch:{aws_region}" in aws_resources.job_definition_arn
    assert job_definition_name in aws_resources.job_definition_arn

    clients = aws_batch_infrastructure.aws_clients
    response = clients.batch.describe_job_definitions()
    assert response_success(response)
    job_definitions = response["jobDefinitions"]
    assert len(job_definitions) == 1
    job_definition = job_definitions[0]
    assert job_definition["jobDefinitionArn"] == aws_resources.job_definition_arn
    assert job_definition["jobDefinitionName"] == aws_resources.job_definition_name


def test_batch_job_queues(aws_batch_infrastructure: AwsBatchInfrastructure):
    aws_resources = aws_batch_infrastructure
    aws_region = aws_resources.aws_region
    job_queue_name = aws_resources.job_queue_name

    assert aws_resources
    assert aws_resources.job_queue_arn
    assert f"arn:aws:batch:{aws_region}" in aws_resources.job_queue_arn
    assert job_queue_name in aws_resources.job_queue_arn

    clients = aws_batch_infrastructure.aws_clients
    response = clients.batch.describe_job_queues()
    assert response_success(response)
    job_queues = response["jobQueues"]
    assert len(job_queues) == 1
    job_queue = job_queues[0]
    assert job_queue["jobQueueArn"] == aws_resources.job_queue_arn
    assert job_queue["jobQueueName"] == aws_resources.job_queue_name


def test_batch_list_jobs(aws_batch_infrastructure: AwsBatchInfrastructure):
    clients = aws_batch_infrastructure.aws_clients
    job_queue_name = aws_batch_infrastructure.job_queue_name

    for job_status in [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]:
        response = clients.batch.list_jobs(
            jobQueue=job_queue_name, jobStatus=job_status
        )
        assert response_success(response)
        assert response["jobSummaryList"] == []
