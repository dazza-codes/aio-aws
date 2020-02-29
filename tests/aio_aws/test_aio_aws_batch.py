
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
according to the moto license. That license container_overrides any global license
applied to my notes project.

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""

import inspect

import pytest
from tinydb import TinyDB

from notes.aio_aws import aio_aws_batch
from notes.aio_aws.aio_aws import response_success
from notes.aio_aws.aio_aws_batch import aio_batch_job_manager
from notes.aio_aws.aio_aws_batch import aio_batch_job_status
from notes.aio_aws.aio_aws_batch import aio_batch_job_submit
from notes.aio_aws.aio_aws_batch import aio_batch_job_terminate
from notes.aio_aws.aio_aws_batch import aio_batch_job_waiter
from notes.aio_aws.aio_aws_batch import AWSBatchDB
from notes.aio_aws.aio_aws_batch import AWSBatchJob
from tests.aio_aws.aiomoto_fixtures import aio_batch_infrastructure
from tests.aio_aws.aiomoto_fixtures import AioAwsBatchClients
from tests.aio_aws.aiomoto_fixtures import AioAwsBatchInfrastructure


def test_async_aws_batch():
    assert inspect.ismodule(aio_aws_batch)


@pytest.fixture
async def aio_aws_batch_infrastructure(
    aio_aws_batch_clients: AioAwsBatchClients,
    compute_env_name: str,
    job_queue_name: str,
    job_definition_name: str,
) -> AioAwsBatchInfrastructure:
    aws_region = aio_aws_batch_clients.region
    aws_resources = await aio_batch_infrastructure(
        aio_aws_batch_clients, aws_region, compute_env_name, job_queue_name, job_definition_name
    )
    return aws_resources


TEST_DB = TinyDB("/tmp/test_batch_jobs_db.json")


@pytest.fixture
def aws_batch_sleep1_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure, monkeypatch):
    TEST_DB.purge()
    monkeypatch.setattr(aio_aws_batch, "AWS_BATCH_DB", TEST_DB)
    monkeypatch.setattr(aio_aws_batch, "BATCH_STARTUP_PAUSE", 0.4)
    monkeypatch.setattr(aio_aws_batch, "MIN_PAUSE", 0.8)
    monkeypatch.setattr(aio_aws_batch, "MAX_PAUSE", 1.0)
    monkeypatch.setattr(aio_aws_batch, "MIN_JITTER", 0.1)
    monkeypatch.setattr(aio_aws_batch, "MAX_JITTER", 0.2)
    return AWSBatchJob(
        job_name="sleep-1-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/bash", "-c", "echo Hello1 && sleep 0.2 && echo Hello2"],
    )


@pytest.fixture
def aws_batch_sleep5_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure, monkeypatch):
    TEST_DB.purge()
    monkeypatch.setattr(aio_aws_batch, "AWS_BATCH_DB", TEST_DB)
    monkeypatch.setattr(aio_aws_batch, "BATCH_STARTUP_PAUSE", 0.4)
    monkeypatch.setattr(aio_aws_batch, "MIN_PAUSE", 0.8)
    monkeypatch.setattr(aio_aws_batch, "MAX_PAUSE", 1.0)
    monkeypatch.setattr(aio_aws_batch, "MIN_JITTER", 0.1)
    monkeypatch.setattr(aio_aws_batch, "MAX_JITTER", 0.2)
    return AWSBatchJob(
        job_name="sleep-5-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/bash", "-c", "echo Hello1 && sleep 5 && echo Hello2"],
    )


@pytest.mark.asyncio
async def test_aws_batch_infrastructure(
    aio_aws_batch_infrastructure: AioAwsBatchInfrastructure,
):
    infrastructure = aio_aws_batch_infrastructure
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


@pytest.mark.asyncio
async def test_aio_batch_job_definitions(
    aio_aws_batch_infrastructure: AioAwsBatchInfrastructure,
):
    aws_resources = aio_aws_batch_infrastructure
    aws_region = aws_resources.aws_region
    job_definition_name = aws_resources.job_definition_name

    assert aws_resources
    assert aws_resources.job_definition_arn
    assert f"arn:aws:batch:{aws_region}" in aws_resources.job_definition_arn
    assert job_definition_name in aws_resources.job_definition_arn

    clients = aio_aws_batch_infrastructure.aio_aws_clients
    response = await clients.batch.describe_job_definitions()
    assert response_success(response)
    job_definitions = response["jobDefinitions"]
    assert len(job_definitions) == 1
    job_definition = job_definitions[0]
    assert job_definition["jobDefinitionArn"] == aws_resources.job_definition_arn
    assert job_definition["jobDefinitionName"] == aws_resources.job_definition_name


@pytest.mark.asyncio
async def test_aio_batch_job_queues(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    aws_resources = aio_aws_batch_infrastructure
    aws_region = aws_resources.aws_region
    job_queue_name = aws_resources.job_queue_name

    assert aws_resources
    assert aws_resources.job_queue_arn
    assert f"arn:aws:batch:{aws_region}" in aws_resources.job_queue_arn
    assert job_queue_name in aws_resources.job_queue_arn

    clients = aio_aws_batch_infrastructure.aio_aws_clients
    response = await clients.batch.describe_job_queues()
    assert response_success(response)
    job_queues = response["jobQueues"]
    assert len(job_queues) == 1
    job_queue = job_queues[0]
    assert job_queue["jobQueueArn"] == aws_resources.job_queue_arn
    assert job_queue["jobQueueName"] == aws_resources.job_queue_name


@pytest.mark.asyncio
async def test_aio_batch_list_jobs(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    clients = aio_aws_batch_infrastructure.aio_aws_clients
    job_queue_name = aio_aws_batch_infrastructure.job_queue_name

    for job_status in AWSBatchJob.STATUSES:
        response = await clients.batch.list_jobs(jobQueue=job_queue_name, jobStatus=job_status)
        assert response_success(response)
        assert response["jobSummaryList"] == []


@pytest.mark.asyncio
async def test_async_batch_job_submit(aws_batch_sleep1_job, aio_aws_batch_client):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client)
    assert response_success(response)
    assert job.job_submission == response
    assert job.job_id
    assert job.job_id == response.get("jobId")
    assert job.job_id in job.job_tries
    assert job.num_tries == 1
    assert job.num_tries <= job.max_tries


@pytest.mark.asyncio
async def test_async_batch_job_status(aws_batch_sleep1_job, aio_aws_batch_client):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client)
    assert response_success(response)

    response = await aio_batch_job_status(jobs=[job.job_id], client=aio_aws_batch_client)
    assert response_success(response)
    jobs = response.get("jobs")
    assert len(jobs) == 1
    job_desc = jobs[0]
    assert job_desc["jobQueue"] == job.job_queue
    assert job_desc["jobDefinition"] == job.job_definition
    assert job_desc["status"] in AWSBatchJob.STATUSES


@pytest.mark.asyncio
async def test_async_batch_job_terminate(aws_batch_sleep5_job, aio_aws_batch_client):
    job = aws_batch_sleep5_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client)
    assert response_success(response)

    reason = "test-job-termination"
    response = await aio_batch_job_terminate(
        job_id=job.job_id, reason=reason, client=aio_aws_batch_client
    )
    assert response_success(response)
    # there are no response details to inspect

    # Waiting for the job to complete is necessary; checking the job status
    # immediately after terminating it can get a status like RUNNABLE or RUNNING.
    job_desc = await aio_batch_job_waiter(job=job, client=aio_aws_batch_client)
    assert job.job_description == job_desc
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "FAILED"
    assert job_desc["statusReason"] == reason


@pytest.mark.asyncio
async def test_async_batch_job_waiter(aws_batch_sleep1_job, aio_aws_batch_client):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client)
    assert response_success(response)

    job_desc = await aio_batch_job_waiter(job=job, client=aio_aws_batch_client)
    assert job.job_description == job_desc
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_async_batch_job_manager(aws_batch_sleep1_job, aio_aws_batch_client):
    job = aws_batch_sleep1_job
    job_desc = await aio_batch_job_manager(job, client=aio_aws_batch_client)
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_async_batch_job_db(aws_batch_sleep1_job, aio_aws_batch_client):
    job = aws_batch_sleep1_job
    job_desc = await aio_batch_job_manager(job, client=aio_aws_batch_client)
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"

    # monkeypatch applies TEST_DB
    job_data = AWSBatchDB.find_by_job_id(job.job_id)
    assert job_data
    assert job_data["status"] == job.status
    # use the data to re-construct an AWSBatchJob
    batch_job = AWSBatchJob(**job_data)
    assert batch_job.db_data == job_data
    assert batch_job.db_data == job.db_data
