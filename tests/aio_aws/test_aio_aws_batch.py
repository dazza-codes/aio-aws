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

import inspect

import botocore.exceptions
import pytest

from notes.aio_aws import aio_aws_batch
from notes.aio_aws.aio_aws import response_success
from notes.aio_aws.aio_aws_batch import aio_batch_job_logs
from notes.aio_aws.aio_aws_batch import aio_batch_job_manager
from notes.aio_aws.aio_aws_batch import aio_batch_job_status
from notes.aio_aws.aio_aws_batch import aio_batch_job_submit
from notes.aio_aws.aio_aws_batch import aio_batch_job_terminate
from notes.aio_aws.aio_aws_batch import aio_batch_job_waiter
from notes.aio_aws.aio_aws_batch import aio_batch_run_jobs
from notes.aio_aws.aio_aws_batch import AWSBatchConfig
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


@pytest.fixture
def test_jobs_db() -> AWSBatchDB:
    batch_db = AWSBatchDB(
        jobs_db_file="/tmp/test_batch_jobs_db.json", logs_db_file="/tmp/test_batch_logs_db.json"
    )
    batch_db.jobs_db.purge()
    batch_db.logs_db.purge()
    yield batch_db
    batch_db.jobs_db.purge()
    batch_db.logs_db.purge()


@pytest.fixture
def batch_config(aio_aws_batch_client, aio_aws_logs_client, mocker) -> AWSBatchConfig:
    config = AWSBatchConfig(
        start_pause=0.4, min_pause=0.8, max_pause=1.0, min_jitter=0.1, max_jitter=0.2,
    )
    mocker.patch.object(config, "get_batch_client", return_value=aio_aws_batch_client)
    mocker.patch.object(config, "get_logs_client", return_value=aio_aws_logs_client)
    return config


@pytest.fixture
def aws_batch_sleep1_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    return AWSBatchJob(
        job_name="sleep-1-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"],
    )


@pytest.fixture
def aws_batch_sleep5_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    return AWSBatchJob(
        job_name="sleep-5-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && sleep 5 && echo Bye"],
    )


@pytest.fixture
def aws_batch_fail_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    return AWSBatchJob(
        job_name="fail-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && exit 1"],
    )


# THIS DOES NOT WORK - DON'T TRY IT AGAIN
# @pytest.fixture
# async def aws_batch_sleep1_job_submitted(
#     aws_batch_sleep1_job, aio_aws_batch_client, batch_config
# ) -> AWSBatchJob:
#     job = aws_batch_sleep1_job
#     response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
#     assert response_success(response)
#     assert response.get("jobId")
#     # To avoid side-effects, the job-submit should not modify the job object directly
#     assert job.job_id is None
#     if response_success(response):
#         # this should be whatever the job-manager does, without saving to the jobs-db
#         job.job_id = response["jobId"]
#         job.job_submission = response
#         job.job_tries.append(job.job_id)
#         job.num_tries += 1
#         return job


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
async def test_async_batch_job_submit(aws_batch_sleep1_job, aio_aws_batch_client, batch_config):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    assert job.job_id is None
    # To avoid side-effects, the job data is not modified by the function
    # assert job.job_submission == response
    # assert job.job_id
    # assert job.job_id == response.get("jobId")
    # assert job.job_id in job.job_tries
    # assert job.num_tries == 1
    # assert job.num_tries <= job.max_tries


@pytest.mark.skip("https://github.com/aio-libs/aiobotocore/issues/781")
@pytest.mark.asyncio
async def test_async_batch_job_submit_retry(
    aws_batch_sleep1_job, aio_aws_batch_client, batch_config, mocker
):
    job = aws_batch_sleep1_job

    exception = botocore.exceptions.ClientError(
        error_response={"Error": {"Code": "TooManyRequestsException"}},
        operation_name="submit_job",  # ?
    )
    mock_client = mocker.patch.object(aio_aws_batch_client, "submit_job", side_effect=exception)
    response = await aio_batch_job_submit(job, client=mock_client, config=batch_config)
    assert False


@pytest.mark.asyncio
async def test_async_batch_job_status(aws_batch_sleep1_job, aio_aws_batch_client, batch_config):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    assert job.job_id is None
    job.job_id = response.get("jobId")

    response = await aio_batch_job_status(
        jobs=[job.job_id], client=aio_aws_batch_client, config=batch_config
    )
    assert response_success(response)
    jobs = response.get("jobs")
    assert len(jobs) == 1
    job_desc = jobs[0]
    assert job_desc["jobQueue"] == job.job_queue
    assert job_desc["jobDefinition"] == job.job_definition
    assert job_desc["status"] in AWSBatchJob.STATUSES


@pytest.mark.skip("https://github.com/spulec/moto/issues/2829")
@pytest.mark.asyncio
async def test_async_batch_job_failed(aws_batch_fail_job, aio_aws_batch_client, batch_config):
    job = aws_batch_fail_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    job.job_id = response.get("jobId")

    job_desc = await aio_batch_job_waiter(
        job=job, client=aio_aws_batch_client, config=batch_config
    )
    assert job_desc["status"] == "FAILED"
    # To avoid side-effects, the job data is not modified by the function
    # assert job.job_description == job_desc
    # assert job.status == job_desc["status"]
    # assert job.status in AWSBatchJob.STATUSES
    # assert job.status == "FAILED"


@pytest.mark.asyncio
async def test_async_batch_job_waiter(aws_batch_sleep1_job, aio_aws_batch_client, batch_config):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    assert job.job_id is None
    job.job_id = response.get("jobId")

    job_desc = await aio_batch_job_waiter(
        job=job, client=aio_aws_batch_client, config=batch_config
    )
    assert job_desc["status"] == "SUCCEEDED"
    # To avoid side-effects, the job data is not modified by the function
    # assert job.job_description == job_desc
    # assert job.status == job_desc["status"]
    # assert job.status in AWSBatchJob.STATUSES
    # assert job.status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_async_batch_job_manager(
    aws_batch_sleep1_job, aio_aws_batch_client, aio_aws_logs_client, test_jobs_db, batch_config
):
    job = aws_batch_sleep1_job
    job_desc = await aio_batch_job_manager(
        job,
        test_jobs_db,
        batch_client=aio_aws_batch_client,
        logs_client=aio_aws_logs_client,
        config=batch_config,
    )
    # The job-manager modifies the job object, it's authorized for side-effects
    assert job.job_id
    assert job.job_description == job_desc
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"
    assert job.logs
    assert isinstance(job.logs, list)
    assert isinstance(job.logs[0], dict)
    # confirm that job data is persisted, but it can't use `find_latest_job_name`
    # due to a bug in moto, see https://github.com/spulec/moto/issues/2829
    job_docs = test_jobs_db.find_by_job_name(job.job_name)
    assert len(job_docs) == 1
    assert job_docs[0] == job.db_data


@pytest.mark.asyncio
async def test_async_batch_job_terminate(
    aws_batch_sleep5_job, aio_aws_batch_client, batch_config
):
    job = aws_batch_sleep5_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    assert job.job_id is None
    job.job_id = response.get("jobId")

    reason = "test-job-termination"  # not a SPOT failure
    response = await aio_batch_job_terminate(
        job_id=job.job_id, reason=reason, client=aio_aws_batch_client, config=batch_config
    )
    assert response_success(response)
    # there are no response details to inspect

    # Waiting for the job to complete is necessary; checking the job status
    # immediately after terminating it can get a status like RUNNABLE or RUNNING.
    job_desc = await aio_batch_job_waiter(
        job=job, client=aio_aws_batch_client, config=batch_config
    )
    assert job_desc["status"] == "FAILED"
    assert job_desc["statusReason"] == reason
    # To avoid side-effects, the job data is not modified by the function
    # assert job.job_description == job_desc
    # assert job.status == job_desc["status"]
    # assert job.status in AWSBatchJob.STATUSES
    # assert job.status == "FAILED"


@pytest.mark.asyncio
async def test_async_batch_job_spot_retry(
    aws_batch_sleep1_job, aio_aws_batch_client, aio_aws_logs_client, test_jobs_db, batch_config
):
    job = aws_batch_sleep1_job
    response = await aio_batch_job_submit(job, client=aio_aws_batch_client, config=batch_config)
    assert response_success(response)
    assert response.get("jobId")
    assert job.job_id is None
    job.job_id = response.get("jobId")

    reason = "Host EC2 instance-id terminated"  # a SPOT failure
    response = await aio_batch_job_terminate(
        job_id=job.job_id, reason=reason, client=aio_aws_batch_client, config=batch_config
    )
    assert response_success(response)

    job_desc = await aio_batch_job_manager(
        job,
        jobs_db=test_jobs_db,
        batch_client=aio_aws_batch_client,
        logs_client=aio_aws_logs_client,
        config=batch_config,
    )
    assert job.job_description == job_desc
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"  # job manager should retry the job


@pytest.mark.asyncio
async def test_async_batch_job_db(
    aws_batch_sleep1_job, aio_aws_batch_client, aio_aws_logs_client, test_jobs_db, batch_config
):
    job = aws_batch_sleep1_job
    job_desc = await aio_batch_job_manager(
        job,
        jobs_db=test_jobs_db,
        batch_client=aio_aws_batch_client,
        logs_client=aio_aws_logs_client,
        config=batch_config,
    )
    assert job.status == job_desc["status"]
    assert job.status in AWSBatchJob.STATUSES
    assert job.status == "SUCCEEDED"

    job_data = test_jobs_db.find_by_job_id(job.job_id)
    assert job_data
    assert job_data["status"] == job.status
    # use the data to re-construct an AWSBatchJob
    batch_job = AWSBatchJob(**job_data)
    assert batch_job.db_data == job_data
    assert batch_job.db_data == job.db_data


@pytest.mark.asyncio
async def test_async_batch_job_logs(
    aws_batch_sleep1_job, aio_aws_batch_client, aio_aws_logs_client, test_jobs_db, batch_config
):
    job = aws_batch_sleep1_job
    job_desc = await aio_batch_job_manager(
        job,
        jobs_db=test_jobs_db,
        batch_client=aio_aws_batch_client,
        logs_client=aio_aws_logs_client,
        config=batch_config,
    )
    assert job.status == job_desc["status"]
    assert job.status == "SUCCEEDED"

    log_events = await aio_batch_job_logs(
        job, logs_client=aio_aws_logs_client, config=batch_config
    )
    assert isinstance(log_events, list)
    assert isinstance(log_events[0], dict)


@pytest.mark.skip("TODO: get this to work without event loop conflicts")
@pytest.mark.asyncio
async def test_async_batch_run_jobs(
    aws_batch_sleep1_job, aio_aws_batch_client, aio_aws_logs_client, test_jobs_db, batch_config
):
    job = aws_batch_sleep1_job
    batch_jobs = [job]

    aio_batch_run_jobs(jobs=batch_jobs, jobs_db=test_jobs_db, config=batch_config)

    assert job.job_id
    job_data = test_jobs_db.find_by_job_id(job.job_id)
    assert job_data
    assert job_data["job_id"] == job.job_id
    assert job_data["status"] == job.status

    assert job.logs
    logs_data = test_jobs_db.find_job_logs(job.job_id)
    assert logs_data
    assert logs_data["job_id"] == job.job_id
    assert logs_data["status"] == job.status
    assert logs_data["logs"] == job.logs
