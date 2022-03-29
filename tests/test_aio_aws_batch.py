# Copyright 2019-2022 Darren Weber
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
import asyncio
import inspect
import time
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import botocore.exceptions
import pytest
from pytest_aiomoto.aiomoto_fixtures import AioAwsBatchClients
from pytest_aiomoto.aiomoto_fixtures import AioAwsBatchInfrastructure
from pytest_aiomoto.aiomoto_fixtures import aio_batch_infrastructure

from aio_aws import aio_aws_batch
from aio_aws.aio_aws_batch import AWSBatchConfig
from aio_aws.aio_aws_batch import RetryError
from aio_aws.aio_aws_batch import aio_batch_cancel_jobs
from aio_aws.aio_aws_batch import aio_batch_describe_jobs
from aio_aws.aio_aws_batch import aio_batch_get_logs
from aio_aws.aio_aws_batch import aio_batch_job_cancel
from aio_aws.aio_aws_batch import aio_batch_job_logs
from aio_aws.aio_aws_batch import aio_batch_job_manager
from aio_aws.aio_aws_batch import aio_batch_job_submit
from aio_aws.aio_aws_batch import aio_batch_job_terminate
from aio_aws.aio_aws_batch import aio_batch_job_waiter
from aio_aws.aio_aws_batch import aio_batch_run_jobs
from aio_aws.aio_aws_batch import aio_batch_submit_jobs
from aio_aws.aio_aws_batch import aio_batch_terminate_jobs
from aio_aws.aio_aws_batch import aio_batch_update_jobs
from aio_aws.aio_aws_batch import batch_cancel_jobs
from aio_aws.aio_aws_batch import batch_get_logs
from aio_aws.aio_aws_batch import batch_monitor_jobs
from aio_aws.aio_aws_batch import batch_submit_jobs
from aio_aws.aio_aws_batch import batch_terminate_jobs
from aio_aws.aio_aws_batch import batch_update_jobs
from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.aws_batch_models import AWSBatchJobStates
from aio_aws.utils import datetime_to_unix_milliseconds
from aio_aws.utils import response_success
from aio_aws.utils import utc_now


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
        aio_aws_batch_clients,
        aws_region,
        compute_env_name,
        job_queue_name,
        job_definition_name,
    )
    return aws_resources


@pytest.fixture
def batch_config(
    aio_aws_session, aio_aws_batch_server, aio_aws_logs_server, test_aio_jobs_db
) -> AWSBatchConfig:
    class TestBatchConfig(AWSBatchConfig):
        session = aio_aws_session

        @asynccontextmanager
        async def create_batch_client(self):
            async with aio_aws_session.create_client(
                "batch", endpoint_url=aio_aws_batch_server
            ) as client:
                yield client

        @asynccontextmanager
        async def create_logs_client(self):
            async with aio_aws_session.create_client(
                "logs", endpoint_url=aio_aws_logs_server
            ) as client:
                yield client

    config = TestBatchConfig(
        aio_batch_db=test_aio_jobs_db,
        start_pause=0.2,
        min_pause=0.2,
        max_pause=0.6,
        min_jitter=0.1,
        max_jitter=0.2,
    )

    # mocker.patch.object(config, "create_batch_client", return_value=create_batch_client)
    # mocker.patch.object(config, "create_logs_client", return_value=create_logs_client)
    yield config


@pytest.fixture
def aws_batch_sleep1_job(aio_aws_batch_infrastructure: AioAwsBatchInfrastructure):
    return AWSBatchJob(
        job_name="sleep-1-job",
        job_definition=aio_aws_batch_infrastructure.job_definition_arn,
        job_queue=aio_aws_batch_infrastructure.job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && sleep 1 && echo Bye"],
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
async def test_aio_batch_job_queues(
    aio_aws_batch_infrastructure: AioAwsBatchInfrastructure,
):
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
async def test_aio_batch_list_jobs(
    aio_aws_batch_infrastructure: AioAwsBatchInfrastructure,
):
    clients = aio_aws_batch_infrastructure.aio_aws_clients
    job_queue_name = aio_aws_batch_infrastructure.job_queue_name

    for job_status in AWSBatchJob.STATES:
        response = await clients.batch.list_jobs(
            jobQueue=job_queue_name, jobStatus=job_status
        )
        assert response_success(response)
        assert response["jobSummaryList"] == []


@pytest.mark.asyncio
async def test_async_batch_job_submit(aws_batch_sleep1_job, batch_config):
    # moto/docker job submission timestamps seem to be too slow (why ?)
    utc_dt = utc_now()
    utc_ts = datetime_to_unix_milliseconds(utc_dt)
    time.sleep(1.0)
    job = aws_batch_sleep1_job
    await aio_batch_job_submit(job, config=batch_config)
    response = job.job_submission
    assert response_success(response)
    assert response.get("jobId")
    # The job-submission modifies the job object, it's authorized for side-effects
    assert job.job_id
    assert job.job_id == response.get("jobId")
    assert job.job_id in job.job_tries
    assert job.num_tries == 1
    assert job.num_tries <= job.max_tries
    assert job.submitted > utc_ts
    assert job.submitted_datetime > utc_dt
    assert job.status in AWSBatchJob.STATES
    assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED
    assert job.job_description is None


@pytest.mark.asyncio
async def test_async_batch_job_submit_retry(
    aws_batch_sleep1_job,
    aio_aws_session,
    aio_aws_batch_server,
    aio_aws_logs_server,
    aio_aws_batch_client,
    batch_config,
    test_aio_jobs_db,
    mocker,
):
    job = aws_batch_sleep1_job

    error_response = {"Error": {"Code": "TooManyRequestsException"}}

    exception = botocore.exceptions.ClientError(
        error_response=error_response,
        operation_name="submit_job",
    )

    # monkey patch MagicMock
    async def async_magic():
        raise exception

    MagicMock.__await__ = lambda x: async_magic().__await__()

    class MockBatchConfig(AWSBatchConfig):
        session = aio_aws_session

        @asynccontextmanager
        async def create_batch_client(self):
            async with aio_aws_session.create_client(
                "batch", endpoint_url=aio_aws_batch_server
            ) as client:
                mock_client = mocker.patch.object(client, "submit_job")
                mock_client.__await__ = lambda x: async_magic().__await__()
                yield mock_client

        @asynccontextmanager
        async def create_logs_client(self):
            async with aio_aws_session.create_client(
                "logs", endpoint_url=aio_aws_logs_server
            ) as client:
                yield client

    mock_config = MockBatchConfig(
        aio_batch_db=test_aio_jobs_db,
        start_pause=0.2,
        min_pause=0.2,
        max_pause=0.6,
        min_jitter=0.1,
        max_jitter=0.2,
    )

    with pytest.raises(RetryError) as err:
        await aio_batch_job_submit(job, config=mock_config)

    assert job.job_id is None
    assert job.num_tries == 0
    assert job.job_submission == error_response


def test_batch_jobs_utils(aws_batch_sleep1_job, batch_config, mocker):
    # Test convenient synchronous functions that wrap async functions
    utc_dt = utc_now()
    utc_ts = datetime_to_unix_milliseconds(utc_dt)
    time.sleep(1.0)

    job1 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job2 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    jobs = [job1, job2]

    mock_config = mocker.patch("aio_aws.aio_aws_batch.AWSBatchConfig")
    mock_config.return_value = batch_config

    batch_submit_jobs(jobs=jobs)
    for job in jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED
        assert job.submitted > utc_ts
        assert job.submitted_datetime > utc_dt

    batch_update_jobs(jobs=jobs)
    for job in jobs:
        assert AWSBatchJobStates[job.status] >= AWSBatchJobStates.SUBMITTED

    batch_monitor_jobs(jobs=jobs)
    for job in jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUCCEEDED
        # TODO: add these tests when moto supports millisecond timestamps
        #       - https://github.com/spulec/moto/issues/4364
        # assert job.started > job.submitted > utc_ts
        # assert job.started_datetime > job.submitted_datetime > utc_dt
        # assert job.created
        # assert job.created_datetime
        assert job.started
        assert job.started_datetime
        assert job.stopped
        assert job.stopped_datetime
        assert job.stopped > job.started
        assert job.stopped_datetime > job.started_datetime

    batch_get_logs(jobs=jobs)
    for job in jobs:
        assert job.logs


def test_batch_jobs_cancel(
    aws_batch_sleep1_job, aws_batch_sleep5_job, batch_config, mocker
):
    # Test convenient synchronous functions that wrap async functions

    mock_config = mocker.patch("aio_aws.aio_aws_batch.AWSBatchConfig")
    mock_config.return_value = batch_config
    batch_config.start_pause = 1.0
    batch_config.min_pause = 0.4
    batch_config.max_pause = 0.8

    pre_job = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    pre_job.job_name = "pre-job"
    batch_submit_jobs(jobs=[pre_job])
    assert AWSBatchJobStates[pre_job.status] == AWSBatchJobStates.SUBMITTED

    depends_on = [{"jobId": pre_job.job_id, "type": "SEQUENTIAL"}]

    job1 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job1.job_name = "cancel-job-1"
    job1.depends_on = depends_on
    job2 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job2.job_name = "cancel-job-2"
    job2.depends_on = depends_on
    cancel_jobs = [job1, job2]

    batch_submit_jobs(jobs=cancel_jobs)
    for job in cancel_jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED

    batch_cancel_jobs(jobs=cancel_jobs)
    for job in cancel_jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.FAILED


def test_batch_jobs_terminate(aws_batch_sleep5_job, batch_config, mocker):
    # Test convenient synchronous functions that wrap async functions
    job1 = AWSBatchJob(**aws_batch_sleep5_job.db_data)
    job2 = AWSBatchJob(**aws_batch_sleep5_job.db_data)
    jobs = [job1, job2]

    mock_config = mocker.patch("aio_aws.aio_aws_batch.AWSBatchConfig")
    mock_config.return_value = batch_config

    batch_submit_jobs(jobs=jobs)
    for job in jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED

    batch_terminate_jobs(jobs=jobs)
    for job in jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.FAILED


@pytest.mark.asyncio
async def test_async_batch_describe_jobs(aws_batch_sleep1_job, batch_config):
    job1 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job2 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    jobs = [job1, job2]
    await aio_batch_submit_jobs(jobs, config=batch_config)
    job_ids = [j.job_id for j in jobs]
    assert all(job_ids)

    response = await aio_batch_describe_jobs(job_ids=job_ids, config=batch_config)
    assert response_success(response)
    job_descriptions = response.get("jobs")
    assert len(job_descriptions) == 2
    for job_desc in job_descriptions:
        assert job_desc["jobQueue"] == job1.job_queue
        assert job_desc["jobDefinition"] == job1.job_definition
        assert job_desc["status"] in AWSBatchJob.STATES


@pytest.mark.asyncio
async def test_async_batch_update_jobs(aws_batch_sleep1_job, batch_config):
    job1 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job2 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    jobs = [job1, job2]
    await aio_batch_submit_jobs(jobs, config=batch_config)
    job_ids = [j.job_id for j in jobs]
    assert all(job_ids)

    await asyncio.sleep(3.0)
    await aio_batch_update_jobs(jobs=jobs, config=batch_config)
    # Since update jobs can get various status conditions, depending
    # on how advanced a job is in a lifecycle, this just tests that
    # the job status is set
    for job in jobs:
        assert job.status in AWSBatchJob.STATES
        assert AWSBatchJobStates[job.status] in AWSBatchJobStates
        assert AWSBatchJobStates[job.status] > AWSBatchJobStates.SUBMITTED


@pytest.mark.asyncio
async def test_async_batch_cancel_jobs(aws_batch_sleep1_job, batch_config):

    batch_config.start_pause = 1.0
    batch_config.min_pause = 0.4
    batch_config.max_pause = 0.8

    pre_job = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    pre_job.job_name = "pre-job"
    await aio_batch_submit_jobs(jobs=[pre_job], config=batch_config)
    assert AWSBatchJobStates[pre_job.status] == AWSBatchJobStates.SUBMITTED

    depends_on = [{"jobId": pre_job.job_id, "type": "SEQUENTIAL"}]

    job1 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job1.job_name = "cancel-job-1"
    job1.depends_on = depends_on
    job2 = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job2.job_name = "cancel-job-2"
    job2.depends_on = depends_on
    cancel_jobs = [job1, job2]

    await aio_batch_submit_jobs(jobs=cancel_jobs, config=batch_config)
    for job in cancel_jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED

    await aio_batch_cancel_jobs(jobs=cancel_jobs, config=batch_config)
    for job in cancel_jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.FAILED


@pytest.mark.asyncio
async def test_async_batch_terminate_jobs(aws_batch_sleep5_job, batch_config):
    job1 = AWSBatchJob(**aws_batch_sleep5_job.db_data)
    job2 = AWSBatchJob(**aws_batch_sleep5_job.db_data)
    jobs = [job1, job2]
    await aio_batch_submit_jobs(jobs, config=batch_config)
    for job in jobs:
        assert job.job_id
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED

    await aio_batch_terminate_jobs(jobs=jobs, config=batch_config)
    # The terminate jobs function should wait for it to fail and set status FAILED
    for job in jobs:
        assert AWSBatchJobStates[job.status] == AWSBatchJobStates.FAILED


@pytest.mark.asyncio
async def test_async_batch_job_failed(aws_batch_fail_job, batch_config):
    job = aws_batch_fail_job
    await aio_batch_job_submit(job, config=batch_config)
    assert job.job_id

    await aio_batch_job_waiter(job=job, config=batch_config)
    # The job-waiter modifies the job object, it's authorized for side-effects
    assert job.job_description
    assert job.status in AWSBatchJob.STATES
    assert job.status == "FAILED"


@pytest.mark.asyncio
async def test_async_batch_job_waiter(aws_batch_sleep1_job, batch_config):
    job = aws_batch_sleep1_job
    await aio_batch_job_submit(job, config=batch_config)
    assert job.job_id

    await aio_batch_job_waiter(job=job, config=batch_config)
    # The job-waiter modifies the job object, it's authorized for side-effects
    assert job.job_description
    assert job.status in AWSBatchJob.STATES
    assert job.status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_async_batch_job_manager(aws_batch_sleep1_job, batch_config):
    job = aws_batch_sleep1_job
    await aio_batch_job_manager(job, config=batch_config)
    # The job-manager modifies the job object, it's authorized for side-effects
    assert job.job_id
    assert job.job_description
    assert job.status in AWSBatchJob.STATES
    assert job.status == "SUCCEEDED"
    assert job.logs is None  # leave logs retrieval to aio_batch_get_logs
    # confirm that job data is persisted, but it can't use `find_latest_job_name`
    # due to a bug in moto, see https://github.com/spulec/moto/issues/2829
    jobs_db = batch_config.aio_batch_db
    job_docs = await jobs_db.find_by_job_name(job.job_name)
    assert len(job_docs) == 1
    assert job_docs[0] == job.db_data


@pytest.mark.asyncio
async def test_async_batch_job_cancel(aws_batch_sleep1_job, batch_config):

    batch_config.start_pause = 1.0
    batch_config.min_pause = 0.4
    batch_config.max_pause = 0.8

    pre_job = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    pre_job.job_name = "pre-job"
    await aio_batch_submit_jobs(jobs=[pre_job], config=batch_config)
    assert AWSBatchJobStates[pre_job.status] == AWSBatchJobStates.SUBMITTED

    depends_on = [{"jobId": pre_job.job_id, "type": "SEQUENTIAL"}]

    job = AWSBatchJob(**aws_batch_sleep1_job.db_data)
    job.job_name = "cancel-job-1"
    job.depends_on = depends_on

    await aio_batch_job_submit(job, config=batch_config)
    assert job.job_id
    assert AWSBatchJobStates[job.status] == AWSBatchJobStates.SUBMITTED

    reason = "test-job-cancel"  # not a SPOT failure
    await aio_batch_job_cancel(job=job, reason=reason, config=batch_config)
    # It waits for the job to cancel and updates the job.
    assert job.job_description["statusReason"] == reason
    assert job.status in AWSBatchJob.STATES
    assert AWSBatchJobStates[job.status] == AWSBatchJobStates.FAILED


@pytest.mark.asyncio
async def test_async_batch_job_terminate(aws_batch_sleep5_job, batch_config):
    job = aws_batch_sleep5_job
    await aio_batch_job_submit(job, config=batch_config)
    assert job.job_id

    reason = "test-job-termination"  # not a SPOT failure
    await aio_batch_job_terminate(job=job, reason=reason, config=batch_config)
    # It waits for the job to terminate and updates the job.
    assert job.job_description["statusReason"] == reason
    assert job.status in AWSBatchJob.STATES
    assert job.status == "FAILED"


@pytest.mark.asyncio
async def test_async_batch_job_spot_retry(aws_batch_sleep1_job, batch_config):
    job = aws_batch_sleep1_job
    await aio_batch_job_submit(job, config=batch_config)
    assert job.job_id

    reason = "Host EC2 instance-id terminated"  # a SPOT failure
    await aio_batch_job_terminate(job=job, reason=reason, config=batch_config)
    assert job.status == "FAILED"

    await aio_batch_job_manager(job, config=batch_config)
    assert job.status in AWSBatchJob.STATES
    assert job.status == "SUCCEEDED"  # job manager should retry the job


@pytest.mark.asyncio
async def test_async_batch_job_db(aws_batch_sleep1_job, batch_config):
    job = aws_batch_sleep1_job
    await aio_batch_job_manager(job, config=batch_config)
    assert job.status in AWSBatchJob.STATES
    assert job.status == "SUCCEEDED"

    jobs_db = batch_config.aio_batch_db
    job_data = await jobs_db.find_by_job_id(job.job_id)
    assert job_data
    assert job_data["status"] == job.status
    # use the data to re-construct an AWSBatchJob
    batch_job = AWSBatchJob(**job_data)
    assert batch_job.db_data == job_data
    assert batch_job.db_data == job.db_data


@pytest.mark.asyncio
async def test_async_batch_job_logs(aws_batch_sleep1_job, batch_config):
    job = aws_batch_sleep1_job
    await aio_batch_job_manager(job, config=batch_config)
    assert job.status == "SUCCEEDED"
    assert job.logs is None

    log_events = await aio_batch_job_logs(job, config=batch_config)
    assert log_events
    assert isinstance(log_events, list)
    assert isinstance(log_events[0], dict)
    assert job.logs
    assert isinstance(job.logs, list)
    assert isinstance(job.logs[0], dict)


@pytest.mark.asyncio
async def test_async_batch_run_jobs(aws_batch_sleep1_job, batch_config, event_loop):
    job = aws_batch_sleep1_job
    batch_jobs = [job]

    await aio_batch_run_jobs(jobs=batch_jobs, config=batch_config)

    jobs_db = batch_config.aio_batch_db
    assert job.job_id
    job_data = await jobs_db.find_by_job_id(job.job_id)
    assert job_data
    assert job_data["job_id"] == job.job_id
    assert job_data["status"] == job.status

    assert job.logs is None


@pytest.mark.asyncio
async def test_async_batch_get_logs(aws_batch_sleep1_job, batch_config, event_loop):
    job = aws_batch_sleep1_job
    batch_jobs = [job]

    await aio_batch_run_jobs(jobs=batch_jobs, config=batch_config)
    await asyncio.sleep(4)  # wait for logs to propagate to cloud watch service
    await aio_batch_get_logs(jobs=batch_jobs, config=batch_config)

    jobs_db = batch_config.aio_batch_db
    assert job.job_id
    job_data = await jobs_db.find_by_job_id(job.job_id)
    assert job_data["job_id"] == job.job_id
    assert job_data["status"] == job.status

    assert job.logs
    logs_data = await jobs_db.find_job_logs(job.job_id)
    assert logs_data
    assert logs_data["job_id"] == job.job_id
    assert logs_data["status"] == job.status
    assert logs_data["logs"] == job.logs
