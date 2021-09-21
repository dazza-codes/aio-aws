# pylint: disable=bad-continuation

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
AioAWS Batch
============

In testing this, it's able to run and monitor 100s of jobs from a laptop with modest
CPU/RAM resources.  It can recover from a crash by using a db-state, without re-running
the same jobs (by jobName).  It should be able to scale to run 1000s of jobs.

To run the example, the ``aio_aws.aio_aws_batch`` module has a ``main`` that will run and
manage about 5 live batch jobs (very small ``sleep`` jobs that don't cost much to run).  The
job state is persisted to ``aws_batch_jobs.json`` and if it runs successfully, it will not
run the jobs again; the `TinyDB`_ is used to recover job state by ``jobName``.  (This demo
assumes that some simple AWS Batch infrastructure exists already.)

.. code-block::

    # setup the python virtualenv
    # check the main details and modify for a preferred batch queue/CE and AWS region

    $ ./aio_aws/aio_aws_batch.py

    Test async batch jobs

    # wait a few minutes and watch the status messages
    # it submits and monitors the jobs until they complete
    # job status is saved and updated in `aws_batch_jobs.json`
    # when it's done, run it again and see that nothing is re-submitted

    $ ./aio_aws/aio_aws_batch.py

If the job monitoring is halted for some reason (like ``CNT-C``), it can recover from
the db-state, e.g.

.. code-block::

    $ ./aio_aws/aio_aws_batch.py

    Test async batch jobs
    [INFO]  2020-03-05T14:51:53.372Z  aio-aws:<module>:485  AWS Batch job (test-sleep-job-0000) recovered from db
    [INFO]  2020-03-05T14:51:53.373Z  aio-aws:<module>:485  AWS Batch job (test-sleep-job-0001) recovered from db
    [INFO]  2020-03-05T14:51:53.373Z  aio-aws:<module>:485  AWS Batch job (test-sleep-job-0002) recovered from db
    [INFO]  2020-03-05T14:51:53.374Z  aio-aws:<module>:485  AWS Batch job (test-sleep-job-0003) recovered from db
    [INFO]  2020-03-05T14:51:53.374Z  aio-aws:<module>:485  AWS Batch job (test-sleep-job-0004) recovered from db
    [INFO]  2020-03-05T14:51:53.690Z  aio-aws:aio_batch_job_waiter:375  AWS Batch job (846d54d4-c3c3-4a3b-9101-646d78d3bbfb) status: RUNNABLE
    [INFO]  2020-03-05T14:51:53.692Z  aio-aws:aio_batch_job_waiter:375  AWS Batch job (dfce3461-9eab-4f5b-846c-6f223d593f6f) status: RUNNABLE
    [INFO]  2020-03-05T14:51:53.693Z  aio-aws:aio_batch_job_waiter:375  AWS Batch job (637e6b27-8d4d-4f45-b988-c00775461616) status: RUNNABLE
    [INFO]  2020-03-05T14:51:53.701Z  aio-aws:aio_batch_job_waiter:375  AWS Batch job (d9ac27c9-e7d3-49cd-8f53-c84a9b4c1750) status: RUNNABLE
    [INFO]  2020-03-05T14:51:53.732Z  aio-aws:aio_batch_job_waiter:375  AWS Batch job (7ebfe7c4-44a4-40d6-9eab-3708e334689d) status: RUNNABLE

For the demo to run quickly, most of the module settings are fit for fast jobs.  For
much longer running jobs, there are functions that only submit jobs or check jobs and
the settings should be changed for monitoring jobs to only check every 10 or 20 minutes.

Quick Start
***********

.. code-block::

    from aio_aws.aws_batch_models import AWSBatchJob
    from aio_aws.aio_aws_batch import batch_monitor_jobs
    from aio_aws.aio_aws_batch import batch_submit_jobs

    job1 = AWSBatchJob(
        job_name="sleep-1-job",
        job_definition=your_job_definition_arn,
        job_queue=your_job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && sleep 1 && echo Bye"],
    )
    job2 = AWSBatchJob(
        job_name="sleep-2-job",
        job_definition=your_job_definition_arn,
        job_queue=your_job_queue_arn,
        command=["/bin/sh", "-c", "echo Hello && sleep 2 && echo Bye"],
    )
    batch_submit_jobs(jobs=jobs)
    batch_monitor_jobs(jobs=jobs)

This example uses high level synchronous wrappers to make it easy get
started.  There are a lot of details hidden in these convenient wrappers,
but all the code is easily read in `aio_aws.aio_aws_batch`.
For advanced use, please read the source code, especially the unit tests
in the code repo. There is very little time to maintain the documentation.


Monitoring Jobs
***************

The :py:func:`aio_aws.aio_aws_batch.aio_batch_job_manager` can submit a job, wait
for it to complete and retry if it fails on a SPOT termination. It saves the job status
using the :py:class:`aio_aws.aio_aws_batch import AWSBatchDB`.  The job manager
uses :py:func:`aio_aws.aio_aws_batch.aio_batch_job_waiter`, which uses these settings
to control the async-wait between polling the job status:

- :py:const:`aio_aws.aio_aws_batch.BATCH_STARTUP_PAUSE`
- :py:const:`aio_aws.aio_aws_config.MAX_PAUSE`
- :py:const:`aio_aws.aio_aws_config.MIN_PAUSE`

These settings control how often job descriptions are polled.  These requests for job status
are also limited by the client connection pool and the client semaphore used by the job
manager.  Since AWS Batch has API limits on the number of requests for job status, it's best
to use a client connection pool and semaphore of about 10 connections.  Any failures to poll
for a job status will be retried a few times (using some random jitter on retry rates).

To modify the polling frequency settings, use a custom config.  For example, the unit
test suite uses much faster polling on mock batch jobs to speed up the unit tests; e.g.

.. code-block::

    config = AWSBatchConfig(
        start_pause=0.4, min_pause=0.8, max_pause=1.0, min_jitter=0.1, max_jitter=0.2,
    )


.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://botocore.amazonaws.com/v1/documentation/api/latest/index.html

"""

import asyncio
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional

import aiobotocore.client  # type: ignore
import aiobotocore.session  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore

from aio_aws.aio_aws_batch_db import AioAWSBatchDB
from aio_aws.aio_aws_batch_db import AioAWSBatchTinyDB
from aio_aws.aio_aws_config import RETRY_EXCEPTIONS
from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_config import delay
from aio_aws.aio_aws_config import jitter
from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.aws_batch_models import AWSBatchJobStates
from aio_aws.logger import get_logger
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)

#: batch job startup pause (seconds)
BATCH_STARTUP_PAUSE: float = 30


class RetryError(RuntimeError):
    pass


@dataclass
class AWSBatchConfig(AioAWSConfig):

    #: a batch job startup pause, ``random.uniform(start_pause, start_pause * 2)``;
    #: this applies when the job status is in ["SUBMITTED", "PENDING", "RUNNABLE"]
    start_pause: float = BATCH_STARTUP_PAUSE

    #: an optional AioAWSBatchDB
    aio_batch_db: Optional[AioAWSBatchDB] = None

    @asynccontextmanager
    async def create_batch_client(self) -> aiobotocore.client.AioBaseClient:
        """
        Create and yield an AWS Batch client using the ``AWSBatchConfig.session``

        :yield: an aiobotocore.client.AioBaseClient for AWS Batch
        """
        async with self.session.create_client("batch") as client:
            yield client

    @asynccontextmanager
    async def create_logs_client(self) -> aiobotocore.client.AioBaseClient:
        """
        Create and yield an AWS CloudWatchLogs client using the ``AWSBatchConfig.session``

        :yield: an aiobotocore.client.AioBaseClient for AWS CloudWatchLogs
        """
        async with self.session.create_client("logs") as client:
            yield client


def parse_job_description(job_id: str, jobs: Dict) -> Optional[Dict]:
    """
    Extract a job description for ``job_id`` from ``jobs``
    :param job_id: an AWS Batch ``jobId``
    :param jobs: a response to AWS Batch job descriptions
    :return: a job description for ``job_id``
    """
    job_desc = None
    for job in jobs["jobs"]:
        if job["jobId"] == job_id:
            job_desc = job
    return job_desc


async def aio_batch_job_submit(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> AWSBatchJob:
    """
    Submit a batch job; for a successful submission, the jobId and submission details are
    updated on the job object (and an optional jobs-db).

    :param job: A set of job parameters
    :param config: settings for task pauses between retries
    :return: an updated AWSBatchJob (modified in-place)
    :raises: botocore.exceptions.ClientError
    """
    # async with config.semaphore:
    #     async with config.session.create_client("batch") as batch_client:

    if config is None:
        config = AWSBatchConfig.get_default_config()

    task_name = "batch-submit-job"

    async with config.create_batch_client() as batch_client:
        for tries in range(config.retries + 1):
            try:
                await jitter(task_name, 0.0001, 0.01)
                params = job.params
                LOGGER.debug("AWS Batch job params: %s", params)
                response = await batch_client.submit_job(**params)
                LOGGER.debug("AWS Batch job response: %s", response)

                job.job_submission = response
                if response_success(response):
                    job.job_id = response["jobId"]
                    job.status = "SUBMITTED"
                    job.job_tries.append(job.job_id)
                    job.num_tries += 1
                    if config.aio_batch_db:
                        await config.aio_batch_db.save_job(job)
                    LOGGER.info(
                        "AWS %s (%s:%s) try: %d of %d",
                        task_name,
                        job.job_name,
                        job.job_id,
                        job.num_tries,
                        job.max_tries,
                    )
                    return job
                else:
                    # TODO: are there some submission failures that could be recovered here?
                    LOGGER.error("AWS %s (%s:xxx) failure.", task_name, job.job_name)

            except botocore.exceptions.ClientError as err:
                job.job_submission = err.response
                error = err.response.get("Error", {})
                if error.get("Code") in RETRY_EXCEPTIONS:
                    # add an extra random sleep period to avoid API throttle
                    await jitter(task_name, config.min_jitter, config.max_jitter)
                else:
                    raise

        raise RetryError(f"AWS {task_name} exceeded retries")


async def aio_batch_describe_jobs(
    job_ids: List[str], config: AWSBatchConfig = None
) -> Optional[Dict]:
    """
    Asynchronous coroutine to issue a batch job description request
    that retrieves status information for up to 100 jobId

    :param job_ids: a list of batch jobId
    :param config: settings for task pauses between retries
    :return: a describe_jobs response
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    task_name = "batch-describe-jobs"

    async with config.create_batch_client() as batch_client:
        for tries in range(config.retries + 1):
            try:
                await jitter(task_name, 0.0001, 0.01)
                return await batch_client.describe_jobs(jobs=job_ids)

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") in RETRY_EXCEPTIONS:
                    # add an extra random sleep period to avoid API throttle
                    await jitter(task_name, config.min_jitter, config.max_jitter)
                else:
                    raise

        raise RetryError(f"AWS {task_name} exceeded retries")


async def aio_batch_update_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    """
    Asynchronous coroutine to update status on batch jobs.

    job status identifiers in the Batch service are:
    ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "FAILED", "SUCCEEDED"]

    :param jobs: a list of batch jobs
    :param config: settings for task pauses between retries
    :return: jobs are updated in-place, with no return
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    try:
        jobs_by_id = {j.job_id: j for j in jobs if j.job_id}
        job_ids = list(jobs_by_id.keys())

        update_limit = 100  # AWS limit

        for offset in range(0, len(job_ids), update_limit):
            jobs_to_update = job_ids[offset : offset + update_limit]
            response = await aio_batch_describe_jobs(
                job_ids=jobs_to_update, config=config
            )
            if response_success(response):
                for job_desc in response["jobs"]:
                    job = jobs_by_id[job_desc["jobId"]]
                    job.job_description = job_desc
                    job.status = job_desc["status"]
                    if config.aio_batch_db:
                        await config.aio_batch_db.save_job(job)
                    LOGGER.info(
                        "AWS Batch job (%s:%s) status: %s",
                        job.job_name,
                        job.job_id,
                        job.status,
                    )
            else:
                LOGGER.error("AWS Batch job status updates failed")
                LOGGER.error(response)
                raise RuntimeError(response)

    except botocore.exceptions.ClientError as err:
        LOGGER.error("AWS Batch job status updates failed")
        raise


async def aio_batch_job_logs(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> Optional[List[Dict]]:
    """
    Asynchronous coroutine to get logs for a batch job log stream.  All
    the events available for a log stream are collected and returned.  The
    AWS Cloud Watch log streams can have delays, don't expect near real-time
    values in these data.

    :param job: A set of job parameters
    :param config: settings for task pauses between retries
    :return: a list of all the log events from get_log_events responses
    :raises: botocore.exceptions.ClientError
    :raises: RetryError if it exceeds retries

    .. seealso::
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    if not job.job_description:
        LOGGER.warning("AWS Batch job has no description, cannot fetch logs")
        return

    log_stream_name = job.job_description.get("container", {}).get("logStreamName")
    if not log_stream_name:
        LOGGER.warning(
            "AWS Batch job (%s:%s) has no log stream.", job.job_name, job.job_id
        )
        return

    task_name = "batch-job-logs"

    async with config.create_logs_client() as logs_client:
        for tries in range(config.retries + 1):
            try:
                log_events = []

                forward_token = None
                while True:
                    kwargs = {
                        "logGroupName": "/aws/batch/job",
                        "logStreamName": log_stream_name,
                        "startFromHead": True,
                        # "startTime": 123,
                        # "endTime": 123,
                        # "limit": 123,
                    }
                    if forward_token:
                        kwargs["nextToken"] = forward_token

                    await jitter(task_name, 0.0001, 0.01)
                    response = await logs_client.get_log_events(**kwargs)

                    events = response.get("events", [])
                    log_events.extend(events)

                    if forward_token != response["nextForwardToken"]:
                        forward_token = response["nextForwardToken"]
                    else:
                        break

                if log_events:
                    LOGGER.info(
                        "AWS %s (%s:%s) events: %d",
                        task_name,
                        job.job_name,
                        job.job_id,
                        len(log_events),
                    )
                    job.logs = log_events
                    if config.aio_batch_db:
                        await config.aio_batch_db.save_job_logs(job)
                else:
                    LOGGER.warning(
                        "AWS Batch job (%s:%s) has no log events",
                        job.job_name,
                        job.job_id,
                    )

                return log_events

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") in RETRY_EXCEPTIONS:
                    # add an extra random sleep period to avoid API throttle
                    await jitter(task_name, config.min_jitter, config.max_jitter)
                else:
                    raise

        raise RetryError(f"AWS {task_name} exceeded retries")


async def aio_batch_job_cancel(
    job: AWSBatchJob, reason: str = "User cancelled job", config: AWSBatchConfig = None
) -> AWSBatchJob:
    """
    Asynchronous coroutine to cancel a job i a batch job queue.
    Jobs that are in the SUBMITTED, PENDING, or RUNNABLE state are canceled.
    Jobs that have progressed to STARTING or RUNNING are not canceled, but
    the API operation still succeeds, even if no job is canceled.
    These jobs must be terminated.

    :param job: an AWSBatchJob
    :param reason: a reason to cancel the job
    :param config: settings for task pauses between retries
    :return: an updated AWSBatchJob
    :raises: botocore.exceptions.ClientError
    :raises: RetryError if it exceeds retries
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    task_name = "batch-job-cancel"

    async with config.create_batch_client() as batch_client:

        for tries in range(config.retries + 1):
            try:
                await jitter(task_name, 0.0001, 0.01)
                LOGGER.info("AWS Batch job to cancel: %s, %s", job.job_id, reason)
                response = await batch_client.cancel_job(
                    jobId=job.job_id, reason=reason
                )
                LOGGER.debug("AWS Batch job response: %s", response)

                if response_success(response):
                    # Waiting for the job to complete is necessary; checking the job status
                    # immediately after terminating it can get a status like RUNNABLE or RUNNING.
                    # The job-waiter updates the job object (it's authorized for side-effects)
                    await aio_batch_job_waiter(job=job, config=config)
                    return job
                else:
                    continue

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") in RETRY_EXCEPTIONS:
                    # add an extra random sleep period to avoid API throttle
                    await jitter(task_name, config.min_jitter, config.max_jitter)
                else:
                    raise err

        raise RetryError(f"AWS {task_name} exceeded retries")


async def aio_batch_job_terminate(
    job: AWSBatchJob, reason: str = "User terminated job", config: AWSBatchConfig = None
) -> AWSBatchJob:
    """
    Asynchronous coroutine to terminate a batch job

    :param job: an AWSBatchJob
    :param reason: a reason to terminate the job
    :param config: settings for task pauses between retries
    :return: an AWSBatchJob after it is terminated
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    task_name = "batch-job-terminate"

    async with config.create_batch_client() as batch_client:

        for tries in range(config.retries + 1):
            try:
                await jitter(task_name, 0.0001, 0.01)
                LOGGER.info("AWS Batch job to terminate: %s, %s", job.job_id, reason)
                response = await batch_client.terminate_job(
                    jobId=job.job_id, reason=reason
                )
                LOGGER.debug("AWS Batch job response: %s", response)

                if response_success(response):
                    # Waiting for the job to complete is necessary, to get the final status.
                    # The job-waiter updates the job object (it's authorized for side-effects)
                    await aio_batch_job_waiter(job=job, config=config)
                    return job

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") in RETRY_EXCEPTIONS:
                    # add an extra random sleep period to avoid API throttle
                    await jitter(task_name, config.min_jitter, config.max_jitter)
                else:
                    raise

        raise RetryError(f"AWS {task_name} exceeded retries")


async def aio_batch_job_status(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> AWSBatchJob:
    """
    Asynchronous coroutine to update a single job description.
    The job data is updated (it's saved to an optional jobs-db).

    Use :py:func:`aio_batch_update_jobs` for more efficient updates
    to many jobs.

    :param job: a batch job
    :param config: settings for task pauses between retries
    :return: an updated AWSBatchJob
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    try:
        response = await aio_batch_describe_jobs(job_ids=[job.job_id], config=config)
        LOGGER.debug(
            "AWS Batch job (%s:%s) status: %s", job.job_name, job.job_id, response
        )

        job_desc = parse_job_description(job.job_id, response)
        if job_desc:

            job.job_description = job_desc
            job.status = job_desc["status"]
            if config.aio_batch_db:
                await config.aio_batch_db.save_job(job)

            LOGGER.info(
                "AWS Batch job (%s:%s) status: %s",
                job.job_name,
                job.job_id,
                job.status,
            )
            return job

        LOGGER.error(
            "AWS Batch job (%s:%s) failed job description",
            job.job_name,
            job.job_id,
        )
        return job

    except botocore.exceptions.ClientError as err:
        LOGGER.error("AWS Batch job (%s:%s) failed to update", job.job_name, job.job_id)


async def aio_batch_job_waiter(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> Optional[AWSBatchJob]:
    """
    Asynchronous coroutine to wait on a batch job.  There is no explict
    timeout on a job waiter, it depends on setting a timeout on the batch
    job definition.  The job waiter exits when the batch job status is
    either "SUCCEEDED" or "FAILED".  The job data is updated each time
    the job description is polled (it's also saved to the jobs-db).

    job status identifiers are assumed to be:
    ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "FAILED", "SUCCEEDED"]

    :param job: a batch job
    :param config: settings for task pauses between retries
    :return: a describe_jobs response for job.job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    try:
        while True:
            job = await aio_batch_job_status(job=job, config=config)

            if job.status in ["FAILED", "SUCCEEDED"]:
                return job

            if job.status in ["SUBMITTED", "PENDING", "RUNNABLE"]:
                # Wait longer than regular pause to allow job startup
                await delay(job.job_name, config.start_pause, config.start_pause * 2)
            else:
                await delay(job.job_name, config.min_pause, config.max_pause)

    except botocore.exceptions.ClientError as err:
        LOGGER.error(
            "AWS Batch job (%s:%s) failed to monitor job", job.job_name, job.job_id
        )
        raise


async def aio_batch_job_manager(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> AWSBatchJob:
    """
    Asynchronous coroutine to manage a batch job.  The job-manager
    will update attributes on the job as the job passes through
    submission and completion.  On completion, the job logs are
    retrieved.  As the job status and logs are collected, the
    job data is persisted to the jobs_db.

    Note that any job with a job.job_id will not re-run, those
    jobs will be monitored until complete.  To re-run a job that
    has already run, first call the job.reset() method to clear
    any previous job state.  (Any previous attempts are recorded
    in job.job_tries and in the job.job_description.)

    :param job: a batch job spec
    :param config: settings for task pauses between retries
    :return: a describe_jobs response for job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    LOGGER.debug(
        "AWS Batch job (%s:%s) management",
        job.job_name,
        job.job_id,
    )

    while True:

        # jobs with an existing job.job_id skip submission to be waited
        if job.allow_submit_job():
            # the job submission can update the job data and jobs-db
            await aio_batch_job_submit(job, config=config)
            if job.job_id is None:
                # If the job-submission failed, don't retry it
                # because there is probably something to fix.
                return job

        # the job waiter updates the job description and status
        await aio_batch_job_waiter(job, config=config)

        if job.status == "FAILED":
            # SPOT failure requires a retry, it usually has
            # statusReason: Host EC2 (instance {instanceId}) terminated
            #
            # Some common reasons with no retry behavior:
            # - user initiated job termination with a custom reason
            # - "Dependent Job failed" - unknown upstream failure
            # - "Essential container in task exited" - container error

            reason = job.job_description.get("statusReason", "")
            if re.match(r"Host EC2.*terminated", reason):
                LOGGER.warning(
                    "AWS Batch job (%s:%s) SPOT failure, run retry.",
                    job.job_name,
                    job.job_id,
                )
                job.reset()  # reset jobs are not saved to the jobs-db
                continue

        return job


async def aio_batch_run_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    """
    Submit jobs that have not been submitted yet,
    and monitor all jobs until they complete.

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    tasks = [
        asyncio.create_task(aio_batch_job_manager(job=job, config=config))
        for job in jobs
    ]
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


async def aio_batch_submit_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    """
    Submit jobs that have not been submitted yet.

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    tasks = [
        asyncio.create_task(aio_batch_job_submit(job=job, config=config))
        for job in jobs
        if job.allow_submit_job()
    ]
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


async def aio_batch_monitor_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    """
    Monitor submitted jobs until they complete.

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    tasks = [
        asyncio.create_task(aio_batch_job_manager(job=job, config=config))
        for job in jobs
        if job.job_id
    ]
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


async def aio_batch_cancel_jobs(
    jobs: Iterable[AWSBatchJob],
    reason: str = "User cancelled job",
    config: AWSBatchConfig = None,
):
    """
    Cancel jobs that can be cancelled (if they are not running or complete).

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    tasks = [
        asyncio.create_task(aio_batch_job_cancel(job=job, reason=reason, config=config))
        for job in jobs
        if job.job_id
    ]
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


async def aio_batch_terminate_jobs(
    jobs: Iterable[AWSBatchJob],
    reason: str = "User terminated job",
    config: AWSBatchConfig = None,
):
    """
    Terminate jobs that can be killed (if they are not complete).

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    tasks = [
        asyncio.create_task(
            aio_batch_job_terminate(job=job, reason=reason, config=config)
        )
        for job in jobs
        if job.job_id
    ]
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


async def aio_batch_get_logs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    """
    Get job logs.  The logs should be updated in any
    configured jobs-db.

    :param jobs: any AWSBatchJob
    :param config: an AWSBatchConfig
    :return: each job maintains state, so this function returns nothing
    """
    batch_tasks = [
        asyncio.create_task(aio_batch_job_logs(job=job, config=config))
        for job in jobs
        if job.job_id
    ]
    for task in asyncio.as_completed(batch_tasks):
        try:
            result = await task
            LOGGER.debug(result)
        except Exception as err:
            LOGGER.error(err)


def job_for_status(job, job_states) -> Optional[AWSBatchJob]:
    if job.job_id and job.status in job_states:
        LOGGER.info(
            "AWS Batch job (%s:%s) has status: %s",
            job.job_name,
            job.job_id,
            job.status,
        )
        return job


async def aio_find_jobs_by_status(
    jobs: Iterable[AWSBatchJob],
    job_states: List[str],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any jobs that match job states.

    This is most often used when jobs are regenerated for jobs that could
    exist in the jobs-db; there are alternative methods on the jobs-db to
    find all jobs matching various job states.

    :param jobs: any AWSBatchJob
    :param job_states: a list of valid job states
    :param jobs_db: a jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    # In this function, avoid side-effects in the jobs-db
    # - treat the job as a read-only object
    # - treat any jobs_db records as read-only objects
    # - if a job is not saved, don't save it to jobs_db

    LOGGER.info("Selecting job states: %s", job_states)

    for job in jobs:

        # First check the job itself before checking the jobs_db
        if job_for_status(job, job_states):
            yield job

        if jobs_db:
            db_job = await jobs_db.find_latest_job_name(job.job_name)
            if db_job:
                if job_for_status(db_job, job_states):
                    yield db_job


async def aio_find_complete_jobs(
    jobs: Iterable[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any complete jobs

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    async for job in aio_find_jobs_by_status(
        jobs=jobs,
        job_states=["SUCCEEDED", "FAILED"],
        jobs_db=jobs_db,
    ):
        yield job


async def aio_find_running_jobs(
    jobs: Iterable[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any running jobs

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    job_states = [s.name for s in AWSBatchJobStates if s < AWSBatchJobStates.SUCCEEDED]
    # find_jobs_by_status should yield
    async for job in aio_find_jobs_by_status(
        jobs=jobs,
        job_states=job_states,
        jobs_db=jobs_db,
    ):
        yield job


def find_jobs_by_status(
    jobs: Iterable[AWSBatchJob],
    job_states: List[str],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any jobs that match job states.

    This is most often used when jobs are regenerated for jobs that could
    exist in the jobs-db; there are alternative methods on the jobs-db to
    find all jobs matching various job states.

    :param jobs: any AWSBatchJob
    :param job_states: a list of valid job states
    :param jobs_db: a jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    # In this function, avoid side-effects in the jobs-db
    # - treat the job as a read-only object
    # - treat any jobs_db records as read-only objects
    # - if a job is not saved, don't save it to jobs_db

    LOGGER.info("Selecting job states: %s", job_states)

    for job in jobs:

        # First check the job itself before checking the jobs_db
        if job_for_status(job, job_states):
            yield job

        if jobs_db:
            db_job = asyncio.run(jobs_db.find_latest_job_name(job.job_name))
            if db_job:
                if job_for_status(db_job, job_states):
                    yield db_job


def find_complete_jobs(
    jobs: Iterable[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any complete jobs

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    for job in find_jobs_by_status(
        jobs=jobs,
        job_states=["SUCCEEDED", "FAILED"],
        jobs_db=jobs_db,
    ):
        yield job


def find_running_jobs(
    jobs: Iterable[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
):
    """
    Find any running jobs

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :return: yield jobs found
    """
    job_states = [s.name for s in AWSBatchJobStates if s < AWSBatchJobStates.SUCCEEDED]
    # find_jobs_by_status should yield
    for job in find_jobs_by_status(
        jobs=jobs,
        job_states=job_states,
        jobs_db=jobs_db,
    ):
        yield job


def find_incomplete_jobs(
    jobs: Iterable[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    reset_failed: bool = False,
):
    """
    This can find any jobs that are not complete.  It can also
    reset FAILED jobs that could be run again.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to check the latest
        job status from the jobs-db, if it is not
        available on the job (highly recommended)
    :param reset_failed: if enabled, any FAILED jobs
        will be reset so they can be run again
    :return: yield jobs found
    """
    # In this function, avoid side-effects in the jobs-db
    # - treat the job as a read-only object
    # - treat any jobs_db records as read-only objects
    # - if a job is not saved, don't save it to jobs_db

    LOGGER.info("Selecting incomplete jobs")

    for job in jobs:

        # First check the job itself before checking the jobs_db
        if job.job_id and job.status == "SUCCEEDED":
            LOGGER.debug(
                "AWS Batch job (%s:%s) has status: %s",
                job.job_name,
                job.job_id,
                job.status,
            )
            # Skip successful jobs, they are done
            continue

        if job.job_id and job.status == "FAILED":
            # Reset any failed jobs, assuming they can be rerun OK.
            if reset_failed:
                job.reset()
                if job.num_tries >= job.max_tries:
                    job.max_tries += 1
                # Don't save the job to the jobs_db here when it already ran to completion;
                # when it is submitted for a rerun, it will create a new db entry.
                LOGGER.info("AWS Batch job (%s) reset", job.job_name)
                yield job
                continue

        if jobs_db:

            db_job = asyncio.run(jobs_db.find_latest_job_name(job.job_name))

            if db_job is None:
                # these jobs are new jobs
                if job.job_id is None:
                    LOGGER.debug("AWS Batch job (%s) has no db entry", job.job_name)
                    yield job
                    continue

            LOGGER.debug(
                "AWS Batch job (%s:%s) has db status: %s",
                db_job.job_name,
                db_job.job_id,
                db_job.status,
            )

            if db_job.job_id and db_job.status == "SUCCEEDED":
                # Skip successful jobs, they are done
                continue

            if db_job.job_id and db_job.status == "FAILED":
                # Reset any failed jobs, assuming they can be rerun OK.
                if reset_failed:
                    db_job.reset()
                    if db_job.num_tries >= db_job.max_tries:
                        db_job.max_tries += 1
                    # Don't save the job to the jobs_db here when it already ran to completion;
                    # when it is submitted for a rerun, it will create a new db entry.
                    LOGGER.info("AWS Batch job (%s) reset to re-run.", db_job.job_name)
                    yield db_job
                    continue

            # these jobs either have not started or they are still
            # in progress and they can be monitored for completion
            yield db_job


def batch_run_jobs(
    jobs: List[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Submit jobs that have not been submitted yet,
    and monitor all jobs until they complete.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The polling is kept to a minimum to avoid interference with the batch API;
    # max_pool_connections = 1 is used because of details in aio-aws where it
    # creates a new client for each monitoring task (that could change in new
    # releases of aio-aws).
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=10,
            max_pause=20,
            start_pause=60,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(aio_batch_run_jobs(jobs=jobs, config=aio_batch_config))


def batch_submit_jobs(
    jobs: List[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Submit jobs that have not been submitted yet.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The job submission can run fairly fast (without any monitoring).
    # max_pool_connections = 1 is used because of details in aio-aws where it
    # creates a new client for each monitoring task (that could change in new
    # releases of aio-aws).
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=2,
            max_pause=10,
            start_pause=10,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(aio_batch_submit_jobs(jobs=jobs, config=aio_batch_config))


def batch_monitor_jobs(
    jobs: List[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Monitor submitted jobs until they complete.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The polling is kept to a minimum to avoid interference with the batch API;
    # max_pool_connections = 1 is used because of details in aio-aws where it
    # creates a new client for each monitoring task (that could change in new
    # releases of aio-aws).
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=10,
            max_pause=20,
            start_pause=60,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(aio_batch_monitor_jobs(jobs=jobs, config=aio_batch_config))


def batch_update_jobs(
    jobs: List[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Update job descriptions.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The polling is kept to a minimum to avoid interference with the batch API;
    # max_pool_connections = 1 is used because of details in aio-aws where it
    # creates a new client for each monitoring task (that could change in new
    # releases of aio-aws).
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=10,
            max_pause=20,
            start_pause=60,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(aio_batch_update_jobs(jobs=jobs, config=aio_batch_config))


def batch_get_logs(
    jobs: List[AWSBatchJob],
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Get job logs.

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The polling is kept to a minimum to avoid interference with the batch API;
    # max_pool_connections = 1 is used because of details in aio-aws where it
    # creates a new client for each monitoring task (that could change in new
    # releases of aio-aws).
    if aio_batch_config is None:
        # AWS Batch logs has limited bandwidth, so default
        # settings try to avoid rate throttling
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_jitter=3,
            max_jitter=8,
            min_pause=2,
            max_pause=10,
            start_pause=2,
            max_pool_connections=1,
            sem=100,
        )
    asyncio.run(aio_batch_get_logs(jobs=jobs, config=aio_batch_config))


def batch_cancel_jobs(
    jobs: List[AWSBatchJob],
    reason: str = "User cancelled job",
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Cancel jobs that can be cancelled (if they are not running or complete).

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The tasks can run fairly fast (without any monitoring).
    # max_pool_connections = 1 is used because a new client is used for each task
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=2,
            max_pause=10,
            start_pause=10,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(
        aio_batch_cancel_jobs(jobs=jobs, reason=reason, config=aio_batch_config)
    )


def batch_terminate_jobs(
    jobs: List[AWSBatchJob],
    reason: str = "User terminated job",
    jobs_db: AioAWSBatchDB = None,
    aio_batch_config: AWSBatchConfig = None,
):
    """
    Terminate jobs that can be killed (if they are not complete).

    :param jobs: any AWSBatchJob
    :param jobs_db: an optional jobs-db to persist job data;
        this is only applied if an aio_batch_config is not provided
    :param aio_batch_config: a custom AWSBatchConfig; if provided,
        it is responsible for providing any optional jobs-db
    :return: each job maintains state, so this function returns nothing
    """
    # The tasks can run fairly fast (without any monitoring).
    # max_pool_connections = 1 is used because a new client is used for each task
    if aio_batch_config is None:
        aio_batch_config = AWSBatchConfig(
            aio_batch_db=jobs_db,
            min_pause=2,
            max_pause=10,
            start_pause=10,
            max_pool_connections=1,
            sem=500,
        )
    asyncio.run(
        aio_batch_terminate_jobs(jobs=jobs, reason=reason, config=aio_batch_config)
    )
