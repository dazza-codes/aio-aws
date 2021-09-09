#! /usr/bin/env python3
# pylint: disable=bad-continuation

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

The batch data is a `TinyDB`_ json file, e.g.

.. code-block::

    >>> import json
    >>> jobs_db_file = '/tmp/aio_batch_jobs.json'
    >>> logs_db_file = '/tmp/aio_batch_logs.json'

    >>> with open(jobs_db_file) as job_file:
    ...     batch_data = json.load(job_file)
    ...
    >>> len(batch_data['aws-batch-jobs'])
    5

    >>> import tinydb
    >>> tinydb.TinyDB.DEFAULT_TABLE = "aws-batch-jobs"
    >>> tinydb.TinyDB.DEFAULT_TABLE_KWARGS = {"cache_size": 0}
    >>> # the jobs and logs are kept in separate DBs so
    >>> # that performance on the jobs_db is not compromised
    >>> # by too much data from job logs.
    >>> jobs_db = tinydb.TinyDB(jobs_db_file)
    >>> logs_db = tinydb.TinyDB(logs_db_file)


For the demo to run quickly, most of the module settings are fit for fast jobs.  For
much longer running jobs, there are functions that only submit jobs or check jobs and
the settings should be changed for monitoring jobs to only check every 10 or 20 minutes.

Monitoring Jobs
***************

The :py:func:`aio_aws.aio_aws_batch.aio_batch_job_manager` can submit a job, wait
for it to complete and retry if it fails on a SPOT termination. It saves the job status
using the :py:class:`aio_aws.aio_aws_batch import AWSBatchDB`.  The job manager
uses :py:func:`aio_aws.aio_aws_batch.aio_batch_job_waiter`, which uses these settings
to control the async-wait between polling the job status:

- :py:const:`aio_aws.aio_aws_config.BATCH_STARTUP_PAUSE`
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


Getting Started
***************

The example above uses code similar to the following.  The following code appears
to work like regular synchronous code; behind the scenes, it wraps asyncio coroutines
and event loops.

To fully understand it, look at the module code directly.  For custom solutions, use
an event loop and some of the coroutines available.  Note that using
asyncio for AWS services requires the `aiobotocore`_ library, which wraps a
release of `botocore`_ to patch it with features for async coroutines using
`asyncio`_ and `aiohttp`_.  To avoid issuing too many concurrent requests (DOS attack),
the async approach should use a client connection limiter, based on ``asyncio.Semaphore()``.
It's recommended to use a single session and a single client with a connection pool.
Although there are context manager patterns, it's also possible to manage closing the client
after everything is done.

.. code-block::

    # python 3.6

    import asyncio

    from aio_aws.aio_aws_batch import AWSBatchConfig
    from aio_aws.aio_aws_batch import AWSBatchDB
    from aio_aws.aio_aws_batch import AWSBatchJob
    from aio_aws.aio_aws_batch import aio_batch_run_jobs
    from aio_aws.aio_aws_batch import aio_batch_get_logs

    # For the `aws_region` where batch will run:
    # - create a `batch-dev` compute environment
    # - create a `batch-dev` batch queue
    # - create a `batch-dev` job definition using alpine:latest

    job_command = "for a in `seq 1 10`; do echo Hello $a; sleep 0.5; done"

    # Create a list of AWSBatchJob objects, which are simple wrappers
    # on AWS Batch job data.  These objects are simple, by design, to
    # allow them to be used in either synchronous or asynchronous code.
    batch_jobs = []
    for i in range(20):
        job_name = f"test-logs-job-{i:04d}"

        # Creating an AWSBatchJob instance does not run anything, it's simply
        # a dataclass to retain and track job attributes.
        # Replace 'command' with 'container_overrides' dict for more options;
        # do not use 'command' together with 'container_overrides'.
        batch_job = AWSBatchJob(
            job_name=job_name,
            job_definition="batch-dev",
            job_queue="batch-dev",
            command=["/bin/sh", "-c", job_command],
        )
        batch_jobs.append(batch_job)

    # The AWSBatchDB wraps TinyDB data stores; it can be
    # used in either synchronous or asynchronous code.
    batch_jobs_db = AWSBatchDB(
        jobs_db_file="/tmp/aio_batch_jobs.json",
        logs_db_file="/tmp/aio_batch_logs.json",
    )
    batch_jobs = batch_jobs_db.jobs_recovery(jobs=batch_jobs)
    batch_jobs = batch_jobs_db.jobs_to_run(jobs=batch_jobs)
    if batch_jobs:

        # Create an event loop for the aio processing
        loop = asyncio.get_event_loop()
        loop.set_debug(enabled=True)
        try:

            # for polling frequency of 10-30 seconds, with 30-60 second job starts;
            # use lower frequency polling for long running jobs
            aio_config = AWSBatchConfig(
                max_pool_connections=10,
                aws_region="us-west-2",
                batch_db=batch_jobs_db,
                min_pause=10,
                max_pause=30,
                start_pause=30,
            )

            loop.run_until_complete(aio_batch_run_jobs(jobs=batch_jobs, config=aio_config))

            complete_jobs = []
            for job in batch_jobs:
                if job.status in ["SUCCEEDED", "FAILED"]:
                    complete_jobs.append(job)

            loop.run_until_complete(aio_batch_get_logs(jobs=complete_jobs, config=aio_config))

        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.stop()
            loop.close()


.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://botocore.amazonaws.com/v1/documentation/api/latest/index.html

"""

import asyncio
import os
import re
from asyncio import AbstractEventLoop
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Iterable
from typing import Optional

import aiobotocore.client  # type: ignore
import aiobotocore.session  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore
import tinydb

from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_config import delay
from aio_aws.aio_aws_config import jitter
from aio_aws.aws_batch_db import AWSBatchDB
from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.utils import response_success
from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)

#: batch job startup pause (seconds)
BATCH_STARTUP_PAUSE: float = 30


@dataclass
class AWSBatchConfig(AioAWSConfig):
    #: a batch job startup pause, ``random.uniform(start_pause, start_pause * 2)``;
    #: this applies when the job status is in ["SUBMITTED", "PENDING", "RUNNABLE"]
    start_pause: float = BATCH_STARTUP_PAUSE
    #: an optional AWSBatchDB
    batch_db: Optional[AWSBatchDB] = None

    def get_batch_db(self) -> AWSBatchDB:
        """
        Return the provided batch_db or create and return a default one

        :return: an AWSBatchDB
        """
        if self.batch_db is None:
            self.batch_db = AWSBatchDB()
        return self.batch_db

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


async def aio_batch_job_submit(job: AWSBatchJob, config: AWSBatchConfig = None) -> Dict:
    """
    Asynchronous coroutine to submit a batch job; for a successful
    job submission, the jobId and other submission details are
    updated on the job object and persisted to ``config.batch_db``.

    :param job: A set of job parameters
    :param config: settings for task pauses between retries
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    # async with config.semaphore:
    #     async with config.session.create_client("batch") as batch_client:

    if config is None:
        config = AWSBatchConfig.get_default_config()

    async with config.create_batch_client() as batch_client:
        tries = 0
        while tries <= config.retries:
            tries += 1
            try:
                params = job.params
                LOGGER.debug("AWS Batch job params: %s", params)
                response = await batch_client.submit_job(**params)
                LOGGER.debug("AWS Batch job response: %s", response)

                if response_success(response):
                    job.job_id = response["jobId"]
                    job.job_submission = response
                    job.job_tries.append(job.job_id)
                    job.num_tries += 1
                    jobs_db = config.get_batch_db()
                    async with jobs_db.db_semaphore:
                        jobs_db.save_job(job)
                    LOGGER.info(
                        "AWS Batch job (%s:%s) submitted try: %d of %d",
                        job.job_name,
                        job.job_id,
                        job.num_tries,
                        job.max_tries,
                    )
                else:
                    # TODO: are there some submission failures that could be recovered here?
                    LOGGER.error(
                        "AWS Batch job (%s:xxx) submission failure.", job.job_name
                    )
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries <= config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter(
                            "batch-job-submit", config.min_jitter, config.max_jitter
                        )
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job submission exceeded retries")


async def aio_batch_job_status(
    jobs: List[str], config: AWSBatchConfig = None
) -> Optional[Dict]:
    """
    Asynchronous coroutine to issue a batch job description request

    :param jobs: a list of batch jobId
    :param config: settings for task pauses between retries
    :return: a describe_jobs response
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    async with config.create_batch_client() as batch_client:
        tries = 0
        while tries <= config.retries:
            tries += 1
            try:
                return await batch_client.describe_jobs(jobs=jobs)
            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries <= config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter(
                            "batch-job-status", config.min_jitter, config.max_jitter
                        )
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job description exceeded retries")


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

    async with config.create_logs_client() as logs_client:
        tries = 0
        while tries <= config.retries:
            tries += 1
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

                    response = await logs_client.get_log_events(**kwargs)

                    events = response.get("events", [])
                    log_events.extend(events)

                    if forward_token != response["nextForwardToken"]:
                        forward_token = response["nextForwardToken"]
                    else:
                        break

                if log_events:
                    LOGGER.info(
                        "AWS Batch job (%s:%s) log events: %d",
                        job.job_name,
                        job.job_id,
                        len(log_events),
                    )
                    job.logs = log_events
                    jobs_db = config.get_batch_db()
                    async with jobs_db.db_semaphore:
                        jobs_db.save_job_logs(job)
                else:
                    LOGGER.warning(
                        "AWS Batch job (%s:%s) has no log events",
                        job.job_name,
                        job.job_id,
                    )

                return log_events

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries <= config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter(
                            "batch-job-logs", config.min_jitter, config.max_jitter
                        )
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job logs exceeded retries")


async def aio_batch_job_terminate(
    job_id: str, reason: str, config: AWSBatchConfig = None
) -> Dict:
    """
    Asynchronous coroutine to terminate a batch job

    :param job_id: a batch jobId
    :param reason: a reason to terminate the job
    :param config: settings for task pauses between retries
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    if config is None:
        config = AWSBatchConfig.get_default_config()

    async with config.create_batch_client() as batch_client:

        tries = 0
        while tries <= config.retries:
            tries += 1
            try:
                LOGGER.info("AWS Batch job to terminate: %s, %s", job_id, reason)
                response = await batch_client.terminate_job(jobId=job_id, reason=reason)
                LOGGER.info("AWS Batch job response: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries <= config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter(
                            "batch-job-terminate", config.min_jitter, config.max_jitter
                        )
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job termination exceeded retries")


async def aio_batch_job_waiter(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> Optional[Dict]:
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
        monitor_failures = 0
        while True:
            response = await aio_batch_job_status([job.job_id], config)
            LOGGER.debug(
                "AWS Batch job (%s:%s) status: %s", job.job_name, job.job_id, response
            )

            job_desc = parse_job_description(job.job_id, response)
            if job_desc:

                job.job_description = job_desc
                job.status = job_desc["status"]
                jobs_db = config.get_batch_db()
                async with jobs_db.db_semaphore:
                    jobs_db.save_job(job)  # TODO: use async-db

                LOGGER.info(
                    "AWS Batch job (%s:%s) status: %s",
                    job.job_name,
                    job.job_id,
                    job.status,
                )

                if job_desc["status"] in ["FAILED", "SUCCEEDED"]:
                    return job_desc

                if job_desc["status"] in ["SUBMITTED", "PENDING", "RUNNABLE"]:
                    # Wait longer than regular pause to allow job startup
                    await delay(
                        job.job_name, config.start_pause, config.start_pause * 2
                    )

            else:
                monitor_failures += 1
                if monitor_failures <= config.retries:
                    LOGGER.warning(
                        "AWS Batch job (%s:%s) retry job description",
                        job.job_name,
                        job.job_id,
                    )
                else:
                    LOGGER.error(
                        "AWS Batch job (%s:%s) failed to monitor job",
                        job.job_name,
                        job.job_id,
                    )
                    break

            # Allow the job status to be checked first, in case it is complete.
            await delay(job.job_name, config.min_pause, config.max_pause)

    except botocore.exceptions.ClientError as err:
        LOGGER.error(
            "AWS Batch job (%s:%s) failed to monitor job", job.job_name, job.job_id
        )
        raise


async def aio_batch_job_manager(
    job: AWSBatchJob, config: AWSBatchConfig = None
) -> Optional[Dict]:
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

    while True:

        # jobs with an existing job.job_id skip submission to be waited
        if job.job_id is None:
            if job.num_tries < job.max_tries:
                # the job submission can update the job data and jobs-db
                response = await aio_batch_job_submit(job, config=config)
                if not response_success(response):
                    # If the job-submission has failed, don't retry it
                    # because there is probably something to fix.
                    return
            else:
                LOGGER.warning(
                    "AWS Batch job (%s) exceeds retries: %d of %d",
                    job.job_name,
                    job.num_tries,
                    job.max_tries,
                )
                return

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

        return job.job_description


async def aio_batch_submit_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    # Each job maintains state, so this function returns nothing.
    tasks = []
    for job in jobs:
        # jobs with an existing job.job_id should skip submission
        # to avoid resubmission for the same job
        if job.job_id is None:
            if job.num_tries < job.max_tries:
                # the job submission can update the job data and jobs-db
                task = asyncio.create_task(aio_batch_job_submit(job=job, config=config))
                tasks.append(task)
            else:
                LOGGER.warning(
                    "AWS Batch job (%s) exceeds retries: %d of %d",
                    job.job_name,
                    job.num_tries,
                    job.max_tries,
                )
    await asyncio.gather(*tasks)


async def aio_batch_monitor_jobs(jobs: Iterable[AWSBatchJob], config: AWSBatchConfig):
    # Each job maintains state, so this function returns nothing.
    await asyncio.gather(
        *[
            asyncio.create_task(aio_batch_job_manager(job=job, config=config))
            for job in jobs
        ]
    )


async def handle_tasks_as_completed(tasks, loop=None):
    for task in asyncio.as_completed(tasks, loop=loop):
        job_desc = await task
        LOGGER.debug(job_desc)


async def aio_batch_run_jobs(
    jobs: Iterable[AWSBatchJob], config: AWSBatchConfig, loop: AbstractEventLoop = None
):
    if loop is None:
        # Ensure that an event loop is running in this thread
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

    batch_tasks = [
        loop.create_task(aio_batch_job_manager(job=job, config=config)) for job in jobs
    ]
    await handle_tasks_as_completed(batch_tasks)

    # Using the asyncio.Task API
    for task in batch_tasks:
        if task.done():
            try:
                task.exception()
            except asyncio.CancelledError as err:
                LOGGER.exception(err)

            result = task.result()
            LOGGER.debug(result)


async def aio_batch_get_logs(
    jobs: Iterable[AWSBatchJob], config: AWSBatchConfig, loop: AbstractEventLoop = None
):
    if loop is None:
        loop = asyncio.get_event_loop()

    batch_tasks = [
        loop.create_task(aio_batch_job_logs(job=job, config=config)) for job in jobs
    ]
    await handle_tasks_as_completed(batch_tasks)

    # Using the asyncio.Task API
    for task in batch_tasks:
        if task.done():
            try:
                task.exception()
            except asyncio.CancelledError as err:
                LOGGER.exception(err)

            result = task.result()
            LOGGER.debug(result)


if __name__ == "__main__":

    print()
    print("Test async batch jobs")

    # job_command = "for a in `seq 1 10000`; do echo Hello $a; done"
    job_command = "for a in `seq 1 10`; do echo Hello $a; sleep 0.5; done"

    n_jobs = 20
    batch_jobs = []
    for i in range(n_jobs):
        job_name = f"test-job-{i:04d}"

        # Creating an AWSBatchJob instance does not run anything, it's simply
        # a dataclass to retain and track job attributes.
        # Replace 'command' with 'container_overrides' dict for more options;
        # do not use 'command' together with 'container_overrides'.
        batch_job = AWSBatchJob(
            job_name=job_name,
            job_definition="batch-dev",
            job_queue="batch-dev",
            command=["/bin/sh", "-c", job_command],
        )
        batch_jobs.append(batch_job)

    batch_jobs_db = AWSBatchDB(
        jobs_db_file="/tmp/aio_batch_jobs.json", logs_db_file="/tmp/aio_batch_logs.json"
    )
    batch_jobs = batch_jobs_db.jobs_recovery(jobs=batch_jobs)
    batch_jobs = batch_jobs_db.jobs_to_run(jobs=batch_jobs)
    if batch_jobs:

        # Create an event loop for the aio processing
        loop = asyncio.get_event_loop()
        loop.set_debug(enabled=True)
        try:

            # for polling frequency of 5-20 seconds, with 30-60 second job starts
            aio_config = AWSBatchConfig(
                aws_region=os.getenv("AWS_DEFAULT_REGION", "us-west-2"),
                batch_db=batch_jobs_db,
                max_pool_connections=10,
                min_pause=5,
                max_pause=20,
                start_pause=30,
            )

            loop.run_until_complete(
                aio_batch_run_jobs(jobs=batch_jobs, config=aio_config)
            )

            complete_jobs = []
            for job in batch_jobs:
                if job.status in ["SUCCEEDED", "FAILED"]:
                    complete_jobs.append(job)

            loop.run_until_complete(
                aio_batch_get_logs(jobs=complete_jobs, config=aio_config)
            )

        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.stop()
            loop.close()
