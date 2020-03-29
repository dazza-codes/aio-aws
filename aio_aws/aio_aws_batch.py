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
    >>> with open('aws_batch_jobs.json') as job_file:
    ...     batch_data = json.load(job_file)
    ...
    >>> len(batch_data['aws-batch-jobs'])
    5

For the demo to run quickly, most of the module settings are fit for fast jobs.  For
much longer running jobs, there are functions that only submit jobs or check jobs and
the settings should be changed for monitoring jobs to only check every 10 or 20 minutes.

Monitoring Jobs
***************

The :py:func:`aio_aws.aio_aws_batch.aio_batch_job_manager` can submit a job, wait
for it to complete and retry if it fails on a SPOT termination. It saves the job status
using the :py:class:`aio_aws.aio_aws_batch.AWSBatchDB`.  The job manager
uses :py:class:`aio_aws.aio_aws_batch.AWSBatchConfig`, which has settings
to control the frequency of polling the job status.

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

    from aio_aws import AIO_AWS_SESSION
    from aio_aws.aio_aws_batch import AWSBatchConfig
    from aio_aws.aio_aws_batch import AWSBatchDB
    from aio_aws.aio_aws_batch import AWSBatchJob
    from aio_aws.aio_aws_batch import jobs_to_run
    from aio_aws.aio_aws_batch import aio_batch_run_jobs

    aws_region = "us-west-2"

    # For the `aws_region`:
    #       - create a `batch-dev` compute environment
    #       - create a `batch-dev` batch queue
    #       - create a `batch-dev` job definition using alpine:latest

    logs_command = "for a in `seq 1 10000`; do echo Hello $a; done"

    # Create a list of AWSBatchJob objects, which are simple wrappers
    # on AWS Batch job data.  These objects are simple, by design, to
    # allow them to be used in either synchronous or asynchronous code.
    batch_jobs = []
    for i in range(2):
        job_name = f"test-logs-job-{i:04d}"
        # use 'container_overrides' dict for more options
        batch_job = AWSBatchJob(
            job_name=job_name,
            job_definition="batch-dev",
            job_queue="batch-dev",
            command=["/bin/sh", "-c", logs_command],
        )
        batch_jobs.append(batch_job)

    # The AWSBatchDB wraps TinyDB data stores; it can be
    # used in either synchronous or asynchronous code.
    batch_jobs_db = AWSBatchDB(
        jobs_db_file="/tmp/aio_batch_jobs.json",
        logs_db_file="/tmp/aio_batch_logs.json",
    )

    batch_jobs = jobs_to_run(jobs=batch_jobs, jobs_db=batch_jobs_db)
    if batch_jobs:
        # for polling frequency of 10-30 seconds, with 30-60 second job starts
        aio_config = AWSBatchConfig(
            aws_region=aws_region, min_pause=10, max_pause=30, start_pause=30,
        )

        aio_batch_run_jobs(
            jobs=batch_jobs, jobs_db=batch_jobs_db, config=aio_config,
        )


.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://botocore.amazonaws.com/v1/documentation/api/latest/index.html

.. _aiobotocore: https://aiobotocore.readthedocs.io/en/latest/
.. _aiohttp: https://aiohttp.readthedocs.io/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _botocore: https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
.. _TinyDB: https://tinydb.readthedocs.io/en/latest/intro.html
"""

import asyncio
import re
from typing import Dict
from typing import List
from typing import Optional

import aiobotocore.client  # type: ignore
import aiobotocore.session  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore
import tinydb
from dataclasses import dataclass

from aio_aws import AIO_AWS_SESSION
from aio_aws import BATCH_STARTUP_PAUSE
from aio_aws import CLIENT_SEMAPHORE
from aio_aws import delay
from aio_aws import jitter
from aio_aws import MAX_JITTER
from aio_aws import MAX_PAUSE
from aio_aws import MIN_JITTER
from aio_aws import MIN_PAUSE
from aio_aws import response_success
from aio_aws.logger import LOGGER


@dataclass
class AWSBatchJobDescription:
    jobName: str = None
    jobId: str = None
    jobQueue: str = None
    status: str = None
    attempts: List[Dict] = None
    statusReason: str = None
    createdAt: int = None
    startedAt: int = None
    stoppedAt: int = None
    dependsOn: List[str] = None
    jobDefinition: str = None
    parameters: Dict = None
    container: Dict = None
    timeout: Dict = None


@dataclass
class AWSBatchJob:
    """
    AWS Batch job

    :param job_definition: A job job_definition.
    :param job_queue: A batch queue.
    :param command: A container command.
    :param depends_on: list of dictionaries like:
        .. code-block::

            [
              {'jobId': 'abc123', ['type': 'N_TO_N' | 'SEQUENTIAL'] },
            ]

        type is optional, used only for job arrays
    :param container_overrides: a dictionary of container container_overrides.
        Overrides include 'vcpus', 'memory', 'instanceType',
        'environment', and 'resourceRequirements'.

    .. seealso::
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    """

    STATUSES = [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]

    job_name: str
    job_queue: str
    job_definition: str
    command: List[str] = None
    depends_on: List[Dict] = None
    container_overrides: Dict = None
    job_id: Optional[str] = None
    status: Optional[str] = None
    job_tries: List[str] = None
    num_tries: int = 0
    max_tries: int = 4
    job_submission: Optional[Dict] = None
    job_description: Optional[Dict] = None
    logs: Optional[List[Dict]] = None

    def __post_init__(self):

        self.job_name = self.job_name[:128]

        if self.job_tries is None:
            self.job_tries = []

        if self.depends_on is None:
            self.depends_on = []

        if self.container_overrides is None:
            self.container_overrides = {}

        if self.command:
            self.container_overrides.update({"command": self.command})

    @property
    def params(self):
        """AWS Batch parameters for job submission"""
        return {
            "jobName": self.job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": self.job_definition,
            "containerOverrides": self.container_overrides,
            "dependsOn": self.depends_on,
        }

    @property
    def db_data(self) -> Dict:
        """AWS Batch job data for state machine persistence"""
        # The job.logs are NOT included here, by design.
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "job_queue": self.job_queue,
            "job_definition": self.job_definition,
            "job_submission": self.job_submission,
            "job_description": self.job_description,
            "container_overrides": self.container_overrides,
            "command": self.command,
            "depends_on": self.depends_on,
            "status": self.status,
            "job_tries": self.job_tries,
            "num_tries": self.num_tries,
            "max_tries": self.max_tries,
        }

    @property
    def db_logs_data(self) -> Optional[Dict]:
        """AWS Batch job with logs persistence"""
        if self.logs:
            data = self.db_data
            data["logs"] = self.logs
            return data

    def reset(self):
        """Clear the job_id and all related job data"""
        self.job_id = None
        self.job_description = None
        self.job_submission = None
        self.status = None
        self.logs = None

    @property
    def created(self) -> Optional[int]:
        if self.job_description:
            return self.job_description["createdAt"]

    @property
    def started(self) -> Optional[int]:
        if self.job_description:
            return self.job_description["startedAt"]

    @property
    def stopped(self) -> Optional[int]:
        if self.job_description:
            return self.job_description["stoppedAt"]

    @property
    def elapsed(self) -> Optional[int]:
        created = self.created
        stopped = self.stopped
        if stopped and created:
            return stopped - created

    @property
    def runtime(self) -> Optional[int]:
        started = self.started
        stopped = self.stopped
        if started and stopped:
            return stopped - started

    @property
    def spinup(self) -> Optional[int]:
        created = self.created
        started = self.started
        if started and created:
            return started - created


@dataclass
class AWSBatchDB:
    """
    AWS Batch job databases

    The jobs and logs are kept in separate DBs so
    that performance on the jobs_db is not compromised
    by too much data from job logs.  This makes it possible
    to use the jobs_db without capturing logs as well.

    .. seealso:: https://tinydb.readthedocs.io/en/latest/
    """

    #: a file used for :py:class::`TinyDB(jobs_db_file)`
    jobs_db_file: str = "/tmp/aws_batch_jobs.json"

    #: a file used for :py:class::`TinyDB(logs_db_file)`
    logs_db_file: str = "/tmp/aws_batch_logs.json"

    #: a semaphore to limit requests to the db
    DB_SEMAPHORE = asyncio.Semaphore(1)

    def __post_init__(self):

        tinydb.TinyDB.DEFAULT_TABLE = "aws-batch-jobs"
        tinydb.TinyDB.DEFAULT_TABLE_KWARGS = {"cache_size": 0}

        # the jobs and logs are kept in separate DBs so
        # that performance on the jobs_db is not compromised
        # by too much data from job logs.
        self.jobs_db = tinydb.TinyDB(self.jobs_db_file)
        self.logs_db = tinydb.TinyDB(self.logs_db_file)
        LOGGER.info("Using batch-jobs-db file: %s", self.jobs_db_file)
        LOGGER.info("Using batch-logs-db file: %s", self.logs_db_file)

    def find_by_job_id(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the :py:meth:`AWSBatchJob.job_data` or None
        """
        if job_id:
            job_query = tinydb.Query()
            db_result = self.jobs_db.get(job_query.job_id == job_id)
            if db_result:
                return db_result

    def find_by_job_name(self, job_name: str) -> List[tinydb.database.Document]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of documents containing :py:meth:`AWSBatchJob.job_data`
        """
        if job_name:
            job_query = tinydb.Query()
            return self.jobs_db.search(job_query.job_name == job_name)

    def find_latest_job_name(self, job_name: str) -> AWSBatchJob:
        """
        Find the latest job matching the jobName,
        based on the "createdAt" time stamp.

        :param job_name: a batch jobName
        :return: the latest job record available
        """
        jobs_saved = self.find_by_job_name(job_name)
        if jobs_saved:
            db_jobs = [AWSBatchJob(**job_doc) for job_doc in jobs_saved]
            db_jobs = sorted(db_jobs, key=lambda j: j.created)
            db_job = db_jobs[-1]
            return db_job

    def remove_by_job_id(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Remove any job matching the jobId

        :param job_id: a batch jobId
        :return: a deleted document
        """
        if job_id:
            job = self.find_by_job_id(job_id)
            if job:
                self.jobs_db.remove(doc_ids=[job.doc_id])
                return job

    def remove_by_job_name(self, job_name: str) -> List[tinydb.database.Document]:
        """
        Remove any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of deleted documents
        """
        if job_name:
            jobs_found = self.find_by_job_name(job_name)
            if jobs_found:
                docs = [doc.doc_id for doc in jobs_found]
                self.jobs_db.remove(doc_ids=docs)
            return jobs_found

    def save_job(self, job: AWSBatchJob) -> List[int]:
        """
        Insert or update a job (if it has a job_id)

        :param job: an AWSBatchJob
        :return: a List[tinydb.database.Document.doc_id]
        """
        if job.job_id:
            job_query = tinydb.Query()
            return self.jobs_db.upsert(job.db_data, job_query.job_id == job.job_id)
        else:
            LOGGER.error("FAIL to save_job without job_id")

    def find_job_logs(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Find job logs by job_id

        :param job_id: str
        :return: a tinydb.database.Document with job logs
        """
        if job_id:
            log_query = tinydb.Query()
            db_result = self.logs_db.get(log_query.job_id == job_id)
            if db_result:
                return db_result
        else:
            LOGGER.error("FAIL to find_job_logs without job_id")

    def save_job_logs(self, job: AWSBatchJob) -> List[int]:
        """
        Insert or update job logs (if the job has a job_id)

        :param job: an AWSBatchJob
        :return: a List[tinydb.database.Document.doc_id]
        """
        if job.job_id:
            log_query = tinydb.Query()
            return self.logs_db.upsert(job.db_logs_data, log_query.job_id == job.job_id)
        else:
            LOGGER.error("FAIL to save_job_logs without job_id")

    def find_jobs_to_run(self) -> List[AWSBatchJob]:
        """
        Find all jobs that have not SUCCEEDED.  Note that any jobs handled
        by the job-manager will not re-run if they have a job.job_id, those
        jobs will be monitored until complete.
        """
        jobs = [AWSBatchJob(**job_doc) for job_doc in self.jobs_db.all()]
        jobs_outstanding = []
        for job in jobs:
            LOGGER.info(
                "AWS Batch job (%s:%s) has db status: %s", job.job_name, job.job_id, job.status,
            )
            if job.job_id and job.status == "SUCCEEDED":
                LOGGER.debug(job.job_description)
                continue

            jobs_outstanding.append(job)

        return jobs_outstanding


def jobs_to_run(jobs: List[AWSBatchJob], jobs_db: AWSBatchDB,) -> List[AWSBatchJob]:
    """
    Use the job.job_name to find any jobs_db records and check the job status
    on the latest submission of any job matching the job-name.  For any job that
    has a matching job-name in the jobs_db, check whether the job has already run and
    SUCCEEDED.  Any jobs that have already run and SUCCEEDED are not returned.

    Return all jobs that have not run yet or have not SUCCEEDED.  Note that any
    jobs subsequently input to the job-manager will not re-run if they already
    have a job.job_id, those jobs will be monitored until complete.  To re-run
    jobs that are returned from this filter function, first use `job.reset()`
    before passing the jobs to the job-manager.

    .. note::
        The input jobs are not modified in any way in this function, i.e. there
        are no updates from the jobs_db applied to the input jobs.

        To recover jobs from the jobs_db, use
        - :py:meth:`AWSBatchDB.find_jobs_to_run()`
        - :py:meth:`AWSBatchDB.jobs_db.all()`
    """
    jobs_outstanding = []
    for job in jobs:
        # Avoid side-effects in this function:
        # - treat the job as a read-only object
        # - treat any jobs_db records as read-only objects
        # - if a job is not saved, don't save it to jobs_db
        # - if a job exists in jobs_db, don't update the job data

        # First check the job itself before checking the jobs_db
        if job.job_id and job.status == "SUCCEEDED":
            LOGGER.debug(job.job_description)
            continue

        db_job = jobs_db.find_latest_job_name(job.job_name)
        if db_job:
            LOGGER.info(
                "AWS Batch job (%s:%s) has db status: %s",
                db_job.job_name,
                db_job.job_id,
                db_job.status,
            )
            if db_job.job_id and db_job.status == "SUCCEEDED":
                LOGGER.debug(db_job.job_description)
                continue

        jobs_outstanding.append(job)

    return jobs_outstanding


@dataclass
class AWSBatchConfig:
    #: an optional AWS region name
    aws_region: str = None
    #: a number of retries for an AWS client request/response
    retries: int = 5
    #: a batch job startup pause, ``random.uniform(start_pause, start_pause * 2)``;
    #: this applies when the job status is in ["SUBMITTED", "PENDING", "RUNNABLE"]
    start_pause: float = BATCH_STARTUP_PAUSE
    #: defines an asyncio.sleep for ``random.uniform(min_pause, max_pause)``
    min_pause: float = MIN_PAUSE
    #: defines an asyncio.sleep for ``random.uniform(min_pause, max_pause)``
    max_pause: float = MAX_PAUSE
    #: defines an asyncio.sleep for ``random.uniform(min_jitter, max_jitter)``
    min_jitter: float = MIN_JITTER
    #: defines an asyncio.sleep for ``random.uniform(min_jitter, max_jitter)``
    max_jitter: float = MAX_JITTER
    #: an asyncio.Semaphore to limit the number of concurrent client requests
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE
    #: an aiobotocore.session.AioSession
    session: aiobotocore.session.AioSession = AIO_AWS_SESSION

    def get_batch_client(self) -> aiobotocore.client.AioBaseClient:
        return self.session.create_client("batch", region_name=self.aws_region)

    def get_logs_client(self) -> aiobotocore.client.AioBaseClient:
        return self.session.create_client("logs", region_name=self.aws_region)


#: a default AWSBatchConfig
AWS_BATCH_CONFIG = AWSBatchConfig()


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
    job: AWSBatchJob,
    client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
) -> Dict:
    """
    Asynchronous coroutine to submit a batch job

    :param job: A set of job parameters
    :param client: an aiobotocore client for AWS batch
    :param config: settings for task pauses between retries
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    async with config.sem:
        tries = 0
        while tries < config.retries:
            tries += 1
            try:
                params = job.params
                LOGGER.info("AWS Batch job params: %s", params)
                response = await client.submit_job(**params)
                LOGGER.info("AWS Batch job response: %s", response)
                # TODO: are there some submission failures that could be recovered here?
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-submit", config.min_jitter, config.max_jitter)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job submission exceeded retries")


async def aio_batch_job_status(
    jobs: List[str],
    client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
) -> Optional[Dict]:
    """
    Asynchronous coroutine to issue a batch job description request

    :param jobs: a list of batch jobId
    :param client: an aiobotocore client for AWS batch
    :param config: settings for task pauses between retries
    :return: a describe_jobs response
    :raises: botocore.exceptions.ClientError
    """
    async with config.sem:
        tries = 0
        while tries < config.retries:
            tries += 1
            try:
                return await client.describe_jobs(jobs=jobs)
            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-status", config.min_jitter, config.max_jitter)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job description exceeded retries")


async def aio_batch_job_logs(
    job: AWSBatchJob,
    logs_client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
) -> Optional[List[Dict]]:
    """
    Asynchronous coroutine to get logs for a batch job log stream.  All
    the events available for a log stream are collected and returned.  The
    AWS Cloud Watch log streams can have delays, don't expect near real-time
    values in these data.

    :param job: A set of job parameters
    :param logs_client: an aiobotocore client for AWS CloudWatchLogs
    :param config: settings for task pauses between retries
    :return: a list of all the log events from get_log_events responses
    :raises: botocore.exceptions.ClientError

    .. seealso::
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html
    """
    if not job.job_description:
        LOGGER.warning("AWS Batch job has no description, cannot fetch logs")
        return

    log_stream_name = job.job_description.get("container", {}).get("logStreamName")
    if not log_stream_name:
        LOGGER.warning("The log_stream_name is not defined")
        return

    async with config.sem:
        tries = 0
        while tries < config.retries:
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

                return log_events

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-logs", config.min_jitter, config.max_jitter)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job logs exceeded retries")


async def aio_batch_job_terminate(
    job_id: str,
    reason: str,
    client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
) -> Dict:
    """
    Asynchronous coroutine to terminate a batch job

    :param job_id: a batch jobId
    :param reason: a reason to terminate the job
    :param client: an aiobotocore client for AWS batch
    :param config: settings for task pauses between retries
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    async with config.sem:
        tries = 0
        while tries < config.retries:
            tries += 1
            try:
                LOGGER.info("AWS Batch job to terminate: %s, %s", job_id, reason)
                response = await client.terminate_job(jobId=job_id, reason=reason)
                LOGGER.info("AWS Batch job response: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < config.retries:
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
    job: AWSBatchJob,
    client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
) -> Optional[Dict]:
    """
    Asynchronous coroutine to wait on a batch job.  There is no explict
    timeout on a job waiter, it depends on setting a timeout on the batch
    job definition.  The job waiter exits when the batch job status is
    either "SUCCEEDED" or "FAILED".

    job status identifiers are assumed to be:
    ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "FAILED", "SUCCEEDED"]

    :param job: a batch job
    :param client: an aiobotocore client for AWS batch
    :param config: settings for task pauses between retries
    :return: a describe_jobs response for job.job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    try:
        monitor_failures = 0
        while True:
            response = await aio_batch_job_status([job.job_id], client, config)
            LOGGER.debug("AWS Batch job (%s:%s) status: %s", job.job_name, job.job_id, response)

            job_desc = parse_job_description(job.job_id, response)
            if job_desc:

                LOGGER.info(
                    "AWS Batch job (%s:%s) status: %s",
                    job.job_name,
                    job.job_id,
                    job_desc["status"],
                )

                if job_desc["status"] in ["FAILED", "SUCCEEDED"]:
                    return job_desc

                if job_desc["status"] in ["SUBMITTED", "PENDING", "RUNNABLE"]:
                    # Wait longer than regular pause to allow job startup
                    await delay(job.job_name, config.start_pause, config.start_pause * 2)

            else:
                LOGGER.warning(
                    "AWS Batch job (%s:%s) has no description", job.job_name, job.job_id
                )
                monitor_failures += 1
                if monitor_failures > config.retries:
                    LOGGER.error(
                        "AWS Batch job (%s:%s) failed to monitor job", job.job_name, job.job_id
                    )
                    break

            # Allow the job status to be checked first, in case it is complete.
            await delay(job.job_name, config.min_pause, config.max_pause)

    except botocore.exceptions.ClientError as err:
        LOGGER.error("AWS Batch job (%s:%s) failed to monitor job", job.job_name, job.job_id)
        raise


async def aio_batch_job_manager(
    job: AWSBatchJob,
    jobs_db: AWSBatchDB,
    batch_client: aiobotocore.client.AioBaseClient,
    logs_client: aiobotocore.client.AioBaseClient,
    config: AWSBatchConfig = AWS_BATCH_CONFIG,
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
    :param jobs_db: an AWSBatchDB
    :param batch_client: an aiobotocore client for AWS batch
    :param logs_client: an aiobotocore client for AWS CloudWatchLogs
    :param config: settings for task pauses between retries
    :return: a describe_jobs response for job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    while job.num_tries < job.max_tries:

        if job.job_id is None:
            response = await aio_batch_job_submit(job, client=batch_client, config=config)
            if response_success(response):
                job.job_id = response["jobId"]
                job.job_submission = response
                job.job_tries.append(job.job_id)
                job.num_tries += 1
                async with jobs_db.DB_SEMAPHORE:
                    jobs_db.save_job(job)  # TODO: use async-db
            else:
                LOGGER.error(
                    "AWS Batch job (%s:%s) submission failure.", job.job_name, job.job_id,
                )
                return

        job_desc = await aio_batch_job_waiter(job, client=batch_client, config=config)
        job.job_description = job_desc
        job.status = job_desc["status"]
        async with jobs_db.DB_SEMAPHORE:
            jobs_db.save_job(job)  # TODO: use async-db

        try:
            # Since cloud watch logs have a delay in gathering the job logs, pause first
            await delay(
                "batch-job-logs-pause", min_pause=config.min_pause, max_pause=config.max_pause
            )
            log_events = await aio_batch_job_logs(job, logs_client=logs_client, config=config)
            if log_events:
                job.logs = log_events
                async with jobs_db.DB_SEMAPHORE:
                    jobs_db.save_job_logs(job)
        except:
            # Allow job logs to fail on any failure
            pass

        if job_desc["status"] == "SUCCEEDED":
            return job_desc

        if job_desc["status"] == "FAILED":
            try:
                # SPOT failure requires a retry, it usually has
                # statusReason: Host EC2 (instance {instanceId}) terminated
                #
                # Some common reasons with no retry behavior:
                # - user initiated job termination with a custom reason
                # - "Dependent Job failed" - unknown upstream failure
                # - "Essential container in task exited" - container error
                reason = job_desc["statusReason"]
                if re.match(r"Host EC2.*terminated", reason):
                    LOGGER.warning(
                        "AWS Batch job (%s:%s) SPOT failure, run retry.",
                        job.job_name,
                        job.job_id,
                    )
                    job.reset()
                    async with jobs_db.DB_SEMAPHORE:
                        jobs_db.save_job(job)  # TODO: use async-db
                    continue
            except KeyError:
                pass

            return job_desc
    else:
        LOGGER.warning("AWS Batch job (%s:%s) retries exceeded.", job.job_name, job.job_id)


def aio_batch_run_jobs(
    jobs: List[AWSBatchJob], jobs_db: AWSBatchDB, config: AWSBatchConfig,
):
    loop = asyncio.get_event_loop()
    batch_client = config.get_batch_client()
    logs_client = config.get_logs_client()
    try:
        batch_tasks = [
            loop.create_task(
                aio_batch_job_manager(
                    job=job,
                    jobs_db=jobs_db,
                    batch_client=batch_client,
                    logs_client=logs_client,
                    config=config,
                )
            )
            for job in jobs
        ]

        async def handle_jobs_as_completed(tasks):
            for task in asyncio.as_completed(tasks):
                job_desc = await task
                LOGGER.debug(job_desc)

        # TODO: handle tasks when the event loop is already running
        # TODO: consider https://github.com/erdewit/nest_asyncio
        loop.run_until_complete(handle_jobs_as_completed(batch_tasks))

    finally:
        # TODO: close clients when the event loop is already running
        loop.run_until_complete(batch_client.close())
        loop.run_until_complete(logs_client.close())
        # TODO: maybe don't close the loop if it was already running
        loop.stop()
        loop.close()


if __name__ == "__main__":

    print()
    print("Test async batch jobs")

    logs_command = "for a in `seq 1 10000`; do echo Hello $a; done"

    batch_jobs = []
    for i in range(2):
        job_name = f"test-logs-job-{i:04d}"
        # use 'container_overrides' dict for more options
        batch_job = AWSBatchJob(
            job_name=job_name,
            job_definition="batch-dev",
            job_queue="batch-dev",
            command=["/bin/sh", "-c", logs_command],
        )
        batch_jobs.append(batch_job)

    batch_jobs_db = AWSBatchDB(
        jobs_db_file="/tmp/aio_batch_jobs.json", logs_db_file="/tmp/aio_batch_logs.json",
    )

    batch_jobs = jobs_to_run(jobs=batch_jobs, jobs_db=batch_jobs_db)
    if batch_jobs:
        # for polling frequency of 10-30 seconds, with 30-60 second job starts
        aio_config = AWSBatchConfig(
            aws_region="us-west-2", min_pause=10, max_pause=30, start_pause=30,
        )

        aio_batch_run_jobs(
            jobs=batch_jobs, jobs_db=batch_jobs_db, config=aio_config,
        )
