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
Async AWS Batch
---------------

In testing this, it's able to run and monitor 100s of jobs from a laptop with modest
CPU/RAM resources.  It can recover from a crash by using a db-state, without re-running
the same jobs (by jobName).  It should be able to scale to run 1000s of jobs.

To run the example, the `notes.aio_aws.aio_aws_batch` module has a `main` that will run and
manage about 5 live batch jobs (very small `sleep` jobs that don't cost much to run).  The
job state is persisted to `aws_batch_jobs.json` and if it runs successfully, it will not
run the jobs again; the [TinyDB](https://tinydb.readthedocs.io/en/latest/intro.html) is
used to recover job state by `jobName`.

```
# setup the python virtualenv
# check the main details and modify for a preferred batch queue/CE and AWS region

$ ./notes/aio_aws/aio_aws_batch.py

Test async batch jobs

# wait a few minutes and watch the status messages
# it submits and monitors the jobs until they complete
# job status is saved and updated in `aws_batch_jobs.json`
# when it's done, run it again and see that nothing is re-submitted

$ ./notes/aio_aws/aio_aws_batch.py
```

If the job monitoring is halted for some reason (like `CNT-C`), it can recover from
the db-state, e.g.
```text
$ ./notes/aio_aws/aio_aws_batch.py

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
```

The batch data is a [TinyDB](https://tinydb.readthedocs.io/en/latest/intro.html) json file, e.g.
```
$ python
>>> import json
>>> with open('aws_batch_jobs.json') as job_file:
...     batch_data = json.load(job_file)
...
>>> len(batch_data['aws-batch-jobs'])
5
```

For the demo to run quickly, most of the module settings are fit for fast jobs.  For
much longer running jobs, there are functions that only submit jobs or check jobs and
the settings should be changed for monitoring jobs to only check every 10 or 20 minutes.

.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://www.mathewmarcus.com/blog/asynchronous-aws-api-requests-with-asyncio.html
"""

import asyncio
import re
from typing import Dict
from typing import List
from typing import Optional
from dataclasses import dataclass

import aiobotocore.client  # type: ignore
import aiobotocore.session  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore

from notes.aio_aws.aio_aws import AIO_AWS_SESSION
from notes.aio_aws.aio_aws import BATCH_STARTUP_PAUSE
from notes.aio_aws.aio_aws import CLIENT_SEMAPHORE
from notes.aio_aws.aio_aws import delay
from notes.aio_aws.aio_aws import jitter
from notes.aio_aws.aio_aws import MAX_JITTER
from notes.aio_aws.aio_aws import MAX_PAUSE
from notes.aio_aws.aio_aws import MIN_JITTER
from notes.aio_aws.aio_aws import MIN_PAUSE
from notes.aio_aws.aio_aws import response_success
from notes.aio_aws.logger import LOGGER

from tinydb import TinyDB, Query

TinyDB.DEFAULT_TABLE = "aws-batch-jobs"
TinyDB.DEFAULT_TABLE_KWARGS = {"cache_size": 0}
AWS_BATCH_DB_FILE = "aws_batch_jobs.json"
AWS_BATCH_DB = TinyDB(AWS_BATCH_DB_FILE)

# for reference:
# from tinydb.storages import MemoryStorage
# TinyDB.DEFAULT_STORAGE = MemoryStorage
# db = TinyDB(storage=MemoryStorage)

#: a semaphore to limit requests to the :py:const:`.AWS_BATCH_DB`
DB_SEMAPHORE = asyncio.Semaphore(1)


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
    job_definition: str
    job_queue: str
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
        return {
            "jobName": self.job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": self.job_definition,
            "containerOverrides": self.container_overrides,
            "dependsOn": self.depends_on,
        }

    @property
    def db_data(self):
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "job_queue": self.job_queue,
            "job_definition": self.job_definition,
            "job_description": self.job_description,
            "job_submission": self.job_submission,
            "container_overrides": self.container_overrides,
            "command": self.command,
            "depends_on": self.depends_on,
            "status": self.status,
            "num_tries": self.num_tries,
            "max_tries": self.max_tries,
        }

    def reset(self):
        """Clear the job_id and all related job data"""
        self.job_id = None
        self.job_description = None
        self.job_submission = None
        self.status = None


class AWSBatchDB:
    """
    AWS Batch job database

    .. seealso:: https://tinydb.readthedocs.io/en/latest/
    """

    @staticmethod
    def find_by_job_id(job_id: str) -> Optional[Dict]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the :py:meth:`AWSBatchJob.job_data` or None
        """
        if job_id:
            job_query = Query()
            db_result = AWS_BATCH_DB.get(job_query.job_id == job_id)
            if db_result:
                return db_result

    @staticmethod
    def find_by_job_name(job_name: str) -> List[Dict]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of :py:meth:`AWSBatchJob.job_data` or None
        """
        if job_name:
            job_query = Query()
            return AWS_BATCH_DB.search(job_query.job_name == job_name)

    @staticmethod
    def save(job: AWSBatchJob) -> bool:
        """
        Insert or update a job (if it has a job_id)

        :param job: an AWSBatchJob
        :return: bool
        """
        if job.job_id:
            job_query = Query()
            return AWS_BATCH_DB.upsert(job.db_data, job_query.job_id == job.job_id)
        else:
            LOGGER.error("FAIL to save job without job_id")


def parse_job_description(job_id: str, jobs: Dict) -> Optional[Dict]:
    """
    Extract a job description for ``job_id` from ``jobs``
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
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to submit a batch job

    :param job: A set of job parameters
    :param client: an aiobotocore client for AWS batch
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1
            try:
                params = job.params
                LOGGER.info("AWS Batch job params: %s", params)
                response = await client.submit_job(**params)
                LOGGER.info("AWS Batch job response: %s", response)
                if response_success(response):
                    job.job_id = response["jobId"]
                    job.job_submission = response
                    job.job_tries.append(job.job_id)
                    job.num_tries += 1
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < 10:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-submit", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job submission exceeded retries")


async def aio_batch_job_status(
    jobs: List[str],
    client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Optional[Dict]:
    """
    Asynchronous coroutine to issue a batch job description request

    :param jobs: a list of batch jobId
    :param client: an aiobotocore client for AWS batch
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a describe_jobs response
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1
            try:
                return await client.describe_jobs(jobs=jobs)
            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < 10:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-status", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job description exceeded retries")


async def aio_batch_job_terminate(
    job_id: str,
    reason: str,
    client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to terminate a batch job

    :param job_id: a batch jobId
    :param reason: a reason to terminate the job
    :param client: an aiobotocore client for AWS batch
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a job response
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1
            try:
                LOGGER.info("AWS Batch job to terminate: %s, %s", job_id, reason)
                response = await client.terminate_job(jobId=job_id, reason=reason)
                LOGGER.info("AWS Batch job response: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < 10:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("batch-job-terminate", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Batch job termination exceeded retries")


async def aio_batch_job_waiter(
    job: AWSBatchJob,
    client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Optional[Dict]:
    """
    Asynchronous coroutine to wait on a batch job

    job status identifiers are assumed to be:
    ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "FAILED", "SUCCEEDED"]

    :param job: a batch job
    :param client: an aiobotocore client for AWS batch
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a describe_jobs response for job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    # TODO: add timeouts here or on the loop?
    #       - get loop time 'now' and add timeout
    #       - compare loop time 'now' with timeout and break
    try:
        monitor_failures = 0
        while True:
            response = await aio_batch_job_status([job.job_id], client, sem)
            LOGGER.debug("AWS Batch job (%s) status: %s", job.job_id, response)

            job_desc = parse_job_description(job.job_id, response)
            if job_desc:

                job.job_description = job_desc
                job.status = job_desc["status"]
                LOGGER.info("AWS Batch job (%s) status: %s", job.job_id, job.status)

                if job_desc["status"] in ["FAILED", "SUCCEEDED"]:
                    return job_desc

                if job_desc["status"] in ["SUBMITTED", "PENDING", "RUNNABLE"]:
                    # Wait longer than regular pause to allow job startup
                    await delay(job.job_id, BATCH_STARTUP_PAUSE, BATCH_STARTUP_PAUSE * 2)

            else:
                LOGGER.warning("AWS Batch job (%s) has no description", job.job_id)
                monitor_failures += 1
                if monitor_failures > 5:
                    LOGGER.error("AWS Batch job (%s) failed to monitor job", job.job_id)
                    break

            # Allow the job status to be checked first, in case it is complete.
            await delay(job.job_id, MIN_PAUSE, MAX_PAUSE)

    except botocore.exceptions.ClientError as err:
        LOGGER.error("AWS Batch job (%s) failed to monitor job", job.job_id)
        raise


async def aio_batch_job_manager(
    job: AWSBatchJob,
    client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Optional[Dict]:
    """
    Asynchronous coroutine to manage a batch job

    :param job: a batch job spec
    :param client: an aiobotocore client for AWS batch
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a describe_jobs response for job_id when it's complete
    :raises: botocore.exceptions.ClientError
    """
    while job.num_tries < job.max_tries:

        if job.job_id is None:
            await aio_batch_job_submit(job, client, sem)
            async with DB_SEMAPHORE:
                AWSBatchDB.save(job)  # TODO: use async-db

        job_desc = await aio_batch_job_waiter(job, client, sem)
        async with DB_SEMAPHORE:
            AWSBatchDB.save(job)  # TODO: use async-db

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
                    LOGGER.warning("AWS Batch job (%s) SPOT failure, run retry.", job.job_id)
                    job.reset()
                    async with DB_SEMAPHORE:
                        AWSBatchDB.save(job)  # TODO: use async-db
                    continue
            except KeyError:
                pass

            return job_desc
    else:
        LOGGER.warning("AWS Batch job (%s) retries exceeded.", job.job_id)


if __name__ == "__main__":

    # pylint: disable=C0103
    aws_region = "us-west-2"

    loop = asyncio.get_event_loop()

    try:
        test_batch = True  # TODO: enable it for live tests
        if test_batch:

            # TODO: check if batch-CE exists or create it
            # TODO: check if batch-queue exists or create it
            # TODO: use async-db for state-machine management
            #       - maybe aioredis with both jobName and jobId keys
            #       - jobName key would just have a list of jobId values
            # TODO: get job logs

            print()
            print("Test async batch jobs")
            aio_client = AIO_AWS_SESSION.create_client("batch", region_name=aws_region)
            try:

                # this function can be used as a task callback
                def print_result(task: asyncio.Future):
                    task_result = task.result()
                    print(task_result)

                batch_tasks = []
                for i in range(5):
                    job_name = f"test-sleep-job-{i:04d}"
                    jobs_saved = AWSBatchDB.find_by_job_name(job_name)
                    if jobs_saved:
                        job_data = jobs_saved[0]  # TODO: find latest jobId?
                        batch_job = AWSBatchJob(**job_data)
                        LOGGER.info("AWS Batch job (%s) recovered from db", batch_job.job_name)
                        if batch_job.job_id and batch_job.status == "SUCCEEDED":
                            LOGGER.info(
                                "AWS Batch job (%s:%s) status: %s",
                                batch_job.job_name,
                                batch_job.job_id,
                                batch_job.status,
                            )
                            LOGGER.info(batch_job.job_description)
                            continue
                    else:
                        # use 'container_overrides' dict for more options
                        batch_job = AWSBatchJob(
                            job_name=job_name,
                            job_definition="batch-dev",
                            job_queue="batch-dev",
                            command=["/bin/bash", "-c", "sleep 1 && echo slept1"],
                        )

                    batch_task = loop.create_task(aio_batch_job_manager(batch_job, aio_client))
                    # # batch_task.add_done_callback(print_result)  # use callbacks
                    batch_tasks.append(batch_task)

                async def handle_as_completed(tasks):
                    for task in asyncio.as_completed(tasks):
                        task_result = await task
                        print(task_result)

                loop.run_until_complete(handle_as_completed(batch_tasks))
                # loop.run_until_complete(asyncio.wait(batch_tasks))  # use callbacks
            finally:
                loop.run_until_complete(aio_client.close())
    finally:
        loop.stop()
        loop.close()
