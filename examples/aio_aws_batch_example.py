#! /usr/bin/env python3

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

import asyncio
import os

from aio_aws.aio_aws_batch import AWSBatchConfig
from aio_aws.aio_aws_batch import aio_batch_get_logs
from aio_aws.aio_aws_batch import aio_batch_run_jobs
from aio_aws.aio_aws_batch_db import AioAWSBatchTinyDB
from aio_aws.aws_batch_models import AWSBatchJob

if __name__ == "__main__":

    print()
    print("Test async batch jobs")

    async def main():

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

        batch_jobs_db = AioAWSBatchTinyDB(
            jobs_db_file="/tmp/aio_batch_jobs.json",
            logs_db_file="/tmp/aio_batch_logs.json",
        )
        batch_jobs = await batch_jobs_db.jobs_recovery(jobs=batch_jobs)
        batch_jobs = await batch_jobs_db.jobs_to_run(jobs=batch_jobs)

        if batch_jobs:

            # for polling frequency of 5-20 seconds, with 30-60 second job starts
            aio_config = AWSBatchConfig(
                aws_region=os.getenv("AWS_DEFAULT_REGION", "us-west-2"),
                aio_batch_db=batch_jobs_db,
                max_pool_connections=1,
                min_pause=5,
                max_pause=20,
                start_pause=30,
            )

            await aio_batch_run_jobs(jobs=batch_jobs, config=aio_config)

            complete_jobs = []
            for job in batch_jobs:
                if job.status in ["SUCCEEDED", "FAILED"]:
                    complete_jobs.append(job)

            await aio_batch_get_logs(jobs=complete_jobs, config=aio_config)

    asyncio.run(main())
