import asyncio
import json
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import aioredis
from aio_aws.aio_aws_batch import AWSBatchJob
from aio_aws.logger import get_logger
from aio_aws.uuid_utils import valid_uuid4

LOGGER = get_logger(__name__)


@dataclass
class AioAWSBatchDB:
    """
    AWS Batch job databases

    The jobs and logs are kept in separate DBs so
    that performance on the jobs_db is not compromised
    by too much data from job logs.  This makes it possible
    to use the jobs_db without capturing logs as well.

    """

    redis_url: str = "redis://localhost"

    # TODO: use files to dump redis-db?
    # #: a file used for dumping jobs-db
    # jobs_db_file: str = "/tmp/aws_batch_jobs.json"
    # #: a file used for dumping logs-db
    # logs_db_file: str = "/tmp/aws_batch_logs.json"

    def __post_init__(self):

        # the jobs and logs are kept in separate DBs so
        # that performance on the jobs_db is not compromised
        # by too much data from job logs.

        # Redis client bound to pool of connections (auto-reconnecting).
        self.jobs_db = aioredis.from_url(
            self.redis_url,
            db=2,
            encoding="utf-8",
            decode_responses=True,
            max_connections=10,
        )
        self.logs_db = aioredis.from_url(
            self.redis_url,
            db=4,
            encoding="utf-8",
            decode_responses=True,
            max_connections=10,
        )

        # Lazy init for any asyncio instances
        self._db_sem = None

    @property
    def db_semaphore(self) -> asyncio.Semaphore:
        """A semaphore to limit requests to the db"""
        if self._db_sem is None:
            self._db_sem = asyncio.Semaphore(10)
        return self._db_sem

    @property
    async def db_alive(self) -> bool:
        try:
            assert await self.jobs_db.ping() is True
            assert await self.logs_db.ping() is True
        except AssertionError:
            return False
        return True

    @property
    async def db_info(self) -> Dict:
        info = {
            "jobs": await self.jobs_db.info(),
            "logs": await self.logs_db.info(),
        }
        LOGGER.debug("Using batch-jobs redis-db: %s", info)
        return info

    async def db_save(self):
        assert await self.jobs_db.bgsave() is True
        assert await self.logs_db.bgsave() is True

    async def find_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the job data or None
        """
        if job_id:
            job_json = await self.jobs_db.get(job_id)
            if job_json:
                return json.loads(job_json)

    async def find_by_job_name(self, job_name: str) -> List[Dict]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of dictionaries containing job data
        """
        jobs = []
        if job_name:
            job_ids = await self.jobs_db.get(job_name)
            if job_ids:
                # This should be a list of batch job-ids for a job name
                job_ids = set(json.loads(job_ids))
                for job_id in job_ids:
                    job_dict = await self.find_by_job_id(job_id)
                    if job_dict:
                        jobs.append(job_dict)
        return jobs

    async def find_latest_job_name(self, job_name: str) -> Optional[AWSBatchJob]:
        """
        Find the latest job matching the jobName,
        based on the "createdAt" time stamp.

        :param job_name: a batch jobName
        :return: the latest job record available
        """
        jobs_saved = await self.find_by_job_name(job_name)
        if jobs_saved:
            db_jobs = [AWSBatchJob(**job_dict) for job_dict in jobs_saved]
            db_jobs = sorted(db_jobs, key=lambda j: j.created)
            db_job = db_jobs[-1]
            return db_job

    async def remove_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Remove any job matching the jobId

        :param job_id: a batch jobId
        :return: a deleted document
        """
        if job_id:
            job_dict = await self.find_by_job_id(job_id)
            if job_dict:
                # TODO: use a transaction
                # First try to remove this job-id from the job-name
                job_name = job_dict["job_name"]
                job_ids = await self.jobs_db.get(job_name)
                if job_ids:
                    # This should be a list of batch job-ids for a job name
                    job_ids = set(json.loads(job_ids))
                    if job_id in job_ids:
                        job_ids.remove(job_id)
                        if job_ids:
                            await self.jobs_db.set(job_name, job_ids)
                        else:
                            # Don't save an empty set, delete the job-name entirely
                            await self.jobs_db.delete(job_name)

                await self.jobs_db.delete(job_id)
                return job_dict

    async def remove_by_job_name(self, job_name: str) -> Set[str]:
        """
        Remove any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a set of deleted job-ids
        """
        job_ids = set()
        if job_name:
            job_ids = await self.jobs_db.get(job_name)
            if not job_ids:
                job_ids = set()
            else:
                job_ids = set(json.loads(job_ids))
            await self.jobs_db.delete(*job_ids)
            await self.jobs_db.delete(job_name)
        return job_ids

    async def save_job(self, job: AWSBatchJob) -> Optional[str]:
        """
        Insert or update a job (if it has a job_id)

        :param job: an AWSBatchJob
        :return: the jobId if the job is saved
        """
        if job.job_id is None:
            LOGGER.error("FAIL to save_job without job_id")
            return

        # TODO: use a transaction
        job_json = json.dumps(job.db_data)
        response = await self.jobs_db.set(job.job_id, job_json)
        LOGGER.debug(response)
        if not response:
            msg = f"FAIL to save_job with job_id: {job.job_id}"
            LOGGER.error(msg)
            raise RuntimeError(msg)

        # Update the job-id set for the job-name
        job_ids = await self.jobs_db.get(job.job_name)
        if not job_ids:
            job_ids = set()
        else:
            job_ids = set(json.loads(job_ids))
        if job.job_id not in job_ids:
            job_ids.add(job.job_id)
            job_ids = json.dumps(list(job_ids))
            await self.jobs_db.set(job.job_name, job_ids)

        return job.job_id

    async def find_job_logs(self, job_id: str) -> Optional[Dict]:
        """
        Find job logs by job_id

        :param job_id: str
        :return: job logs document
        """
        if job_id:
            job_logs = await self.logs_db.get(job_id)
            if job_logs:
                return job_logs
        LOGGER.error("FAIL to find_job_logs with job_id: %s", job_id)

    async def save_job_logs(self, job: AWSBatchJob) -> List[int]:
        """
        Insert or update job logs (if the job has a job_id)

        :param job: an AWSBatchJob
        :return: a List[tinydb.database.Document.doc_id]
        """
        # TODO: update return data type and content
        if job.job_id:
            return await self.logs_db.set(job.job_id, job.db_logs_data)
        LOGGER.error("FAIL to save_job_logs")

    async def all_job_ids(self) -> Set[str]:
        """
        Find all jobId keys.
        """
        job_ids = set()
        async for key in self.jobs_db.scan_iter():
            if valid_uuid4(key):
                job_ids.add(key)
        return job_ids

    async def find_jobs_to_run(self) -> List[AWSBatchJob]:
        """
        Find all jobs that have not SUCCEEDED.
        """
        jobs_outstanding = []
        async for key in self.jobs_db.scan_iter():
            if valid_uuid4(key):
                job_dict = await self.find_by_job_id(key)
                LOGGER.info(
                    "AWS Batch job (%s:%s) has db status: %s",
                    job_dict["job_name"],
                    job_dict["job_id"],
                    job_dict["status"],
                )
                if job_dict["job_id"] and job_dict["status"] == "SUCCEEDED":
                    LOGGER.debug(job_dict["job_description"])
                    continue

                jobs_outstanding.append(AWSBatchJob(**job_dict))
        return jobs_outstanding

    async def jobs_to_run(self, jobs: List[AWSBatchJob]) -> List[AWSBatchJob]:
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

            # First check the job itself before checking the jobs_db
            if job.job_id and job.status == "SUCCEEDED":
                LOGGER.debug(job.job_description)
                continue

            db_job = await self.find_latest_job_name(job.job_name)
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

    async def jobs_recovery(self, jobs: List[AWSBatchJob]) -> List[AWSBatchJob]:
        """
        Use the job.job_name to find any jobs_db records to recover job data.
        """
        jobs_recovered = []
        for job in jobs:

            db_job = await self.find_latest_job_name(job.job_name)
            if db_job:
                LOGGER.info(
                    "AWS Batch job (%s:%s) recovered from db with status: %s",
                    db_job.job_name,
                    db_job.job_id,
                    db_job.status,
                )
                jobs_recovered.append(db_job)
            else:
                jobs_recovered.append(job)

        return jobs_recovered