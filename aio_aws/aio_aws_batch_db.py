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
AioAWS Batch-DB
===============

"""

import abc
import asyncio
import json
from collections import Counter
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import tinydb

from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.aws_batch_models import AWSBatchJobStates
from aio_aws.logger import get_logger
from aio_aws.uuid_utils import valid_uuid4

LOGGER = get_logger(__name__)


@dataclass
class AioAWSBatchDB(abc.ABC):
    """
    Abstract Base Class for AWS Batch job databases
    """

    @abc.abstractmethod
    async def all_jobs(self) -> List[AWSBatchJob]:
        """
        Collect all jobs.

        Warning: this could exceed memory, try to use
        the :py:meth:`gen_all_jobs` wherever possible.
        """
        pass

    @abc.abstractmethod
    async def gen_all_jobs(self) -> AsyncIterator[AWSBatchJob]:
        """
        Generate all jobs.
        """
        yield

    @abc.abstractmethod
    async def all_job_ids(self) -> Set[str]:
        """
        Collect all jobIds.
        """
        pass

    @abc.abstractmethod
    async def gen_job_ids(self) -> AsyncIterator[str]:
        """
        Generate all jobIds.
        """
        yield

    @abc.abstractmethod
    async def count_by_job_status(self) -> Counter:
        """
        Count all jobs by jobStatus

        :return: a Counter of jobs by job status (could contain multiple
            entries for the same jobName, if it is run more than once)
        """
        pass

    @abc.abstractmethod
    async def group_by_job_status(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        Group all jobId by jobStatus

        :return: a dictionary of job info by job status (could contain multiple
            entries with the same jobName if it is run multiple times);
            the dictionary values contain a list of job information tuples, e.g.

            .. code-block::

                { status: [ (job.job_id, job.job_name, job.status), ] }

        """
        pass

    @abc.abstractmethod
    async def find_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the job data or None
        """
        pass

    @abc.abstractmethod
    async def find_by_job_name(self, job_name: str) -> List[Dict]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of dictionaries containing job data
        """
        pass

    @abc.abstractmethod
    async def find_latest_job_name(self, job_name: str) -> Optional[AWSBatchJob]:
        """
        Find the latest job matching the jobName,
        based on the "createdAt" time stamp.

        :param job_name: a batch jobName
        :return: the latest job record available
        """
        pass

    @abc.abstractmethod
    async def find_by_job_status(self, job_states: List[str]) -> List[AWSBatchJob]:
        """
        Find any jobs matching any job status values

        :param job_states: a list of valid job states
        :return: a list of AWSBatchJob (could contain multiple
            entries with the same jobName if it is run multiple times
            with any jobStatus in the jobStatus values)
        """
        pass

    @abc.abstractmethod
    async def remove_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Remove any job matching the jobId

        :param job_id: a batch jobId
        :return: a deleted document
        """
        pass

    @abc.abstractmethod
    async def remove_by_job_name(self, job_name: str) -> Set[str]:
        """
        Remove any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a set of deleted job-ids
        """
        pass

    @abc.abstractmethod
    async def save_job(self, job: AWSBatchJob) -> Optional[str]:
        """
        Insert or update a job (if it has a job_id)

        :param job: an AWSBatchJob
        :return: the jobId if the job is saved
        """
        pass

    @abc.abstractmethod
    async def find_job_logs(self, job_id: str) -> Optional[Dict]:
        """
        Find job logs by job_id

        :param job_id: str
        :return: job logs document
        """
        pass

    @abc.abstractmethod
    async def save_job_logs(self, job: AWSBatchJob) -> List[int]:
        """
        Insert or update job logs (if the job has a job_id)

        :param job: an AWSBatchJob
        :return: a List[tinydb.database.Document.doc_id]
        """
        pass

    @abc.abstractmethod
    async def find_jobs_to_run(self) -> List[AWSBatchJob]:
        """
        Find all jobs that have not SUCCEEDED.
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    async def jobs_recovery(self, jobs: List[AWSBatchJob]) -> List[AWSBatchJob]:
        """
        Use the job.job_name to find any jobs_db records to recover job data.
        """
        pass


@dataclass
class AioAWSBatchRedisDB(AioAWSBatchDB):
    """
    AWS Batch job databases - aioredis implementation

    The jobs and logs are kept in separate DBs so
    that performance on the jobs_db is not compromised
    by too much data from job logs.  This makes it possible
    to use the jobs_db without capturing logs as well.

    """

    redis_url: str = "redis://127.0.0.1:6379"

    # TODO: use files to dump redis-db?
    # #: a file used for dumping jobs-db
    # jobs_db_file: str = "/tmp/aws_batch_jobs.json"
    # #: a file used for dumping logs-db
    # logs_db_file: str = "/tmp/aws_batch_logs.json"

    def __post_init__(self):

        # the jobs and logs are kept in separate DBs so
        # that performance on the jobs_db is not compromised
        # by too much data from job logs.

        # Lazy init for any asyncio instances
        self._db_sem = None

    @property
    async def jobs_db(self):
        # lazy load this optional dependency
        import aioredis

        # need to instantiate redis-connection on-demand to avoid
        # Future <Future pending> attached to a different loop
        # TODO: lean how to optimize connections to auto-close them

        async with self.db_semaphore:
            return aioredis.from_url(
                self.redis_url,
                db=2,
                encoding="utf-8",
                decode_responses=True,
                max_connections=1,
            )

    @property
    async def logs_db(self):
        # lazy load this optional dependency
        import aioredis

        # need to instantiate redis-connection on-demand to avoid
        # Future <Future pending> attached to a different loop
        async with self.db_semaphore:
            return aioredis.from_url(
                self.redis_url,
                db=4,
                encoding="utf-8",
                decode_responses=True,
                max_connections=1,
            )

    @property
    def db_semaphore(self) -> asyncio.Semaphore:
        """A semaphore to limit requests to the db"""
        if self._db_sem is None:
            self._db_sem = asyncio.Semaphore(100)
        return self._db_sem

    @classmethod
    async def _is_db_alive(cls, db) -> bool:
        try:
            assert await db.ping() is True
            return True
        except AssertionError:
            return False

    @property
    async def jobs_db_alive(self) -> "Redis":
        for tries in range(5):
            jobs_db = await self.jobs_db
            db_alive = await self._is_db_alive(jobs_db)
            if db_alive:
                return jobs_db
            else:
                await asyncio.sleep(0.0001, 0.005)

    @property
    async def logs_db_alive(self) -> "Redis":
        for tries in range(5):
            logs_db = await self.logs_db
            db_alive = await self._is_db_alive(logs_db)
            if db_alive:
                return logs_db
            else:
                await asyncio.sleep(0.0001, 0.005)

    @property
    async def db_alive(self) -> bool:
        try:
            assert await self.jobs_db_alive
            assert await self.logs_db_alive
            return True
        except AssertionError:
            return False

    @property
    async def db_info(self) -> Dict:
        jobs_db = await self.jobs_db_alive
        logs_db = await self.logs_db_alive
        info = {
            "jobs": await jobs_db.info(),
            "logs": await logs_db.info(),
        }
        LOGGER.debug("Using batch-jobs redis-db: %s", info)
        return info

    # async def db_close(self):
    #     await self.jobs_db.close()
    #     await self.logs_db.close()

    async def db_save(self):
        jobs_db = await self.jobs_db_alive
        logs_db = await self.logs_db_alive
        assert await jobs_db.bgsave() is True
        assert await logs_db.bgsave() is True

    @classmethod
    async def _find_by_job_id(cls, job_id: str, jobs_db: "Redis") -> Dict:
        """
        Private method to find any job matching the jobId

        :param job_id: a batch jobName
        :param jobs_db: an async Redis
        :return: the job data or None
        """
        if job_id:
            job_json = await jobs_db.get(job_id)
            if job_json:
                return json.loads(job_json)

    @classmethod
    async def _find_by_job_name(cls, job_name: str, jobs_db: "Redis") -> List[Dict]:
        """
        Private method to find any jobs matching the jobName

        :param job_name: a batch jobName
        :param jobs_db: an async Redis
        :return: a list of dictionaries containing job data
        """
        jobs = []
        if job_name:
            job_ids = await jobs_db.get(job_name)
            if job_ids:
                # This should be a list of batch job-ids for a job name
                job_ids = set(json.loads(job_ids))
                for job_id in job_ids:
                    job_json = await jobs_db.get(job_id)
                    if job_json:
                        job_dict = json.loads(job_json)
                        jobs.append(job_dict)
        return jobs

    async def all_job_ids(self) -> Set[str]:
        """
        Collect all jobIds.
        """
        job_ids = set()
        async for key in self.gen_job_ids():
            job_ids.add(key)
        return job_ids

    async def gen_job_ids(self) -> AsyncIterator[str]:
        """
        Generate all jobIds.
        """
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                yield key

    async def all_jobs(self) -> List[AWSBatchJob]:
        """
        Collect all jobs.

        Warning: this could exceed memory, try to use
        the :py:meth:`gen_all_jobs` wherever possible.
        """
        jobs = []
        async for j in self.gen_all_jobs():
            jobs.append(j)
        return jobs

    async def gen_all_jobs(self) -> AsyncIterator[AWSBatchJob]:
        """
        Generate all jobs.
        """
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                # The key is a jobId that conforms to UUID
                job_json = await jobs_db.get(key)
                if job_json:
                    job_dict = json.loads(job_json)
                    yield AWSBatchJob(**job_dict)

    async def count_by_job_status(self) -> Counter:
        """
        Count all jobs by jobStatus

        :return: a Counter of jobs by job status (could contain multiple
            entries for the same jobName, if it is run more than once)
        """
        counter = Counter()
        for job_status in AWSBatchJobStates:
            counter[job_status.name] = 0
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                # The key is a jobId that conforms to UUID
                job_json = await jobs_db.get(key)
                if job_json:
                    job_dict = json.loads(job_json)
                    job = AWSBatchJob(**job_dict)
                    counter[job.status] += 1
        return counter

    async def group_by_job_status(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        Group all jobId by jobStatus

        :return: a dictionary of job info by job status (could contain multiple
            entries with the same jobName if it is run multiple times);
            the dictionary values contain a list of job information tuples, e.g.

            .. code-block::

                { status: [ (job.job_id, job.job_name, job.status), ] }

        """
        groups = defaultdict(list)
        for job_status in AWSBatchJobStates:
            groups[job_status.name] = []
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                # The key is a jobId that conforms to UUID
                job_json = await jobs_db.get(key)
                if job_json:
                    job_dict = json.loads(job_json)
                    job = AWSBatchJob(**job_dict)
                    job_info = (job.job_id, job.job_name, job.status)
                    groups[job.status].append(job_info)
        return groups

    async def find_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the job data or None
        """
        if job_id:
            jobs_db = await self.jobs_db_alive
            return await self._find_by_job_id(job_id=job_id, jobs_db=jobs_db)

    async def find_by_job_name(self, job_name: str) -> List[Dict]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of dictionaries containing job data
        """
        if job_name:
            jobs_db = await self.jobs_db_alive
            return await self._find_by_job_name(job_name=job_name, jobs_db=jobs_db)

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
            db_jobs = sorted(db_jobs, key=lambda j: j.created or j.submitted)
            db_job = db_jobs[-1]
            return db_job

    async def find_by_job_status(self, job_states: List[str]) -> List[AWSBatchJob]:
        """
        Find any jobs matching any jobStatus values

        :param job_states: a list of valid job status values
        :return: a list of AWSBatchJob (could contain multiple
            entries with the same jobName if it is run multiple times
            with any jobStatus in the jobStatus values)
        """
        for status in job_states:
            assert status in AWSBatchJob.STATES

        jobs = []
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                # The key is a jobId that conforms to UUID
                job_json = await jobs_db.get(key)
                if job_json:
                    job_dict = json.loads(job_json)
                    job = AWSBatchJob(**job_dict)
                    if job.status in job_states:
                        jobs.append(job)
        return jobs

    async def remove_by_job_id(self, job_id: str) -> Optional[Dict]:
        """
        Remove any job matching the jobId

        :param job_id: a batch jobId
        :return: a deleted document
        """
        # TODO: use a transaction
        if job_id:
            jobs_db = await self.jobs_db_alive
            job_dict = await self._find_by_job_id(job_id=job_id, jobs_db=jobs_db)
            if job_dict:
                # First try to remove this job-id from the job-name
                job_name = job_dict["job_name"]
                job_name_doc = await jobs_db.get(job_name)
                if job_name_doc:
                    # This should be a list of batch job-ids for a job name
                    job_ids: List = json.loads(job_name_doc)
                    if job_id in job_ids:
                        job_ids.remove(job_id)
                    if job_ids:
                        job_name_doc = json.dumps(list(job_ids))
                        await jobs_db.set(job_name, job_name_doc)
                    else:
                        # Don't save an empty set, delete the job-name entirely
                        await jobs_db.delete(job_name)

                await jobs_db.delete(job_id)
                return job_dict

    async def remove_by_job_name(self, job_name: str) -> Set[str]:
        """
        Remove any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a set of deleted job-ids
        """
        if job_name:
            jobs_db = await self.jobs_db_alive
            job_name_doc = await jobs_db.get(job_name)
            if job_name_doc:
                job_ids = set(json.loads(job_name_doc))
                await jobs_db.delete(*job_ids)
                await jobs_db.delete(job_name)
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

        jobs_db = await self.jobs_db_alive
        job_json = json.dumps(job.db_data)

        for tries in range(3):
            try:
                response = await jobs_db.set(job.job_id, job_json)
                LOGGER.debug(response)
                if response:
                    LOGGER.info(
                        "AWS batch-db (%s:%s) saved status: %s",
                        job.job_name,
                        job.job_id,
                        job.status,
                    )

                # Update the job-id set for the job-name
                job_ids = []
                job_name_doc = await jobs_db.get(job.job_name)
                if job_name_doc:
                    job_ids = json.loads(job_name_doc)
                    if sorted(job_ids) != sorted(set(job_ids)):
                        LOGGER.warning("jobs-db has duplicates for %s", job.job_name)

                if job.job_id not in job_ids:
                    job_ids.append(job.job_id)
                    job_ids = json.dumps(job_ids)
                    await jobs_db.set(job.job_name, job_ids)

                return job.job_id

            except Exception as err:
                LOGGER.error(err)

    async def find_job_logs(self, job_id: str) -> Optional[Dict]:
        """
        Find job logs by job_id

        :param job_id: str
        :return: job logs document
        """
        if job_id:
            logs_db = await self.logs_db_alive
            job_logs = await logs_db.get(job_id)
            if job_logs:
                return json.loads(job_logs)
        LOGGER.error("FAIL to find_job_logs with job_id: %s", job_id)

    async def save_job_logs(self, job: AWSBatchJob) -> List[int]:
        """
        Insert or update job logs (if the job has a job_id)

        :param job: an AWSBatchJob
        :return: a List[tinydb.database.Document.doc_id]
        """
        # TODO: update return data type and content
        if job.job_id:
            logs_db = await self.logs_db_alive
            return await logs_db.set(job.job_id, json.dumps(job.db_logs_data))
        LOGGER.error("FAIL to save_job_logs")

    async def find_jobs_to_run(self) -> List[AWSBatchJob]:
        """
        Find all jobs that have not SUCCEEDED.
        """
        jobs_outstanding = []
        jobs_db = await self.jobs_db_alive
        async for key in jobs_db.scan_iter():
            if valid_uuid4(key):
                job_dict = await self._find_by_job_id(job_id=key, jobs_db=jobs_db)
                job = AWSBatchJob(**job_dict)
                LOGGER.debug(
                    "AWS batch-db (%s:%s) status: %s",
                    job.job_name,
                    job.job_id,
                    job.status,
                )
                if job.job_id and job.status == "SUCCEEDED":
                    LOGGER.debug(job.job_description)
                    continue

                jobs_outstanding.append(job)
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
        # Avoid side-effects in this function:
        # - treat the job as a read-only object
        # - treat any jobs_db records as read-only objects
        # - if a job is not saved, don't save it to jobs_db

        jobs_outstanding = []
        for job in jobs:
            # First check the job itself before checking the jobs_db
            if job.job_id and job.status == "SUCCEEDED":
                LOGGER.debug(job.job_description)
                continue

            # TODO: use a common jobs-db instance for all these

            db_job = await self.find_latest_job_name(job.job_name)
            if db_job:
                LOGGER.debug(
                    "AWS batch-db (%s:%s) status: %s",
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
                LOGGER.debug(
                    "AWS batch-db (%s:%s) status: %s",
                    db_job.job_name,
                    db_job.job_id,
                    db_job.status,
                )
                jobs_recovered.append(db_job)
            else:
                jobs_recovered.append(job)

        return jobs_recovered


@dataclass
class AioAWSBatchTinyDB(AioAWSBatchDB):
    """
    AWS Batch job databases - TinyDB implementation

    The jobs and logs are kept in separate DBs so
    that performance on the jobs_db is not compromised
    by too much data from job logs.  This makes it possible
    to use the jobs_db without capturing logs as well.

    .. seealso:: https://tinydb.readthedocs.io/en/latest/

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

    """

    #: a file used for :py:class::`TinyDB(jobs_db_file)`
    jobs_db_file: str = "/tmp/aws_batch_jobs.json"

    #: a file used for :py:class::`TinyDB(logs_db_file)`
    logs_db_file: str = "/tmp/aws_batch_logs.json"

    def __post_init__(self):

        tinydb.TinyDB.DEFAULT_TABLE = "aws-batch-jobs"
        tinydb.TinyDB.DEFAULT_TABLE_KWARGS = {"cache_size": 0}

        # the jobs and logs are kept in separate DBs so
        # that performance on the jobs_db is not compromised
        # by too much data from job logs.
        self.jobs_db = tinydb.TinyDB(self.jobs_db_file)
        self.logs_db = tinydb.TinyDB(self.logs_db_file)
        LOGGER.info("Using jobs-db file: %s", self.jobs_db_file)
        LOGGER.info("Using logs-db file: %s", self.logs_db_file)

        # Lazy init for any asyncio instances
        self._db_sem = None

    @classmethod
    def get_paged_batch_db(cls, db_path: Path, offset: int, limit: int):
        db_page = f"{offset:06d}_{offset + limit:06d}"
        jobs_db_file = str(db_path / f"aio_batch_jobs_{db_page}.json")
        logs_db_file = str(db_path / f"aio_batch_logs_{db_page}.json")
        return cls(jobs_db_file=jobs_db_file, logs_db_file=logs_db_file)

    @property
    def db_semaphore(self) -> asyncio.Semaphore:
        """A semaphore to limit requests to the db"""
        if self._db_sem is None:
            self._db_sem = asyncio.Semaphore()
        return self._db_sem

    async def all_jobs(self) -> List[AWSBatchJob]:
        """
        Collect all jobs.

        Warning: this could exceed memory, try to use
        the :py:meth:`gen_all_jobs` wherever possible.
        """
        jobs = []
        async for j in self.gen_all_jobs():
            jobs.append(j)
        return jobs

    async def gen_all_jobs(self) -> AsyncIterator[AWSBatchJob]:
        """
        Generate all jobs.
        """
        async with self.db_semaphore:
            for job_doc in self.jobs_db.all():
                job_id = job_doc.get("job_id", "")
                if valid_uuid4(job_id):
                    yield AWSBatchJob(**job_doc)

    async def all_job_ids(self) -> Set[str]:
        """
        Collect all jobIds.
        """
        job_ids = set()
        async for job_id in self.gen_job_ids():
            job_ids.add(job_id)
        return job_ids

    async def gen_job_ids(self) -> AsyncIterator[str]:
        """
        Generate all jobIds.
        """
        async with self.db_semaphore:
            for job_doc in self.jobs_db.all():
                job_id = job_doc.get("job_id", "")
                if valid_uuid4(job_id):
                    yield job_id

    async def count_by_job_status(self) -> Counter:
        """
        Count all jobs by jobStatus

        :return: a Counter of jobs by job status (could contain multiple
            entries for the same jobName, if it is run more than once)
        """
        counter = Counter()
        for job_status in AWSBatchJobStates:
            counter[job_status.name] = 0
        async with self.db_semaphore:
            for job_doc in self.jobs_db.all():
                job = AWSBatchJob(**job_doc)
                counter[job.status] += 1
        return counter

    async def group_by_job_status(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        Group all jobId by jobStatus

        :return: a dictionary of job info by job status (could contain multiple
            entries with the same jobName if it is run multiple times);
            the dictionary values contain a list of job information tuples, e.g.

            .. code-block::

                { status: [ (job.job_id, job.job_name, job.status), ] }

        """
        groups = defaultdict(list)
        for job_status in AWSBatchJobStates:
            groups[job_status.name] = []
        async with self.db_semaphore:
            for job_doc in self.jobs_db.all():
                job = AWSBatchJob(**job_doc)
                job_info = (job.job_id, job.job_name, job.status)
                groups[job.status].append(job_info)
        return groups

    async def find_by_job_id(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Find one job by the jobId

        :param job_id: a batch jobId
        :return: the :py:meth:`AWSBatchJob.job_data` or None
        """
        if job_id:
            async with self.db_semaphore:
                job_query = tinydb.Query()
                return self.jobs_db.get(job_query.job_id == job_id)

    async def find_by_job_name(self, job_name: str) -> List[tinydb.database.Document]:
        """
        Find any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a list of documents containing :py:meth:`AWSBatchJob.job_data`
        """
        if job_name:
            async with self.db_semaphore:
                job_query = tinydb.Query()
                return self.jobs_db.search(job_query.job_name == job_name)

    async def find_latest_job_name(self, job_name: str) -> Optional[AWSBatchJob]:
        """
        Find the latest job matching the jobName,
        based on the "createdAt" time stamp.

        :param job_name: a batch jobName
        :return: the latest job record available
        """
        jobs_saved = await self.find_by_job_name(job_name)
        if jobs_saved:
            db_jobs = [AWSBatchJob(**job_doc) for job_doc in jobs_saved]
            db_jobs = sorted(db_jobs, key=lambda j: j.created or j.submitted)
            db_job = db_jobs[-1]
            return db_job

    async def find_by_job_status(self, job_states: List[str]) -> List[AWSBatchJob]:
        """
        Find any jobs matching any jobStatus values

        :param job_states: a list of valid job status values
        :return: a list of AWSBatchJob (could contain multiple
            entries with the same jobName if it is run multiple times
            with any jobStatus in the jobStatus values)
        """
        for status in job_states:
            assert status in AWSBatchJob.STATES

        jobs = []
        async with self.db_semaphore:
            for job_doc in self.jobs_db.all():
                job = AWSBatchJob(**job_doc)
                if job.status in job_states:
                    jobs.append(job)
        return jobs

    async def remove_by_job_id(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Remove any job matching the jobId

        :param job_id: a batch jobId
        :return: a deleted document
        """
        if job_id:
            doc = await self.find_by_job_id(job_id)
            if doc:
                removed_ids = self.jobs_db.remove(doc_ids=[doc.doc_id])
                if removed_ids:
                    return doc

    async def remove_by_job_name(self, job_name: str) -> Set[str]:
        """
        Remove any jobs matching the jobName

        :param job_name: a batch jobName
        :return: a set of deleted job-ids
        """
        if job_name:
            jobs_found = await self.find_by_job_name(job_name)
            if jobs_found:
                jobs_by_doc_id = {doc.doc_id: doc["job_id"] for doc in jobs_found}
                doc_ids = list(jobs_by_doc_id.keys())
                async with self.db_semaphore:
                    removed_ids = self.jobs_db.remove(doc_ids=doc_ids)
                    return set([jobs_by_doc_id[doc_id] for doc_id in removed_ids])

    async def save_job(self, job: AWSBatchJob) -> Optional[str]:
        """
        Insert or update a job (if it has a job_id)

        :param job: an AWSBatchJob
        :return: the jobId if the job is saved
        """
        if job.job_id:
            async with self.db_semaphore:
                job_query = tinydb.Query()
                doc_ids = self.jobs_db.upsert(
                    job.db_data, job_query.job_id == job.job_id
                )
                if doc_ids:
                    return job.job_id
        else:
            LOGGER.error("FAIL to save_job without job_id")

    async def find_job_logs(self, job_id: str) -> Optional[tinydb.database.Document]:
        """
        Find job logs by job_id

        :param job_id: str
        :return: a tinydb.database.Document with job logs
        """
        if job_id:
            async with self.db_semaphore:
                log_query = tinydb.Query()
                return self.logs_db.get(log_query.job_id == job_id)
        else:
            LOGGER.error("FAIL to find_job_logs without job_id")

    async def save_job_logs(self, job: AWSBatchJob) -> Optional[str]:
        """
        Insert or update job logs (if the job has a job_id)

        :param job: an AWSBatchJob
        :return: a job.job_id if the logs are saved
        """
        if job.job_id:
            async with self.db_semaphore:
                log_query = tinydb.Query()
                doc_ids = self.logs_db.upsert(
                    job.db_logs_data, log_query.job_id == job.job_id
                )
                if doc_ids:
                    return job.job_id
        else:
            LOGGER.error("FAIL to save_job_logs without job_id")

    async def find_jobs_to_run(self) -> List[AWSBatchJob]:
        """
        Find all jobs that have not SUCCEEDED.  Note that any jobs handled
        by the job-manager will not re-run if they have a job.job_id, those
        jobs will be monitored until complete.
        """
        async with self.db_semaphore:
            jobs = [AWSBatchJob(**job_doc) for job_doc in self.jobs_db.all()]

        jobs_outstanding = []
        for job in jobs:
            LOGGER.info(
                "AWS batch-db (%s:%s) status: %s",
                job.job_name,
                job.job_id,
                job.status,
            )
            if job.job_id and job.status == "SUCCEEDED":
                LOGGER.debug(job.job_description)
                continue

            jobs_outstanding.append(job)

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
                    "AWS batch-db (%s:%s) status: %s",
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
                    "AWS batch-db (%s:%s) status: %s",
                    db_job.job_name,
                    db_job.job_id,
                    db_job.status,
                )
                jobs_recovered.append(db_job)
            else:
                jobs_recovered.append(job)

        return jobs_recovered
