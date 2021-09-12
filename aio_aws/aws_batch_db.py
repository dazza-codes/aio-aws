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
AWS Batch DB
============

"""

import asyncio
from dataclasses import dataclass
from typing import List
from typing import Optional

import tinydb

from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)


@dataclass
class AWSBatchDB:
    """
    AWS Batch job databases

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
        LOGGER.info("Using batch-jobs-db file: %s", self.jobs_db_file)
        LOGGER.info("Using batch-logs-db file: %s", self.logs_db_file)

        # Lazy init for any asyncio instances
        self._db_sem = None

    @property
    def db_semaphore(self) -> asyncio.Semaphore:
        """A semaphore to limit requests to the db"""
        if self._db_sem is None:
            self._db_sem = asyncio.Semaphore()
        return self._db_sem

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

    def find_latest_job_name(self, job_name: str) -> Optional[AWSBatchJob]:
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
                "AWS Batch job (%s:%s) has db status: %s",
                job.job_name,
                job.job_id,
                job.status,
            )
            if job.job_id and job.status == "SUCCEEDED":
                LOGGER.debug(job.job_description)
                continue

            jobs_outstanding.append(job)

        return jobs_outstanding

    def jobs_to_run(self, jobs: List[AWSBatchJob]) -> List[AWSBatchJob]:
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

            db_job = self.find_latest_job_name(job.job_name)
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

    def jobs_recovery(self, jobs: List[AWSBatchJob]) -> List[AWSBatchJob]:
        """
        Use the job.job_name to find any jobs_db records to recover job data.
        """
        jobs_recovered = []
        for job in jobs:

            db_job = self.find_latest_job_name(job.job_name)
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
