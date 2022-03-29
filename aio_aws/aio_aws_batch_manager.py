import asyncio
import json
import os
import sys
from abc import abstractmethod
from pathlib import Path
from typing import Any
from typing import Generator
from typing import Iterable
from typing import Optional

from pydantic import BaseSettings
from pydantic import Field

from aio_aws.aio_aws_batch import batch_get_logs
from aio_aws.aio_aws_batch import batch_monitor_jobs
from aio_aws.aio_aws_batch import batch_run_jobs
from aio_aws.aio_aws_batch import batch_submit_jobs
from aio_aws.aio_aws_batch import batch_update_jobs
from aio_aws.aio_aws_batch import batch_update_jobs_db
from aio_aws.aio_aws_batch import find_complete_jobs
from aio_aws.aio_aws_batch import find_incomplete_jobs
from aio_aws.aio_aws_batch import find_jobs_by_status
from aio_aws.aio_aws_batch import find_latest_jobs_with_jobs_db
from aio_aws.aio_aws_batch import find_running_jobs
from aio_aws.aio_aws_batch import get_logs_by_status
from aio_aws.aio_aws_batch_db import AioAWSBatchDB
from aio_aws.aio_aws_s3 import validate_s3_files
from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.logger import get_logger
from aio_aws.s3_io import YamlBaseModel

LOGGER = get_logger(__name__)

BATCH_ENV = {
    "development": "dev",
    "testing": "test",
    "production": "prod",
    "dev": "dev",
    "test": "test",
    "prod": "prod",
}

PAGE_LIMIT_MAX = 1000


class AioAwsBatchManager(YamlBaseModel, BaseSettings):

    aws_batch_job_definition: str = Field(
        required=True,
        env="AWS_BATCH_JOB_DEFINITION",
        help="a named AWS Batch Job Definition",
    )

    aws_batch_job_queue: str = Field(
        required=True,
        env="AWS_BATCH_JOB_QUEUE",
        help="a named AWS Batch Job Queue",
    )

    aws_batch_region: str = Field(
        required=True,
        env="AWS_DEFAULT_REGION",
        description="an AWS region, like 'us-east-1'",
    )

    debug: bool = Field(
        default=True, env="DEBUG", description="Select a small subset of data"
    )
    dry_run: bool = Field(
        default=True, env="DRY_RUN", description="Report work to do, without doing any"
    )
    page_offset: int = Field(default=0, description="")
    page_limit: int = Field(default=PAGE_LIMIT_MAX, description="")
    jobs_db_path: Optional[Path] = Field(default=None, description="")
    list_jobs: bool = Field(default=False, description="")
    count_db_jobs: bool = Field(default=False, description="")
    list_db_jobs: bool = Field(default=False, description="")
    update_db_jobs: bool = Field(default=False, description="")
    update_db_logs: bool = Field(default=False, description="")
    run_jobs: bool = Field(default=False, description="Submit and monitor jobs")
    submit_jobs: bool = Field(default=False, description="Submit jobs")
    monitor_jobs: bool = Field(default=False, description="Monitor submitted jobs")
    update_jobs: bool = Field(default=False, description="Update job descriptions")
    update_logs: bool = Field(default=False, description="Update job logs")
    update_logs_by_status: Optional[str] = Field(
        default=None,
        description="Update job logs by status",
        # TODO: add aws batch status choices?
    )
    failed_jobs: bool = Field(default=False, description="List failed jobs")
    rerun_all: bool = Field(default=False, description="Reset and rerun all jobs")
    rerun_failed: bool = Field(default=False, description="Reset and rerun failed jobs")
    redis_jobs_db: bool = Field(default=False, description="Use Redis for DB data")

    @classmethod
    def get_jobs_db_path(cls, db_path: Path = None) -> Path:
        if db_path is None:
            home = os.getenv("HOME")
            db_path = Path(f"{home}/batch_jobs")
        else:
            db_path = Path(db_path)
        db_path.mkdir(parents=True, exist_ok=True)
        assert db_path.exists()
        return db_path

    def get_paged_batch_db(
        self, db_path: Path, offset: int, limit: int
    ) -> AioAWSBatchDB:
        from aio_aws.aio_aws_batch_db import AioAWSBatchTinyDB

        db_page = f"{offset:06d}_{offset + limit:06d}"

        db_path = self.get_jobs_db_path(db_path)

        # TODO: use the jobs-queue in the path?

        jobs_db_file = str(db_path / f"aws_batch_jobs_{db_page}.json")
        logs_db_file = str(db_path / f"aws_batch_logs_{db_page}.json")

        return AioAWSBatchTinyDB(jobs_db_file=jobs_db_file, logs_db_file=logs_db_file)

    @classmethod
    def get_aioredis_batch_db(cls) -> AioAWSBatchDB:
        from aio_aws.aio_aws_batch_db import AioAWSBatchRedisDB

        return AioAWSBatchRedisDB()

    @abstractmethod
    def gen_tasks(self) -> Generator[Any, None, None]:
        raise NotImplementedError("Subclass implements this")

    @abstractmethod
    def gen_batch_jobs(
        self, tasks: Iterable[Any]
    ) -> Generator[AWSBatchJob, None, None]:
        raise NotImplementedError("Subclass implements this")

    @abstractmethod
    def dry_run_summary(self, tasks: Iterable[Any], jobs: Iterable[AWSBatchJob]):
        raise NotImplementedError("Subclass implements this")

    def run_batch_jobs(self):
        LOGGER.info("Running AWS-Batch Jobs")

        tasks = list(self.gen_tasks())
        if not tasks:
            LOGGER.error("No tasks generated")
            sys.exit(1)

        if self.debug:
            tasks = tasks[-2:]

        if self.redis_jobs_db:
            aio_batch_db = self.get_aioredis_batch_db()
            if self.update_db_jobs:
                batch_update_jobs_db(aio_batch_db)
                return
            if self.update_db_logs:
                raise NotImplementedError("TODO")
                # batch_update_logs_db(aio_batch_db)
                # return
            if self.count_db_jobs:
                counter = asyncio.run(aio_batch_db.count_by_job_status())
                LOGGER.info("AWS Batch jobs by status: %s", json.dumps(counter))
                return
            if self.list_db_jobs:
                jobs_by_status = asyncio.run(aio_batch_db.group_by_job_status())
                # {status: [(job.job_id, job.job_name, job.status), ]}
                for _, job_group in jobs_by_status.items():
                    for job_data in job_group:
                        job_id, job_name, job_status = job_data
                        LOGGER.info(
                            "AWS Batch job (%s:%s) has status: %s",
                            job_name,
                            job_id,
                            job_status,
                        )
                return

        # Paginate the jobs in groups of 1000 jobs with a new jobs-db
        # for each group to avoid poor db query performance when the
        # jobs-db become too large
        page_limit = self.page_limit
        if page_limit > PAGE_LIMIT_MAX:
            page_limit = PAGE_LIMIT_MAX
            LOGGER.info("The page_limit max is reset to %d jobs", PAGE_LIMIT_MAX)

        page_offsets = list(range(self.page_offset, len(tasks), page_limit))
        n_pages = len(page_offsets)

        for page_n, offset in enumerate(page_offsets):

            page_tasks = tasks[offset : offset + page_limit]
            s3_access = validate_s3_files((query.tile_file for query in page_tasks))

            # Maintain the original sequence of the input files while using the s3_access dict;
            # the sequence is important when paginating large input lists
            page_tasks = [
                query for query in page_tasks if s3_access.get(query.tile_file)
            ]

            LOGGER.info(
                "%6d page files (page %d of %d)", len(page_tasks), page_n + 1, n_pages
            )

            if not page_tasks:
                LOGGER.error("No valid s3 geotiff files")
                continue

            if self.redis_jobs_db:
                aio_batch_db = self.get_aioredis_batch_db()
            else:
                aio_batch_db = self.get_paged_batch_db(
                    self.jobs_db_path, offset, page_limit
                )

            if self.update_db_jobs:
                batch_update_jobs_db(aio_batch_db)
                continue

            if self.update_db_logs:
                raise NotImplementedError("TODO")
                # batch_update_logs_db(aio_batch_db)

            if self.count_db_jobs:
                counter = asyncio.run(aio_batch_db.count_by_job_status())
                LOGGER.info("AWS Batch jobs by status: %s", json.dumps(counter))
                continue

            if self.list_db_jobs:
                jobs_by_status = asyncio.run(aio_batch_db.group_by_job_status())
                # {status: [(job.job_id, job.job_name, job.status), ]}
                for _, job_group in jobs_by_status.items():
                    for job_data in job_group:
                        job_id, job_name, job_status = job_data
                        LOGGER.info(
                            "AWS Batch job (%s:%s) has status: %s",
                            job_name,
                            job_id,
                            job_status,
                        )
                continue

            #
            # Everything below requires generating an AWSBatchJob
            #
            # Most operations require collecting jobs into a list that will not be
            # exhausted after 1 iteration because each job has its own state that
            # is updated by job monitoring.
            #
            # Several convenience methods will check the generated jobs against a
            # jobs-db to resurrect the job data using a jobName
            #

            jobs = self.gen_batch_jobs(tasks=page_tasks)

            if self.dry_run:
                self.dry_run_summary(tasks=page_tasks, jobs=jobs)
                # Only print one page of jobs
                sys.exit()

            if self.list_jobs:

                for job in find_latest_jobs_with_jobs_db(
                    jobs=jobs, jobs_db=aio_batch_db
                ):
                    LOGGER.info(
                        "AWS Batch job (%s:%s) has status: %s",
                        job.job_name,
                        job.job_id,
                        job.status,
                    )

            elif self.run_jobs or self.submit_jobs:

                if self.rerun_all:
                    # WARNING: this ignores any jobs-db entries to allow re-submitting
                    # the same jobs (to get new job-id tries)
                    incomplete_jobs = list(jobs)

                else:
                    incomplete_jobs = list(
                        find_incomplete_jobs(
                            jobs=jobs,
                            jobs_db=aio_batch_db,
                            reset_failed=self.rerun_failed,
                        )
                    )

                if not incomplete_jobs:
                    LOGGER.info("There are no jobs to run")
                    continue

                if self.run_jobs:
                    batch_run_jobs(jobs=incomplete_jobs, jobs_db=aio_batch_db)
                    LOGGER.info("All jobs are complete")

                elif self.submit_jobs:
                    batch_submit_jobs(jobs=incomplete_jobs, jobs_db=aio_batch_db)
                    LOGGER.info("All jobs are submitted")

            elif self.monitor_jobs or self.update_jobs:

                running_jobs = list(find_running_jobs(jobs, aio_batch_db))
                if not running_jobs:
                    LOGGER.info("There are no running jobs")
                    continue

                if self.monitor_jobs:
                    # Update jobs first (it's more efficient than monitor-jobs
                    # because it uses bulk describe-jobs)
                    batch_update_jobs(jobs=running_jobs, jobs_db=aio_batch_db)
                    LOGGER.info("All jobs are updated")
                    batch_monitor_jobs(jobs=running_jobs, jobs_db=aio_batch_db)
                    LOGGER.info("All jobs are complete (SUCCEEDED or FAILED)")

                elif self.update_jobs:
                    batch_update_jobs(jobs=running_jobs, jobs_db=aio_batch_db)
                    LOGGER.info("All jobs are updated")

            elif self.update_logs:

                jobs = asyncio.run(
                    aio_batch_db.jobs_recovery(jobs=jobs, include_logs=True)
                )
                complete_jobs = list(find_complete_jobs(jobs, aio_batch_db))
                if not complete_jobs:
                    LOGGER.info("There are no complete jobs")
                    continue
                batch_get_logs(
                    jobs=complete_jobs, jobs_db=aio_batch_db, skip_existing=True
                )
                LOGGER.info("Collected all job logs")

            elif self.update_logs_by_status:
                # this is most useful for FAILED jobs
                job_states = []
                for job_state in self.update_logs_by_status.split(","):
                    if job_state in AWSBatchJob.STATES:
                        job_states.append(job_state)
                if not job_states:
                    LOGGER.error(
                        "Specify job states as a CSV string like: 'FAILED,SUCCEEDED'"
                    )
                    return
                get_logs_by_status(
                    jobs=jobs,
                    job_states=job_states,
                    jobs_db=aio_batch_db,
                    print_logs=True,  # intended to view FAILED job logs
                )

            elif self.failed_jobs:
                complete_jobs = list(
                    find_jobs_by_status(
                        jobs, job_states=["FAILED"], jobs_db=aio_batch_db
                    )
                )
                if not complete_jobs:
                    LOGGER.info("There are no FAILED jobs")
                    continue

            else:

                LOGGER.error("No action specified; defaulting to a dry-run")

                self.dry_run_summary(tasks=page_tasks, jobs=jobs)
                # Only print one page of jobs
                sys.exit()
