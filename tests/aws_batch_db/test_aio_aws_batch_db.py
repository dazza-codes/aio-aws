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
Test Aio AWS Batch DB
"""
from typing import Dict
from typing import List
from typing import Set

import pytest

from aio_aws.aio_aws_batch import AWSBatchJob
from aio_aws.aio_aws_batch_db import AioAWSBatchDB


@pytest.fixture
def redis_url(redisdb) -> str:
    redis_client = redisdb.client()
    yield f"unix://{redis_client.connection.path}"


@pytest.fixture
@pytest.mark.asyncio
async def aioredis_jobs_db(redis_url) -> AioAWSBatchDB:
    batch_jobs_db = AioAWSBatchDB(redis_url=redis_url)
    key_count = 0
    async for key in batch_jobs_db.jobs_db.scan_iter():
        key_count += 1
    async for key in batch_jobs_db.logs_db.scan_iter():
        key_count += 1
    assert key_count == 0
    yield batch_jobs_db


@pytest.mark.asyncio
async def test_aio_aws_batch_db_init(redis_url):
    batch_db = AioAWSBatchDB(redis_url=redis_url)
    assert await batch_db.db_alive is True
    db_info = await batch_db.db_info
    assert db_info["jobs"]["redis_version"]
    assert db_info["logs"]["redis_version"]


@pytest.mark.asyncio
async def test_batch_job_db_save(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.job_id
    assert job.job_name
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    db_job = await aioredis_jobs_db.find_by_job_id(job.job_id)
    assert db_job


@pytest.mark.asyncio
async def test_batch_job_db_find_by_job_id(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    job_dict = await aioredis_jobs_db.find_by_job_id(job.job_id)
    assert isinstance(job_dict, dict)
    assert job_dict["job_id"] == job.job_id
    assert job_dict["job_name"] == job.job_name
    assert job_dict["status"] == job.status
    # test all the fields, since it behaves like a dict
    assert job.db_data == job_dict


@pytest.mark.asyncio
async def test_batch_job_db_remove_by_job_id(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    job_dict = await aioredis_jobs_db.remove_by_job_id(job.job_id)
    assert isinstance(job_dict, Dict)
    assert job.db_data == job_dict


@pytest.mark.asyncio
async def test_batch_job_db_find_by_job_name(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    job_dicts = await aioredis_jobs_db.find_by_job_name(job.job_name)
    assert isinstance(job_dicts, List)
    assert len(job_dicts) == 1
    job_dict = job_dicts[0]
    assert isinstance(job_dict, Dict)
    assert job.db_data == job_dict


@pytest.mark.asyncio
async def test_batch_job_db_remove_by_job_name(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    job_ids = await aioredis_jobs_db.remove_by_job_name(job.job_name)
    assert isinstance(job_ids, Set)
    assert len(job_ids) == 1
    for job_id in job_ids:
        assert isinstance(job_id, str)
        assert job_id == job.job_id
    job_id = await aioredis_jobs_db.find_by_job_id(job.job_id)
    assert job_id is None
    job_id = await aioredis_jobs_db.remove_by_job_id(job.job_id)
    assert job_id is None


@pytest.mark.asyncio
async def test_batch_job_db_find_jobs_to_run(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    jobs = await aioredis_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, List)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


@pytest.mark.asyncio
async def test_batch_job_db_find_jobs_to_run_empty(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    jobs = await aioredis_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, List)
    assert len(jobs) == 0  # successful jobs are done


@pytest.mark.asyncio
async def test_batch_job_db_filter_jobs_to_run(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, List)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


@pytest.mark.asyncio
async def test_batch_job_db_jobs_to_run_empty(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, List)
    assert len(jobs) == 0  # successful jobs are done


@pytest.mark.asyncio
async def test_batch_job_db_saved_filter_jobs_to_run(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, List)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


@pytest.mark.asyncio
async def test_batch_job_db_saved_filter_jobs_to_run_empty(
    aioredis_jobs_db, aws_batch_job
):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, List)
    assert len(jobs) == 0  # successful jobs are done


@pytest.mark.asyncio
async def test_batch_job_db_saved_filter_jobs_to_run_for_recovery(
    aioredis_jobs_db, aws_batch_job
):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert len(jobs) == 0  # successful jobs are done
    # Assume the job is recreated and needs to be recovered from the db
    job.reset()
    assert job.job_id is None
    assert job.job_name  # used to recover the job from the db
    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, List)
    assert len(jobs) == 0  # the job.job_name is used to recover the job


@pytest.mark.asyncio
async def test_batch_job_db_find_latest_job_name(aioredis_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id

    # Fake another submission of the same job-name
    new_job_dict = job.db_data
    new_job_dict["job_id"] = job.job_id.replace("08986fbb7144", "08986fbb7145")
    new_job = AWSBatchJob(**new_job_dict)
    new_job.job_submission["jobId"] = new_job.job_id
    new_job.job_description["status"] = "SUCCEEDED"
    new_job.status = job.job_description["status"]
    new_job.job_description["createdAt"] += 5
    new_job.job_description["startedAt"] += 5
    new_job.job_description["stoppedAt"] += 5
    job_id = await aioredis_jobs_db.save_job(new_job)
    assert job_id == new_job.job_id
    job_dicts = await aioredis_jobs_db.find_by_job_name(job.job_name)
    assert len(job_dicts) == 2
    assert sorted([j["job_id"] for j in job_dicts]) == sorted(
        [job.job_id, new_job.job_id]
    )
    job_found = await aioredis_jobs_db.find_latest_job_name(job.job_name)
    assert job_found.job_id == new_job.job_id


@pytest.mark.asyncio
async def test_batch_job_db_saved_filter_jobs_to_run_with_duplicate(
    aioredis_jobs_db, aws_batch_job
):
    job = aws_batch_job
    # Fake a job failure
    job.job_description["status"] = "FAILED"
    job.status = job.job_description["status"]
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id

    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert len(jobs) == 1  # failed jobs could be run again (if reset)

    # Fake another submission of the same job-name
    fake_job_id = job.job_id.replace("08986fbb7144", "08986fbb7145")
    job.job_id = fake_job_id
    job.job_submission["jobId"] = fake_job_id
    job.job_description["status"] = "SUCCEEDED"
    job.status = job.job_description["status"]
    job.job_description["createdAt"] += 5
    job.job_description["startedAt"] += 5
    job.job_description["stoppedAt"] += 5
    job_id = await aioredis_jobs_db.save_job(job)
    assert job_id == job.job_id

    jobs = await aioredis_jobs_db.jobs_to_run([job])
    assert len(jobs) == 0  # successful jobs are done
