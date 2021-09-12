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
Test AWS Batch TinyDB
"""
import tinydb

from aio_aws.aws_batch_models import AWSBatchJob
from aio_aws.uuid_utils import valid_uuid4


def test_batch_job(aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    assert isinstance(job.job_name, str)
    assert isinstance(job.job_description, dict)
    assert isinstance(job.job_tries, list)
    assert job.job_tries == [job.job_id]
    assert valid_uuid4(job.job_id)

    assert job.created == 1584977374
    assert job.started == 1584977376
    assert job.stopped == 1584977386
    assert job.elapsed == 1584977386 - 1584977374
    assert job.runtime == 1584977386 - 1584977376
    assert job.spinup == 1584977376 - 1584977374


def test_batch_job_db_save(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]


def test_batch_job_db_find_by_job_id(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    job_doc = test_jobs_db.find_by_job_id(job.job_id)
    assert isinstance(job_doc, tinydb.database.Document)
    assert isinstance(job_doc, dict)  # it's also a dict
    assert job_doc["job_id"] == job.job_id
    assert job_doc["job_name"] == job.job_name
    assert job_doc["status"] == job.status
    # test all the fields, since job_doc behaves like a dict
    assert job.db_data == job_doc


def test_batch_job_db_remove_by_job_id(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    job_doc = test_jobs_db.remove_by_job_id(job.job_id)
    assert isinstance(job_doc, tinydb.database.Document)
    assert job_doc["job_id"] == job.job_id


def test_batch_job_db_find_by_job_name(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    job_docs = test_jobs_db.find_by_job_name(job.job_name)
    assert isinstance(job_docs, list)
    assert len(job_docs) == 1
    job_doc = job_docs[0]
    assert isinstance(job_doc, tinydb.database.Document)
    assert isinstance(job_doc, dict)  # it's also a dict
    assert job_doc["job_id"] == job.job_id
    assert job_doc["job_name"] == job.job_name
    assert job_doc["status"] == job.status
    # test all the fields, since job_doc behaves like a dict
    assert job.db_data == job_doc


def test_batch_job_db_remove_by_job_name(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    job_docs = test_jobs_db.remove_by_job_name(job.job_name)
    assert isinstance(job_docs, list)
    assert len(job_docs) == 1
    job_doc = job_docs[0]
    assert isinstance(job_doc, tinydb.database.Document)
    assert job_doc["job_name"] == job.job_name


def test_batch_job_db_find_jobs_to_run(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    jobs = test_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_find_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    jobs = test_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done


def test_batch_job_db_filter_jobs_to_run(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    jobs = test_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    jobs = test_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done


def test_batch_job_db_saved_filter_jobs_to_run(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    jobs = test_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_saved_filter_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_docs = test_jobs_db.save_job(job)
    assert job_docs == [1]
    jobs = test_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done


def test_batch_job_db_saved_filter_jobs_to_run_for_recovery(
    test_jobs_db, aws_batch_job
):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_docs = test_jobs_db.save_job(job)
    jobs = test_jobs_db.jobs_to_run([job])
    assert len(jobs) == 0  # successful jobs are done
    # Assume the job is recreated and needs to be recovered from the db
    job.reset()
    assert job.job_id is None
    assert job.job_name  # used to recover the job from the db
    jobs = test_jobs_db.jobs_to_run([job])
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # the job.job_name is used to recover the job


def test_batch_job_db_find_latest_job_name(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    test_jobs_db.save_job(job)

    # Fake another submission of the same job-name
    orig_job_id = job.job_id
    fake_job_id = job.job_id.replace("08986fbb7144", "08986fbb7145")
    job.job_id = fake_job_id
    job.job_submission["jobId"] = fake_job_id
    job.job_description["status"] = "SUCCEEDED"
    job.status = job.job_description["status"]
    job.job_description["createdAt"] += 5
    job.job_description["startedAt"] += 5
    job.job_description["stoppedAt"] += 5
    test_jobs_db.save_job(job)
    job_docs = test_jobs_db.find_by_job_name(job.job_name)
    assert len(job_docs) == 2
    assert [j["job_id"] for j in job_docs] == [orig_job_id, fake_job_id]
    job_found = test_jobs_db.find_latest_job_name(job.job_name)
    assert job_found.job_id == fake_job_id


def test_batch_job_db_saved_filter_jobs_to_run_with_duplicate(
    test_jobs_db, aws_batch_job
):
    job = aws_batch_job
    # Fake a job failure
    job.job_description["status"] = "FAILED"
    job.status = job.job_description["status"]
    test_jobs_db.save_job(job)

    jobs = test_jobs_db.jobs_to_run([job])
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
    test_jobs_db.save_job(job)

    jobs = test_jobs_db.jobs_to_run([job])
    assert len(jobs) == 0  # successful jobs are done
