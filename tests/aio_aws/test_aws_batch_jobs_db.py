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
Test AWS Batch TinyDB
"""

import pytest
import tinydb

from notes.aio_aws.aio_aws_batch import AWSBatchDB
from notes.aio_aws.aio_aws_batch import AWSBatchJob
from notes.aio_aws.aio_aws_batch import jobs_to_run


@pytest.fixture
def test_jobs_db() -> AWSBatchDB:
    batch_jobs_db = AWSBatchDB(jobs_db_file="/tmp/test_batch_jobs_db.json")
    batch_jobs_db.jobs_db.purge()
    assert batch_jobs_db.jobs_db.all() == []
    yield batch_jobs_db
    batch_jobs_db.jobs_db.purge()
    assert batch_jobs_db.jobs_db.all() == []


@pytest.fixture
def aws_batch_job() -> AWSBatchJob:
    job_data = {
        "job_id": "1aac2eab-897a-4b89-9eb3-08986fbb7144",
        "job_name": "sleep-1-job",
        "job_queue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
        "job_submission": {
            "ResponseMetadata": {
                "HTTPHeaders": {
                    "content-length": "75",
                    "content-type": "text/html; " "charset=utf-8",
                    "date": "Mon, 23 Mar " "2020 " "15:29:33 GMT",
                    "server": "amazon.com",
                },
                "HTTPStatusCode": 200,
                "RetryAttempts": 0,
            },
            "jobId": "1aac2eab-897a-4b89-9eb3-08986fbb7144",
            "jobName": "sleep-1-job",
        },
        "job_tries": ["1aac2eab-897a-4b89-9eb3-08986fbb7144"],
        "max_tries": 4,
        "num_tries": 1,
        "status": "SUCCEEDED",
        "command": ["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"],
        "container_overrides": {
            "command": ["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"]
        },
        "depends_on": [],
        "job_definition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
        "job_description": {
            "container": {
                "command": [
                    '/bin/sh -c "for a in `seq 1 ' "10`; do echo Hello World; " 'sleep 1; done"'
                ],
                "logStreamName": "moto_test_job_definition/default/1aac2eab-897a-4b89-9eb3-08986fbb7144",
                "privileged": False,
                "readonlyRootFilesystem": False,
                "ulimits": [],
                "vcpus": 1,
                "volumes": [],
            },
            "dependsOn": [],
            "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
            "jobId": "1aac2eab-897a-4b89-9eb3-08986fbb7144",
            "jobName": "sleep-1-job",
            "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
            "createdAt": 1584977374,  # https://github.com/spulec/moto/issues/2829
            "startedAt": 1584977376,
            "status": "SUCCEEDED",
            "stoppedAt": 1584977386,
        },
    }
    return AWSBatchJob(**job_data)


def test_batch_job(aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    assert isinstance(job.job_name, str)
    assert isinstance(job.job_description, dict)
    assert isinstance(job.job_tries, list)
    assert job.job_tries == [job.job_id]

    assert job.created == 1584977374
    assert job.started == 1584977376
    assert job.stopped == 1584977386
    assert job.elapsed == 1584977386 - 1584977374
    assert job.runtime == 1584977386 - 1584977376
    assert job.spinup == 1584977376 - 1584977374


def test_batch_job_db_save(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]


def test_batch_job_db_find_by_job_id(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save(job)
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
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]
    job_doc = test_jobs_db.remove_by_job_id(job.job_id)
    assert isinstance(job_doc, tinydb.database.Document)
    assert job_doc["job_id"] == job.job_id


def test_batch_job_db_find_by_job_name(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job_docs = test_jobs_db.save(job)
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
    job_docs = test_jobs_db.save(job)
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
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]
    jobs = test_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_find_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]
    jobs = test_jobs_db.find_jobs_to_run()
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done


def test_batch_job_db_filter_jobs_to_run(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    jobs = jobs_to_run([job], test_jobs_db)
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    jobs = jobs_to_run([job], test_jobs_db)
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done


def test_batch_job_db_saved_filter_jobs_to_run(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    job.status = "SUBMITTED"  # this job can be 'recovered'
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]
    jobs = jobs_to_run([job], test_jobs_db)
    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], AWSBatchJob)


def test_batch_job_db_saved_filter_jobs_to_run_empty(test_jobs_db, aws_batch_job):
    job = aws_batch_job
    assert job.status == "SUCCEEDED"
    job_docs = test_jobs_db.save(job)
    assert job_docs == [1]
    jobs = jobs_to_run([job], test_jobs_db)
    assert isinstance(jobs, list)
    assert len(jobs) == 0  # successful jobs are done
