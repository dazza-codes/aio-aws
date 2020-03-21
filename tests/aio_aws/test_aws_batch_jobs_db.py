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
        "job_id": "c4d0be00-03de-4842-af47-9586872c3f53",
        "job_name": "sleep-1-job",
        "job_queue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
        "job_definition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
        "job_description": {
            "jobName": "sleep-1-job",
            "jobId": "c4d0be00-03de-4842-af47-9586872c3f53",
            "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
            "status": "SUCCEEDED",
            "startedAt": 1584818746,
            "stoppedAt": 1584818757,
            "dependsOn": [],
            "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
            "container": {
                "vcpus": 1,
                "command": ["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"],
                "volumes": [],
                "readonlyRootFilesystem": False,
                "ulimits": [],
                "privileged": False,
                "logStreamName": "moto_test_job_definition/default/c4d0be00-03de-4842-af47-9586872c3f53",
            },
        },
        "job_submission": {
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "server": "amazon.com",
                    "content-type": "text/html; charset=utf-8",
                    "content-length": "75",
                    "date": "Sat, 21 Mar 2020 19:25:43 GMT",
                },
                "RetryAttempts": 0,
            },
            "jobName": "sleep-1-job",
            "jobId": "c4d0be00-03de-4842-af47-9586872c3f53",
        },
        "container_overrides": {
            "command": ["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"]
        },
        "command": ["/bin/sh", "-c", "echo Hello && sleep 0.2 && echo Bye"],
        "depends_on": [],
        "status": "SUCCEEDED",
        "num_tries": 1,
        "max_tries": 4,
    }
    return AWSBatchJob(**job_data)


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
