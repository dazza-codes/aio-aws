import pytest

from aio_aws.aws_batch_models import AWSBatchJob


@pytest.fixture
def aws_batch_job() -> AWSBatchJob:
    # TODO: use some kind of faker-db for pydantic models and
    #       create pydantic models or schema for all elements
    job_data = {
        "job_id": "1aac2eab-897a-4b89-9eb3-08986fbb7144",
        "job_name": "sleep-1-job",
        "job_queue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
        "job_submission": {
            "ResponseMetadata": {
                "HTTPHeaders": {
                    "content-length": "75",
                    "content-type": "text/html; charset=utf-8",
                    "date": "Mon, 23 Mar 2020 15:29:33 GMT",
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
                    '/bin/sh -c "for a in `seq 1 '
                    "10`; do echo Hello World; "
                    'sleep 1; done"'
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


PENDING = {
    "jobName": "sleep-1-job",
    "jobId": "f6b11be1-1a92-4999-8628-bf8ff9380106",
    "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
    "status": "PENDING",
    "dependsOn": [],
    "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
}

RUNNABLE = {
    "jobName": "sleep-1-job",
    "jobId": "347a2870-d688-4fa7-bbec-4f9ffd556980",
    "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
    "status": "RUNNABLE",
    "dependsOn": [],
    "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
}

STARTING = {
    "jobName": "sleep-1-job",
    "jobId": "4190b441-0ade-4584-a474-162bb793bf50",
    "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
    "status": "STARTING",
    "dependsOn": [],
    "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
}

RUNNING = {
    "jobName": "sleep-1-job",
    "jobId": "fe93a1e2-b415-43df-bb82-f9081bd8b97b",
    "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
    "status": "RUNNING",
    "startedAt": 1631597959,
    "dependsOn": [],
    "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
}

SUCCEEDED = {
    "jobName": "sleep-1-job",
    "jobId": "e8a69543-2cc3-4412-917d-a189d1567d47",
    "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/moto_test_job_queue",
    "status": "SUCCEEDED",
    "startedAt": 1631598039,
    "stoppedAt": 1631598040,
    "dependsOn": [],
    "jobDefinition": "arn:aws:batch:us-west-2:123456789012:job-definition/moto_test_job_definition:1",
    "container": {
        "vcpus": 1,
        "command": [
            '/bin/sh -c "for a in `seq 1 10`; do echo Hello World; sleep 1; done"'
        ],
        "volumes": [],
        "readonlyRootFilesystem": False,
        "ulimits": [],
        "privileged": False,
        "logStreamName": "moto_test_job_definition/default/e8a69543-2cc3-4412-917d-a189d1567d47",
    },
}


@pytest.fixture
def aws_batch_job_succeeded(aws_batch_job):
    job = AWSBatchJob(**aws_batch_job.db_data)
    job.job_name = "job-SUCCEEDED"
    assert job.status == "SUCCEEDED"
    return job


@pytest.fixture
def aws_batch_job_failed(aws_batch_job):
    job = AWSBatchJob(**aws_batch_job.db_data)
    job.job_name = "job-FAILED"
    job.job_id = aws_batch_job.job_id.replace("08986fbb7144", "08986fbb7145")
    job.job_submission["jobId"] = job.job_id
    job.job_description["status"] = "FAILED"
    job.status = job.job_description["status"]
    return job


@pytest.fixture
def aws_batch_job_starting(aws_batch_job):
    job = AWSBatchJob(**aws_batch_job.db_data)
    job.job_name = "job-RUNNING"
    job.job_id = aws_batch_job.job_id.replace("08986fbb7144", "08986fbb7146")
    job.job_submission["jobId"] = job.job_id
    job.job_description["status"] = "STARTING"
    del job.job_description["startedAt"]
    del job.job_description["stoppedAt"]
    del job.job_description["container"]
    job.status = job.job_description["status"]
    return job


@pytest.fixture
def aws_batch_job_running(aws_batch_job):
    job = AWSBatchJob(**aws_batch_job.db_data)
    job.job_name = "job-RUNNING"
    job.job_id = aws_batch_job.job_id.replace("08986fbb7144", "08986fbb7147")
    job.job_submission["jobId"] = job.job_id
    job.job_description["status"] = "RUNNING"
    del job.job_description["stoppedAt"]
    del job.job_description["container"]
    job.status = job.job_description["status"]
    return job


@pytest.fixture
def aws_batch_jobs(
    aws_batch_job_succeeded, aws_batch_job_failed, aws_batch_job_running
):
    return aws_batch_job_succeeded, aws_batch_job_failed, aws_batch_job_running
