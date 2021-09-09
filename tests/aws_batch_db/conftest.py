import pytest

from aio_aws.aio_aws_batch import AWSBatchJob


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
