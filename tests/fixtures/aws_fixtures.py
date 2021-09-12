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
AWS test fixtures

This test suite uses a large suite of moto mocks for the AWS batch
infrastructure. These infrastructure mocks are derived from the moto test
suite for testing the batch client. The test infrastructure should be used
according to the moto license.

.. seealso::

    - https://github.com/spulec/moto/pull/1197/files
    - https://github.com/spulec/moto/blob/master/tests/test_batch/test_batch.py
"""
import os
import uuid
from itertools import chain
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional

import boto3
import botocore.waiter
import pytest
from botocore.exceptions import ClientError
from moto import mock_batch
from moto import mock_ec2
from moto import mock_ecs
from moto import mock_iam
from moto import mock_logs
from moto import mock_s3
from moto.core.models import BotocoreStubber

# import moto.settings
# moto.settings.TEST_SERVER_MODE = True

AWS_HOST = "127.0.0.1"
AWS_PORT = "5000"

AWS_REGION = "us-west-2"
AWS_ACCESS_KEY_ID = "test_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "test_AWS_SECRET_ACCESS_KEY"


class S3Object(NamedTuple):
    """
    Just the bucket_name and key for an :code:`s3.ObjectSummary`.
    This simple named tuple should work around problems with :code:`Pickle`
    for an :code:`s3.ObjectSummary`
    """

    bucket: str
    key: str

    @property
    def bucket_name(self) -> str:
        return self.bucket

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def response_success(response: Dict) -> bool:
    """
    Parse a response from a request issued by any botocore.client.BaseClient
    to determine whether the request was successful or not.
    :param response:
    :return: boolean
    :raises: KeyError if the response is not an AWS response
    """
    # If the response dict is not constructed as expected for an AWS response,
    # this should raise a KeyError to indicate something is very wrong.
    status_code = int(response["ResponseMetadata"]["HTTPStatusCode"])
    if status_code:
        # consider 300+ responses to be unsuccessful
        return 200 <= status_code < 300
    else:
        return False


def assert_status_code(response, status_code: int):
    assert (
        int(response.get("ResponseMetadata", {}).get("HTTPStatusCode")) == status_code
    )


def has_moto_mocks(client, event_name):
    # moto registers mock callbacks with the `before-send` event-name, using
    # specific callbacks for the methods that are generated dynamically. By
    # checking that the first callback is a BotocoreStubber, this verifies
    # that moto mocks are intercepting client requests.
    callbacks = client.meta.events._emitter._lookup_cache[event_name]
    if len(callbacks) > 0:
        stub = callbacks[0]
        assert isinstance(stub, BotocoreStubber)
        return stub.enabled
    return False


@pytest.fixture
def aws_host(monkeypatch):
    host = os.getenv("AWS_HOST", AWS_HOST)
    monkeypatch.setenv("AWS_HOST", host)
    yield


@pytest.fixture
def aws_port(monkeypatch):
    port = os.getenv("AWS_PORT", AWS_PORT)
    monkeypatch.setenv("AWS_PORT", port)
    yield


@pytest.fixture
def aws_proxy(aws_host, aws_port, monkeypatch):
    # only required if using a moto stand-alone server or similar local stack
    monkeypatch.setenv("HTTP_PROXY", f"http://{aws_host}:{aws_port}")
    monkeypatch.setenv("HTTPS_PROXY", f"http://{aws_host}:{aws_port}")


@pytest.fixture
def aws_region(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", AWS_REGION)
    yield AWS_REGION


@pytest.fixture
def aws_credentials(aws_region, monkeypatch):
    """Mocked AWS Credentials for moto."""
    boto3.DEFAULT_SESSION = None
    # monkeypatch.setenv("AWS_DEFAULT_PROFILE", "testing")
    # monkeypatch.setenv("AWS_PROFILE", "testing")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    yield


@pytest.fixture(scope="session")
def job_queue_name():
    return "moto_test_job_queue"


@pytest.fixture(scope="session")
def job_definition_name():
    return "moto_test_job_definition"


@pytest.fixture(scope="session")
def compute_env_name():
    return "moto_test_compute_env"


#
# AWS Clients
#


@pytest.fixture
def aws_batch_client(aws_region):
    with mock_batch():
        yield boto3.client("batch", region_name=aws_region)


@pytest.fixture
def aws_ec2_client(aws_region):
    with mock_ec2():
        yield boto3.client("ec2", region_name=aws_region)


@pytest.fixture
def aws_ecs_client(aws_region):
    with mock_ecs():
        yield boto3.client("ecs", region_name=aws_region)


@pytest.fixture
def aws_iam_client(aws_region):
    with mock_iam():
        yield boto3.client("iam", region_name=aws_region)


@pytest.fixture
def aws_logs_client(aws_region):
    with mock_logs():
        yield boto3.client("logs", region_name=aws_region)


@pytest.fixture
def aws_s3_client(aws_region):
    with mock_s3():
        yield boto3.client("s3", region_name=aws_region)


@pytest.fixture
def aws_s3_resource(aws_region):
    with mock_s3():
        yield boto3.resource("s3", region_name=aws_region)


##################################################################
#
# Generic S3 bucket and key fixtures
#


def assert_bucket_200(bucket, s3_client):
    head = s3_client.head_bucket(Bucket=bucket)
    assert_status_code(head, 200)


def assert_bucket_404(bucket, s3_client):
    try:
        head = s3_client.head_bucket(Bucket=bucket)
        assert_status_code(head, 404)
    except botocore.exceptions.ClientError as err:
        resp = err.response
        assert_status_code(resp, 404)


def assert_object_200(bucket, key, s3_client):
    head = s3_client.head_object(Bucket=bucket, Key=key)
    assert_status_code(head, 200)


def assert_object_404(bucket, key, s3_client):
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        assert_status_code(head, 404)
    except botocore.exceptions.ClientError as err:
        resp = err.response
        assert_status_code(resp, 404)


def create_s3_bucket(bucket_name, s3_client, aws_region):
    resp = s3_client.create_bucket(
        Bucket=bucket_name,
        ACL="public-read-write",
        CreateBucketConfiguration={"LocationConstraint": aws_region},
    )
    assert response_success(resp)
    exists_waiter = s3_client.get_waiter("bucket_exists")
    exists_waiter.wait(Bucket=bucket_name)
    head = s3_client.head_bucket(Bucket=bucket_name)
    assert response_success(head)


def create_s3_bucket_resource(bucket_name, s3_resource, aws_region) -> "s3.Bucket":
    bucket = s3_resource.create_bucket(
        Bucket=bucket_name,
        ACL="public-read-write",
        CreateBucketConfiguration={"LocationConstraint": aws_region},
    )
    bucket.wait_until_exists()
    s3_client = s3_resource.meta.client
    head = s3_client.head_bucket(Bucket=bucket_name)
    assert response_success(head)
    return bucket


def create_s3_object(
    bucket_name, key, object_body, s3_resource, s3_client
) -> "s3.ObjectSummary":
    bucket = s3_resource.Bucket(bucket_name)
    s3_obj = bucket.put_object(Key=key, Body=object_body)
    exists_waiter = s3_client.get_waiter("object_exists")
    exists_waiter.wait(Bucket=bucket_name, Key=key)
    head = s3_client.head_object(Bucket=bucket_name, Key=key)
    assert response_success(head)
    return s3_obj


def delete_s3_bucket(bucket_name, s3_client):
    # Recursively deletes a bucket and all of its contents.

    try:
        # - ensure the s3-client is loaded with moto mocks
        # - never allow this to apply to live s3 resources
        # - the event-name mocks are dynamically generated after calling the method
        assert has_moto_mocks(s3_client, "before-send.s3.HeadBucket")

        paginator = s3_client.get_paginator("list_object_versions")

        for n in paginator.paginate(Bucket=bucket_name, Prefix=""):
            for obj in chain(
                n.get("Versions", []),
                n.get("DeleteMarkers", []),
                n.get("Contents", []),
                n.get("CommonPrefixes", []),
            ):
                kwargs = dict(Bucket=bucket_name, Key=obj["Key"])
                if "VersionId" in obj:
                    kwargs["VersionId"] = obj["VersionId"]
                resp = s3_client.delete_object(**kwargs)
                assert response_success(resp)

        resp = s3_client.delete_bucket(Bucket=bucket_name)
        assert response_success(resp)
        # Ensure the bucket is gone
        waiter = s3_client.get_waiter("bucket_not_exists")
        waiter.wait(Bucket=bucket_name)
        try:
            head = s3_client.head_bucket(Bucket=bucket_name)
            assert_status_code(head, 404)
        except ClientError as err:
            resp = err.response
            assert_status_code(resp, 404)

    except ClientError as err:
        print(f"COULD NOT CLEANUP S3 BUCKET: {bucket_name}")
        print(err)


def delete_s3_bucket_resource(bucket_name, s3_resource):
    delete_s3_bucket(bucket_name, s3_resource.meta.client)


def delete_s3_prefix(s3_client, bucket_name, prefix):
    try:
        # - ensure the s3-client is loaded with moto mocks
        # - never allow this to apply to live s3 resources
        # - the event-name mocks are dynamically generated after calling the method
        assert has_moto_mocks(s3_client, "before-send.s3.HeadBucket")
        paginator = s3_client.get_paginator("list_objects_v2")
        for objects in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if objects["KeyCount"]:
                keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
                s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": keys})
    except ClientError as err:
        print(f"COULD NOT CLEANUP S3 PREFIX: s3://{bucket_name}/{prefix}")
        print(err)


@pytest.fixture
def s3_protocol() -> str:
    """An s3:// protocol prefix"""
    return "s3://"


@pytest.fixture
def s3_bucket_name() -> str:
    """
    A valid S3 bucket name with a UUID suffix.

    This bucket may not exist yet; for a bucket that exists, use
    the `s3_bucket` fixture instead.  It creates a moto-bucket
    for this bucket name.

    :return: str for the bucket component of 's3://{bucket}/{key}'
    """
    return "moto-bucket-" + str(uuid.uuid4())


@pytest.fixture
def s3_bucket(s3_bucket_name, aws_s3_client, aws_region) -> str:
    """
    The s3_bucket fixture provides a moto-bucket for the s3_bucket_name
    fixture, where the moto-bucket is cleaned up on exit.
    :return: the s3_bucket_name
    """
    with mock_s3():
        create_s3_bucket(s3_bucket_name, aws_s3_client, aws_region)
        yield s3_bucket_name
        delete_s3_bucket(s3_bucket_name, aws_s3_client)


@pytest.fixture
def s3_bucket_resource(s3_bucket_name, aws_s3_resource, aws_region) -> "s3.Bucket":
    """
    The s3_bucket fixture provides a moto-bucket for the s3_bucket_name
    fixture, where the moto-bucket is cleaned up on exit.
    :return: the s3.Bucket resource
    """
    with mock_s3():
        bucket = create_s3_bucket_resource(s3_bucket_name, aws_s3_resource, aws_region)
        yield bucket
        delete_s3_bucket_resource(s3_bucket_name, aws_s3_resource)


@pytest.fixture
def s3_buckets(s3_bucket_name, aws_s3_client, aws_region) -> List[str]:
    """
    The s3_buckets fixture creates moto-buckets for the s3_bucket_name
    fixture, with a numeric suffix for each bucket, where the
    moto-buckets are cleaned up on exit.
    :return: a list of bucket names
    """
    with mock_s3():
        bucket_names = []
        for i in range(10):
            bucket_name = f"{s3_bucket_name}-{i:02d}"
            create_s3_bucket(bucket_name, aws_s3_client, aws_region)
            bucket_names.append(bucket_name)

        yield bucket_names

        for bucket_name in bucket_names:
            delete_s3_bucket(bucket_name, aws_s3_client)


@pytest.fixture(scope="session")
def s3_key_path() -> str:
    """A valid S3 key name that is not a file name, it's like a directory
    The key component of 's3://{bucket}/{key}' that is composed of '{key_path}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return "s3_key_path"


@pytest.fixture(scope="session")
def s3_key_file() -> str:
    """A valid S3 key name that is also a file name
    The key component of 's3://{bucket}/{key}' that is composed of '{key_file}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return "s3_file_test.txt"


@pytest.fixture(scope="session")
def s3_key(s3_key_path, s3_key_file) -> str:
    """A valid S3 key composed of a key_path and a key_file
    The key component of 's3://{bucket}/{key}' that is composed of '{key_path}/{key_file}';
    the key does not begin or end with any delimiters (e.g. '/')
    :return: str for the key component of 's3://{bucket}/{key}'
    """
    return f"{s3_key_path}/{s3_key_file}"


@pytest.fixture(scope="session")
def s3_object_text() -> str:
    """s3 object data: 's3 test object text\n'"""
    return "s3 test object text\n"


@pytest.fixture
def s3_uri_str(s3_protocol, s3_bucket_name, s3_key) -> str:
    """A valid S3 URI comprised of 's3://{bucket}/{key}'

    This s3_uri_str may not exist yet; for an object that exists, use
    the `s3_uri_object` fixture instead.

    :return: str
    """
    return f"{s3_protocol}{s3_bucket_name}/{s3_key}"


@pytest.fixture
def s3_uri_object(
    aws_s3_client, aws_s3_resource, aws_region, s3_bucket_name, s3_key, s3_object_text
) -> S3Object:
    """
    The s3_uri_object fixture creates a moto-bucket and moto-object for
    the s3_uri_str fixture, where the moto-bucket is cleaned up on exit.
    :return: an S3Object(bucket=s3_bucket_name, key=s3_key)
    """
    with mock_s3():
        create_s3_bucket(s3_bucket_name, aws_s3_client, aws_region)
        create_s3_object(
            s3_bucket_name, s3_key, s3_object_text, aws_s3_resource, aws_s3_client
        )
        yield S3Object(bucket=s3_bucket_name, key=s3_key)
        delete_s3_bucket(s3_bucket_name, aws_s3_client)


@pytest.fixture
def s3_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def s3_dynamic_uri(s3_bucket, s3_bucket_name, s3_key_path, s3_uuid) -> S3Object:
    uuid_prefix = f"{s3_key_path}/{s3_uuid}"
    yield S3Object(bucket=s3_bucket_name, key=uuid_prefix)


@pytest.fixture
def s3_temp_dir(s3_key_path, s3_uuid) -> str:
    return f"{s3_key_path}/{s3_uuid}"


@pytest.fixture
def s3_temp_file(
    aws_s3_client, aws_s3_resource, s3_bucket, s3_temp_dir, s3_uuid
) -> S3Object:
    file_path = f"{s3_temp_dir}/{s3_uuid}.txt"
    aws_s3_resource.Object(s3_bucket, file_path).put(Body="foo".encode())

    yield S3Object(bucket=s3_bucket, key=file_path)

    delete_s3_prefix(aws_s3_client, s3_bucket, s3_temp_dir)


@pytest.fixture
def s3_temp_objects(
    aws_s3_client, aws_s3_resource, s3_bucket, s3_temp_dir, s3_uuid
) -> List[S3Object]:
    """
    This creates 10 files, 5 with .txt and 5 with .tif file extensions,
    below the s3://s3_bucket/s3_temp_dir path
    """
    # Since a mock_s3 context is created by the s3_bucket
    # and aws_s3_client fixtures, it is not required here.

    file_key = f"{s3_temp_dir}/{s3_uuid}.txt"
    s3_file = create_s3_object(
        s3_bucket, file_key, s3_uuid, aws_s3_resource, aws_s3_client
    )
    s3_objects = [s3_file]

    files_prefix = f"{s3_temp_dir}/{s3_uuid}"
    for i in range(10):
        file_stem = f"{files_prefix}_{i:04d}"
        if i % 2 > 0:
            key = f"{file_stem}.txt"
        else:
            key = f"{file_stem}.tif"
        body = file_stem.encode()
        s3_obj = create_s3_object(s3_bucket, key, body, aws_s3_resource, aws_s3_client)
        s3_objects.append(s3_obj)

    # create a sub-key path for derivative files
    derivative_path = str(uuid.uuid4())
    derivative_prefix = f"{s3_temp_dir}/{derivative_path}/{s3_uuid}"
    for i in range(10):
        file_stem = f"{derivative_prefix}_{i:04d}"
        if i % 2 > 0:
            key = f"{file_stem}.txt"
        else:
            key = f"{file_stem}.tif"
        body = file_stem.encode()
        s3_obj = create_s3_object(s3_bucket, key, body, aws_s3_resource, aws_s3_client)
        s3_objects.append(s3_obj)

    s3_objects = [
        S3Object(bucket=s3_obj.bucket_name, key=s3_obj.key) for s3_obj in s3_objects
    ]

    yield s3_objects

    delete_s3_prefix(aws_s3_client, s3_bucket, s3_temp_dir)


@pytest.fixture
def s3_temp_1000s_objects(
    aws_s3_client, aws_s3_resource, s3_bucket, s3_temp_dir, s3_uuid
) -> List[S3Object]:
    """
    This creates 1010 files, half with .txt and others with .tif file extensions,
    below the s3://s3_bucket/s3_temp_dir path; the default page limit for s3
    object listings is usually 1000, so this should exceed 1 page.
    """
    # Since a mock_s3 context is created by the s3_bucket
    # and aws_s3_client fixtures, it is not required here.
    file_key = f"{s3_temp_dir}/{s3_uuid}.txt"
    s3_file = create_s3_object(
        s3_bucket, file_key, s3_uuid, aws_s3_resource, aws_s3_client
    )
    s3_objects = [s3_file]

    for i in range(1010):
        if i % 2 > 0:
            key = f"{s3_temp_dir}/{s3_uuid}_{i:04d}.txt"
        else:
            key = f"{s3_temp_dir}/{s3_uuid}_{i:04d}.tif"
        body = f"{s3_uuid}-{i:04d}".encode()
        s3_obj = create_s3_object(s3_bucket, key, body, aws_s3_resource, aws_s3_client)
        s3_objects.append(s3_obj)

    s3_objects = [
        S3Object(bucket=s3_obj.bucket_name, key=s3_obj.key) for s3_obj in s3_objects
    ]

    yield s3_objects

    delete_s3_prefix(aws_s3_client, s3_bucket, s3_temp_dir)


#
# Batch Infrastructure
#


class AwsBatchClients(NamedTuple):
    batch: "botocore.client.Batch"
    ec2: "botocore.client.EC2"
    ecs: "botocore.client.ECS"
    iam: "botocore.client.IAM"
    logs: "botocore.client.CloudWatchLogs"
    region: str


@pytest.fixture
def aws_batch_clients(
    aws_batch_client,
    aws_ec2_client,
    aws_ecs_client,
    aws_iam_client,
    aws_logs_client,
    aws_region,
):
    return AwsBatchClients(
        batch=aws_batch_client,
        ec2=aws_ec2_client,
        ecs=aws_ecs_client,
        iam=aws_iam_client,
        logs=aws_logs_client,
        region=aws_region,
    )


class AwsBatchInfrastructure:
    aws_region: str
    aws_clients: AwsBatchClients
    vpc_id: Optional[str] = None
    subnet_id: Optional[str] = None
    security_group_id: Optional[str] = None
    iam_arn: Optional[str] = None
    compute_env_name: Optional[str] = None
    compute_env_arn: Optional[str] = None
    job_queue_name: Optional[str] = None
    job_queue_arn: Optional[str] = None
    job_definition_name: Optional[str] = None
    job_definition_arn: Optional[str] = None


def batch_infrastructure(
    aws_clients: AwsBatchClients,
    compute_env_name: str,
    job_queue_name: str,
    job_definition_name: str,
) -> AwsBatchInfrastructure:
    """
    Create AWS Batch infrastructure, including:
    - VPC with subnet
    - Security group and IAM role
    - Batch compute environment and job queue
    - Batch job job_definition

    This function is not a fixture so that tests can pass the AWS clients to it and then
    continue to use the infrastructure created by it while the client fixtures are in-tact for
    the duration of a test.
    """

    infrastructure = AwsBatchInfrastructure()
    infrastructure.aws_region = aws_clients.region
    infrastructure.aws_clients = aws_clients

    resp = aws_clients.ec2.create_vpc(CidrBlock="172.30.0.0/24")
    vpc_id = resp["Vpc"]["VpcId"]

    resp = aws_clients.ec2.create_subnet(
        AvailabilityZone=f"{aws_clients.region}a",
        CidrBlock="172.30.0.0/25",
        VpcId=vpc_id,
    )
    subnet_id = resp["Subnet"]["SubnetId"]

    resp = aws_clients.ec2.create_security_group(
        Description="moto_test_sg_desc", GroupName="moto_test_sg", VpcId=vpc_id
    )
    sg_id = resp["GroupId"]

    resp = aws_clients.iam.create_role(
        RoleName="MotoTestRole", AssumeRolePolicyDocument="moto_test_policy"
    )
    iam_arn = resp["Role"]["Arn"]

    resp = aws_clients.batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="UNMANAGED",
        state="ENABLED",
        serviceRole=iam_arn,
    )
    compute_env_arn = resp["computeEnvironmentArn"]

    resp = aws_clients.batch.create_job_queue(
        jobQueueName=job_queue_name,
        state="ENABLED",
        priority=123,
        computeEnvironmentOrder=[{"order": 123, "computeEnvironment": compute_env_arn}],
    )
    assert resp["jobQueueName"] == job_queue_name
    assert resp["jobQueueArn"]
    job_queue_arn = resp["jobQueueArn"]

    resp = aws_clients.batch.register_job_definition(
        jobDefinitionName=job_definition_name,
        type="container",
        containerProperties={
            "image": "busybox",
            "vcpus": 2,
            "memory": 8,
            "command": ["sleep", "10"],  # NOTE: job runs for 10 sec without overrides
        },
    )
    assert resp["jobDefinitionName"] == job_definition_name
    assert resp["jobDefinitionArn"]
    job_definition_arn = resp["jobDefinitionArn"]
    assert resp["revision"]
    assert resp["jobDefinitionArn"].endswith(
        "{0}:{1}".format(resp["jobDefinitionName"], resp["revision"])
    )

    infrastructure.vpc_id = vpc_id
    infrastructure.subnet_id = subnet_id
    infrastructure.security_group_id = sg_id
    infrastructure.iam_arn = iam_arn
    infrastructure.compute_env_name = compute_env_name
    infrastructure.compute_env_arn = compute_env_arn
    infrastructure.job_queue_name = job_queue_name
    infrastructure.job_queue_arn = job_queue_arn
    infrastructure.job_definition_name = job_definition_name
    infrastructure.job_definition_arn = job_definition_arn
    return infrastructure
