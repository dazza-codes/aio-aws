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
AWS asyncio test fixtures

This test suite is adapted from aiobotocore. This should be used according
to the aiobotocore, botocore and moto licenses.
"""

import asyncio
import random
import shutil
import string
import tempfile
from itertools import chain

import aiobotocore
import aiobotocore.config
import aiohttp
import pytest
from aiobotocore.config import AioConfig
from pytest_aiomoto.aiomoto_services import AWS_HOST
from pytest_aiomoto.aiomoto_services import CONNECT_TIMEOUT


@pytest.fixture(
    scope="session", params=[True, False], ids=["debug[true]", "debug[false]"]
)
def debug(request):
    return request.param


def random_bucketname():
    # 63 is the max bucket length.
    return random_name()


def random_tablename():
    return random_name()


def random_name():
    """Return a string with presumably unique contents

    The string contains only symbols allowed for s3 buckets
    (alphanumeric, dot and hyphen).
    """
    return "".join(random.sample(string.ascii_letters, k=40))


def assert_status_code(response, status_code):
    assert response["ResponseMetadata"]["HTTPStatusCode"] == status_code


async def assert_num_uploads_found(
    aio_s3_client,
    bucket_name,
    operation,
    num_uploads,
    *,
    max_items=None,
    num_attempts=5,
    event_loop
):
    amount_seen = None
    paginator = aio_s3_client.get_paginator(operation)
    for _ in range(num_attempts):
        pages = paginator.paginate(
            Bucket=bucket_name, PaginationConfig={"MaxItems": max_items}
        )
        responses = []
        async for page in pages:
            responses.append(page)
        # It sometimes takes a while for all the uploads to show up,
        # especially if the upload was just created.  If we don't
        # see the expected amount, we retry up to num_attempts time
        # before failing.
        amount_seen = len(responses[0]["Uploads"])
        if amount_seen == num_uploads:
            # Test passed.
            return
        else:
            # Sleep and try again.
            await asyncio.sleep(2, loop=event_loop)
        pytest.fail(
            "Expected to see %s uploads, instead saw: %s" % (num_uploads, amount_seen)
        )


def pytest_configure():
    class AIOUtils:
        def __init__(self):
            self.assert_status_code = assert_status_code
            self.assert_num_uploads_found = assert_num_uploads_found

    pytest.aio = AIOUtils()


@pytest.fixture
def aa_fail_proxy_config(monkeypatch):
    # NOTE: name of this fixture must be alphabetically first to run first
    monkeypatch.setenv("HTTP_PROXY", "http://{}:54321".format(AWS_HOST))
    monkeypatch.setenv("HTTPS_PROXY", "http://{}:54321".format(AWS_HOST))


@pytest.fixture
def aa_succeed_proxy_config(monkeypatch):
    # NOTE: name of this fixture must be alphabetically first to run first
    monkeypatch.setenv("HTTP_PROXY", "http://{}:54321".format(AWS_HOST))
    monkeypatch.setenv("HTTPS_PROXY", "http://{}:54321".format(AWS_HOST))

    # this will cause us to skip proxying
    monkeypatch.setenv("NO_PROXY", "amazonaws.com")


@pytest.fixture
def session(event_loop):
    session = aiobotocore.session.AioSession(loop=event_loop)
    return session


@pytest.fixture
def region():
    return "us-east-1"


@pytest.fixture
def alt_region():
    return "us-west-2"


@pytest.fixture
def signature_version():
    return "s3"


@pytest.fixture
def config(region, signature_version):
    connect_timeout = read_timeout = CONNECT_TIMEOUT

    return AioConfig(
        region_name=region,
        signature_version=signature_version,
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
    )


@pytest.fixture
def mocking_test():
    # change this flag for test with real aio_aws
    # TODO: this should be merged with pytest.mark.moto
    return True


def moto_config(endpoint_url):
    return dict(
        endpoint_url=endpoint_url,
        aws_secret_access_key="aiomoto_AWS_SECRET_ACCESS_KEY",
        aws_access_key_id="aiomoto_AWS_ACCESS_KEY_ID",
    )


def create_client(client_type, request, event_loop, session, region, config, **kw):
    async def f():
        return session.create_client(
            client_type, region_name=region, config=config, **kw
        )

    client = event_loop.run_until_complete(f())

    def fin():
        event_loop.run_until_complete(client.close())

    request.addfinalizer(fin)
    return client


@pytest.fixture
def aio_batch_client(
    request, session, region, config, s3_server, mocking_test, event_loop
):
    kw = {}
    if mocking_test:
        kw = moto_config(s3_server)

    return create_client("s3", request, event_loop, session, region, config, **kw)


@pytest.fixture
def aio_s3_client(
    request, session, region, config, s3_server, mocking_test, event_loop
):
    kw = {}
    if mocking_test:
        kw = moto_config(s3_server)

    return create_client("s3", request, event_loop, session, region, config, **kw)


@pytest.fixture
def alt_s3_client(
    request, session, alt_region, config, s3_server, mocking_test, event_loop
):
    kw = {}
    if mocking_test:
        kw = moto_config(s3_server)

    config.region_name = alt_region
    return create_client("s3", request, event_loop, session, alt_region, config, **kw)


@pytest.fixture
def aio_dynamodb_client(
    request, session, region, config, dynamodb2_server, mocking_test, event_loop
):
    kw = {}
    if mocking_test:
        kw = moto_config(dynamodb2_server)
    client = create_client(
        "dynamodb", request, event_loop, session, region, config, **kw
    )
    return client


@pytest.fixture
def aio_cloudformation_client(
    request, session, region, config, cloudformation_server, mocking_test, event_loop
):
    kw = {}
    if mocking_test:
        kw = moto_config(cloudformation_server)
    client = create_client(
        "cloudformation", request, event_loop, session, region, config, **kw
    )
    return client


@pytest.fixture
def aio_sns_client(
    request, session, region, config, sns_server, mocking_test, event_loop
):
    kw = moto_config(sns_server) if mocking_test else {}
    client = create_client("sns", request, event_loop, session, region, config, **kw)
    return client


@pytest.fixture
def aio_sqs_client(
    request, session, region, config, sqs_server, mocking_test, event_loop
):
    kw = moto_config(sqs_server) if mocking_test else {}
    client = create_client("sqs", request, event_loop, session, region, config, **kw)
    return client


async def recursive_delete(aio_s3_client, bucket_name):
    # Recursively deletes a bucket and all of its contents.
    paginator = aio_s3_client.get_paginator("list_object_versions")
    async for n in paginator.paginate(Bucket=bucket_name, Prefix=""):
        for obj in chain(
            n.get("Versions", []),
            n.get("DeleteMarkers", []),
            n.get("Contents", []),
            n.get("CommonPrefixes", []),
        ):
            kwargs = dict(Bucket=bucket_name, Key=obj["Key"])
            if "VersionId" in obj:
                kwargs["VersionId"] = obj["VersionId"]
            resp = await aio_s3_client.delete_object(**kwargs)
            assert_status_code(resp, 204)

    resp = await aio_s3_client.delete_bucket(Bucket=bucket_name)
    assert_status_code(resp, 204)


@pytest.fixture
async def bucket_name(region, create_bucket):
    name = await create_bucket(region)
    yield name


@pytest.fixture
async def table_name(create_table):
    name = await create_table()
    yield name


@pytest.fixture
async def create_bucket(aio_s3_client):
    _bucket_name = None

    async def _f(region_name, bucket_name=None):
        nonlocal _bucket_name
        if bucket_name is None:
            bucket_name = random_bucketname()
        _bucket_name = bucket_name
        bucket_kwargs = {"Bucket": bucket_name}
        if region_name != "us-east-1":
            bucket_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": region_name,
            }
        response = await aio_s3_client.create_bucket(**bucket_kwargs)
        assert_status_code(response, 200)
        await aio_s3_client.put_bucket_versioning(
            Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"}
        )
        return bucket_name

    try:
        yield _f
    finally:
        await recursive_delete(aio_s3_client, _bucket_name)


@pytest.fixture
async def create_table(aio_dynamodb_client):
    _table_name = None

    async def _is_table_ready(table_name):
        response = await aio_dynamodb_client.describe_table(TableName=table_name)
        return response["Table"]["TableStatus"] == "ACTIVE"

    async def _f(table_name=None):
        nonlocal _table_name
        if table_name is None:
            table_name = random_tablename()
        _table_name = table_name
        table_kwargs = {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "testKey", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "testKey", "KeyType": "HASH"},
            ],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }
        response = await aio_dynamodb_client.create_table(**table_kwargs)
        while not (await _is_table_ready(table_name)):
            pass
        assert_status_code(response, 200)
        return table_name

    try:
        yield _f
    finally:
        await delete_table(aio_dynamodb_client, _table_name)


async def delete_table(aio_dynamodb_client, table_name):
    response = await aio_dynamodb_client.delete_table(TableName=table_name)
    assert_status_code(response, 200)


@pytest.fixture
def tempdir(request):
    tempdir = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(tempdir)

    request.addfinalizer(fin)
    return tempdir


@pytest.fixture
def create_object(aio_s3_client, bucket_name):
    async def _f(key_name, body="foo"):
        r = await aio_s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=body)
        assert_status_code(r, 200)
        return r

    return _f


@pytest.fixture
def create_multipart_upload(request, aio_s3_client, bucket_name, event_loop):
    _key_name = None
    upload_id = None

    async def _f(key_name):
        nonlocal _key_name
        nonlocal upload_id
        _key_name = key_name

        parsed = await aio_s3_client.create_multipart_upload(
            Bucket=bucket_name, Key=key_name
        )
        upload_id = parsed["UploadId"]
        return upload_id

    def fin():
        event_loop.run_until_complete(
            aio_s3_client.abort_multipart_upload(
                UploadId=upload_id, Bucket=bucket_name, Key=_key_name
            )
        )

    request.addfinalizer(fin)
    return _f


@pytest.fixture
async def aio_session(event_loop):
    async with aiohttp.ClientSession(loop=event_loop) as session:
        yield session


@pytest.fixture
def dynamodb_put_item(request, aio_dynamodb_client, table_name):
    async def _f(key_string_value):
        response = await aio_dynamodb_client.put_item(
            TableName=table_name,
            Item={"testKey": {"S": key_string_value}},
        )
        assert_status_code(response, 200)

    return _f


@pytest.fixture
def topic_arn(region, create_topic, aio_sns_client, event_loop):
    arn = event_loop.run_until_complete(create_topic())
    return arn


async def delete_topic(aio_sns_client, topic_arn):
    response = await aio_sns_client.delete_topic(TopicArn=topic_arn)
    assert_status_code(response, 200)


@pytest.fixture
def create_topic(request, aio_sns_client, event_loop):
    _topic_arn = None

    async def _f():
        nonlocal _topic_arn
        response = await aio_sns_client.create_topic(Name=random_name())
        _topic_arn = response["TopicArn"]
        assert_status_code(response, 200)
        return _topic_arn

    def fin():
        event_loop.run_until_complete(delete_topic(aio_sns_client, _topic_arn))

    request.addfinalizer(fin)
    return _f


@pytest.fixture
def create_sqs_queue(request, aio_sqs_client, event_loop):
    _queue_url = None

    async def _f():
        nonlocal _queue_url

        response = await aio_sqs_client.create_queue(QueueName=random_name())
        _queue_url = response["QueueUrl"]
        assert_status_code(response, 200)
        return _queue_url

    def fin():
        event_loop.run_until_complete(delete_sqs_queue(aio_sqs_client, _queue_url))

    request.addfinalizer(fin)
    return _f


@pytest.fixture
def sqs_queue_url(region, create_sqs_queue, aio_sqs_client, event_loop):
    name = event_loop.run_until_complete(create_sqs_queue())
    return name


async def delete_sqs_queue(aio_sqs_client, queue_url):
    response = await aio_sqs_client.delete_queue(QueueUrl=queue_url)
    assert_status_code(response, 200)
