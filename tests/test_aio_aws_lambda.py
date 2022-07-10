# Copyright 2019-2022 Darren Weber
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
Test Asyncio AWS Lambda

"""
import inspect
import io
import json
import zipfile
from contextlib import asynccontextmanager
from typing import Dict

import botocore.client
import botocore.exceptions
import pytest
import pytest_asyncio

from aio_aws import aio_aws_lambda
from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_lambda import AWSLambdaFunction
from aio_aws.logger import get_logger
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)


def test_async_aws_lambda():
    assert inspect.ismodule(aio_aws_lambda)


@pytest.fixture
def lambda_config(
    aio_aws_session,
    aio_aws_iam_server,
    aio_aws_lambda_server,
    aio_aws_logs_server,
) -> AioAWSConfig:
    class TestConfig(AioAWSConfig):
        session = aio_aws_session

        @asynccontextmanager
        async def create_client(self, service: str):
            if service == "iam":
                async with aio_aws_session.create_client(
                    "iam", endpoint_url=aio_aws_iam_server.endpoint_url
                ) as client:
                    yield client
            if service == "lambda":
                async with aio_aws_session.create_client(
                    "lambda", endpoint_url=aio_aws_lambda_server.endpoint_url
                ) as client:
                    yield client
            if service == "logs":
                async with aio_aws_session.create_client(
                    "logs", endpoint_url=aio_aws_logs_server.endpoint_url
                ) as client:
                    yield client

    config = TestConfig(
        max_pool_connections=1,
        min_pause=0.2,
        max_pause=0.6,
        min_jitter=0.1,
        max_jitter=0.2,
    )

    yield config


@pytest_asyncio.fixture
async def lambda_iam_role(lambda_config):

    async with lambda_config.create_client("iam") as client:
        try:
            response = await client.get_role(RoleName="my-role")
            iam = response["Role"]["Arn"]
        except botocore.client.ClientError:
            response = await client.create_role(
                RoleName="my-role",
                AssumeRolePolicyDocument="some policy",
                Path="/my-path/",
            )
            iam = response["Role"]["Arn"]
    return iam


def _zip_lambda(func_str) -> bytes:
    zip_output = io.BytesIO()
    zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
    zip_file.writestr("lambda_function.py", func_str)
    zip_file.close()
    zip_output.seek(0)
    return zip_output.read()


def lambda_handler(event, context):
    import sys

    print(f"event: {event}")
    action = event.get("action")
    if action == "too-large":
        x = ["xxx" for x in range(10 ** 6)]
        assert sys.getsizeof(x) > 6291556
        return {"statusCode": 200, "body": x}
    if action == "runtime-error":
        raise RuntimeError(action)
    return {"statusCode": 200, "body": event}


@pytest.fixture
def aws_lambda_zip() -> bytes:
    lambda_src = """
import sys

def lambda_handler(event, context):
    print(f"event: {event}")
    action = event.get("action")
    if action == "too-large":
        x = ["xxx" for x in range(10 ** 6)]
        assert sys.getsizeof(x) > 6291556
        return {"statusCode": 200, "body": x}
    if action == "runtime-error":
        raise RuntimeError(action)
    return {"statusCode": 200, "body": event}
"""
    return _zip_lambda(lambda_src)


@pytest_asyncio.fixture
async def aws_lambda_func(
    aws_lambda_zip, lambda_config, lambda_iam_role
) -> AWSLambdaFunction:

    func = AWSLambdaFunction(name="lambda_dev")

    async with lambda_config.create_client("lambda") as client:
        response = await client.create_function(
            FunctionName=func.name,
            Runtime="python3.7",
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": aws_lambda_zip},
            Role=lambda_iam_role,
            Description="lambda_dev function",
            Timeout=10,
            MemorySize=128,
            Publish=True,
        )
        assert response_success(response)

    return func


# see also https://github.com/spulec/moto/blob/master/tests/test_awslambda/test_lambda.py
# https://github.com/spulec/moto/issues/2886
@pytest.mark.asyncio
async def test_async_lambda_invoke_success(aws_lambda_func, lambda_config):

    func: AWSLambdaFunction = aws_lambda_func

    event = {"i": 0}
    payload = json.dumps(event).encode()
    func.payload = payload

    async with lambda_config.create_client("lambda") as lambda_client:

        lambda_func = await func.invoke(lambda_config, lambda_client)
        assert id(lambda_func) == id(func)
        assert response_success(lambda_func.response)
        # A successful response could have handled errors
        if lambda_func.data is None:
            assert lambda_func.error
        else:
            assert lambda_func.data

        # since this function should work, test the response data
        assert lambda_func.json == {"statusCode": 200, "body": event}
        assert lambda_func.content_type == "application/json"
        assert lambda_func.content_length > 0
        assert lambda_func.status_code == 200


@pytest.mark.skip("https://github.com/spulec/moto/issues/3988")
@pytest.mark.asyncio
async def test_async_lambda_invoke_too_large(aws_lambda_func, lambda_config):
    # TODO: see also stub issue at https://github.com/aio-libs/aiobotocore/issues/781

    func: AWSLambdaFunction = aws_lambda_func

    event = {"action": "too-large"}
    func.payload = json.dumps(event).encode()

    lambda_error = {
        "errorMessage": "Response payload size exceeded maximum allowed payload size (6291556 bytes).",
        "errorType": "Function.ResponseSizeTooLarge",
    }

    async with lambda_config.create_client("lambda") as lambda_client:

        lambda_func = await func.invoke(lambda_config, lambda_client)
        assert id(lambda_func) == id(func)
        assert not response_success(lambda_func.response)
        assert lambda_func.json is None
        assert lambda_func.error == lambda_error


@pytest.mark.asyncio
async def test_async_lambda_invoke_error(aws_lambda_func, lambda_config):

    func: AWSLambdaFunction = aws_lambda_func

    event = {"action": "runtime-error"}
    func.payload = json.dumps(event).encode()
    lambda_config.retries = 0

    async with lambda_config.create_client("lambda") as lambda_client:

        lambda_func = await func.invoke(lambda_config, lambda_client)
        assert id(lambda_func) == id(func)
        # Note that Lambda still has a 200 response code for errors
        assert lambda_func.status_code == 200
        assert response_success(lambda_func.response)
        assert lambda_func.json is None
        assert lambda_func.content_length > 0

        assert isinstance(lambda_func.error, Dict)
        assert lambda_func.error.get("errorType") == "RuntimeError"
        assert lambda_func.error.get("errorMessage") == "runtime-error"
        assert lambda_func.error.get("stackTrace")
