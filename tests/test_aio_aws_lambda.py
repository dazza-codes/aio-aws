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
Test Asyncio AWS Lambda

"""
import inspect
import io
import json
import zipfile

import botocore.client
import botocore.exceptions
import pytest
from async_generator import asynccontextmanager

from aio_aws import aio_aws_lambda
from aio_aws import AioAWSConfig
from aio_aws import response_success
from aio_aws.aio_aws_lambda import AWSLambdaFunction


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
                    "iam", endpoint_url=aio_aws_iam_server
                ) as client:
                    yield client
            if service == "lambda":
                async with aio_aws_session.create_client(
                    "lambda", endpoint_url=aio_aws_lambda_server
                ) as client:
                    yield client
            if service == "logs":
                async with aio_aws_session.create_client(
                    "logs", endpoint_url=aio_aws_logs_server
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


@pytest.fixture
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


def _process_lambda(func_str) -> bytes:
    zip_output = io.BytesIO()
    zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
    zip_file.writestr("lambda_function.py", func_str)
    zip_file.close()
    zip_output.seek(0)
    return zip_output.read()


@pytest.fixture
def aws_lambda_zip() -> bytes:
    lambda_src = """
def lambda_handler(event, context):
    print(f"event: {event}")
    return {"statusCode": 200, "body": event}
"""
    return _process_lambda(lambda_src)


@pytest.fixture
async def aws_lambda_func(
    aws_lambda_zip, lambda_config, lambda_iam_role
) -> AWSLambdaFunction:

    event = {"i": 1}
    payload = json.dumps(event).encode()
    func = AWSLambdaFunction(name="lambda_dev", payload=payload)

    async with lambda_config.create_client("lambda") as client:
        response = await client.create_function(
            FunctionName=func.name,
            Runtime="python3.6",
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
# @pytest.mark.skip("https://github.com/aio-libs/aiobotocore/issues/793")
@pytest.mark.asyncio
async def test_async_lambda_invoke(
    aws_lambda_zip, aws_lambda_func, lambda_iam_role, lambda_config
):

    func: AWSLambdaFunction = aws_lambda_func

    response = await func.invoke(lambda_config)
    assert response_success(response)
    # A successful response could have handled errors
    if func.content is None:
        assert func.error
    else:
        assert func.content

    # since this function should work, test the response data
    assert func.content == {"statusCode": 200, "body": {"i": 1}}
