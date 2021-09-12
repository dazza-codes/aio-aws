#! /usr/bin/env python3

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
AioAWS Lambda
-------------

Manually create a 'lambda_dev' function with simple code like:

.. code-block::

    # lambda_dev/lambda_function.py

    import json

    def lambda_handler(event, context):
        csv = ", ".join([str(i) for i in range(10)])
        print(csv)  # to log something
        return {
            'statusCode': 200,
            'body': json.dumps(csv)
        }

The `aio_aws_lambda.py` module has a main script to call this simple lambda function.
This lambda function easily runs within a 128 Mb memory allocation and it runs in
less time than the minimal billing period, so it's as cheap as it can be and could
fit within a monthly free quota.

.. seealso::
    - https://aws.amazon.com/lambda/pricing/

.. code-block::

    $ ./aio_aws/aio_aws_lambda.py
    Test async lambda functions
    [INFO]  2020-04-07T03:10:21.741Z  aio-aws:aio_lambda_invoke:114  AWS Lambda (lambda_dev) invoked OK
    1 lambdas finished in 0.31 seconds.

    $ N_LAMBDAS=1000 ./aio_aws/aio_aws_lambda.py
    # snipped logging messages
    1000 lambdas finished in 14.95 seconds.

"""

import asyncio
import base64
import concurrent.futures
import json
import os
import time
from dataclasses import dataclass
from functools import partial
from json import JSONDecodeError
from typing import Dict
from typing import List
from typing import Optional

import aiobotocore.client
import botocore.exceptions

from aio_aws.aio_aws_config import MAX_POOL_CONNECTIONS
from aio_aws.aio_aws_config import RETRY_EXCEPTIONS
from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_config import jitter
from aio_aws.logger import get_logger
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)

MAXIMUM_PAYLOAD_SIZE = 6291556


@dataclass
class AWSLambdaFunction:
    """
    AWS Lambda Function

    Creating an AWSLambdaFunction instance does not create or invoke anything, it's
    a dataclass to retain and track function parameters and response data.

    :param name: A lambda FunctionName (truncated to 64 characters).
    :param type: the type of function invocation ("RequestResponse", "Event", "DryRun")
    :param log_type: the type of function logging ("None" or "Tail")
    :param context: the client context; for an example of a ClientContext JSON,
        see PutEvents in the Amazon Mobile Analytics API Reference and User Guide. The
        ClientContext JSON must be base64-encoded and has a maximum size of 3583 bytes.
    :param payload: an input payload (bytes or seekable file-like object); JSON that you
        want to provide to your Lambda function as input.
    :param qualifier: an optional function version or alias name;
        If you specify a function version, the API uses the qualified
        function ARN to invoke a specific Lambda function. If you specify an alias
        name, the API uses the alias ARN to invoke the Lambda function version to
        which the alias points. If you don't provide this parameter, then the API
        uses unqualified function ARN which results in invocation of the $LATEST version.
    """

    name: str
    type: str = "RequestResponse"  # or "Event" or "DryRun"
    log_type: str = "None"  # "None" or "Tail"
    context: str = None
    payload: bytes = b""  # or a file
    qualifier: str = None
    response: Dict = None
    data: bytes = None

    TYPES = ["RequestResponse", "Event", "DryRun"]
    LOG_TYPES = ["None", "Tail"]

    def __post_init__(self):

        self.name = self.name[:64]

        if self.type not in self.TYPES:
            raise ValueError(f"The type ({self.type}) must be one of {self.TYPES}")

        if self.type != "RequestResponse":
            self.log_type = "None"
        elif self.log_type not in self.LOG_TYPES:
            raise ValueError(
                f"The log_type ({self.log_type}) must be one of {self.LOG_TYPES}"
            )

    @property
    def response_metadata(self) -> Optional[Dict]:
        if self.response:
            return self.response.get("ResponseMetadata")

    @property
    def response_headers(self) -> Optional[Dict]:
        metadata = self.response_metadata
        if metadata:
            return metadata.get("HTTPHeaders")

    @property
    def content_type(self) -> Optional[str]:
        headers = self.response_headers
        if headers:
            return headers.get("content-type")

    @property
    def content_length(self) -> Optional[int]:
        headers = self.response_headers
        if headers:
            return int(headers.get("content-length"))

    @property
    def status_code(self) -> Optional[int]:
        if self.response:
            return self.response.get("StatusCode")

    @property
    def error(self):
        """This could be a lambda function error or a client error"""
        if self.response:
            if self.response.get("FunctionError"):
                # lambda function error
                if self.data:
                    error = self.data.decode()
                    try:
                        error = json.loads(error)
                    except JSONDecodeError:
                        pass
                    return error

            if self.response.get("Error"):
                # boto3 client error
                return self.response.get("Error")

    @property
    def text(self):
        if self.response and not self.error:
            if self.data:
                return self.data.decode()

    @property
    def json(self):
        text = self.text
        if text:
            return json.loads(text)

    @property
    def logs(self):
        if self.response:
            log_result = self.response.get("LogResult")
            if log_result:
                return base64.b64decode(log_result)

    @property
    def params(self):
        """AWS Lambda parameters to invoke function"""
        params = {
            "FunctionName": self.name,
            "InvocationType": self.type,
            "LogType": self.log_type,
        }
        if self.context:
            params["ClientContext"] = self.context
        if self.payload:
            params["Payload"] = self.payload
        if self.qualifier:
            params["Qualifier"] = self.qualifier
        return params

    async def invoke(
        self, config: AioAWSConfig, lambda_client: aiobotocore.client.AioBaseClient
    ) -> "AWSLambdaFunction":
        """
        Asynchronous coroutine to invoke a lambda function; this
        updates the ``response`` and calls the py:meth:`.read_response`
        method to handle the response.

        :param config: aio session and client settings
        :param lambda_client: aio client for lambda
        :return: a lambda response
        :raises: botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError
        """
        async with config.semaphore:
            for tries in range(config.retries + 1):
                try:
                    LOGGER.debug("AWS Lambda params: %s", self.params)
                    response = await lambda_client.invoke(**self.params)
                    self.response = response
                    LOGGER.debug("AWS Lambda response: %s", self.response)

                    if response_success(self.response):
                        await self.read_response()  # updates self.data
                        if self.data:
                            LOGGER.info("AWS Lambda invoked OK: %s", self.name)
                        else:
                            error = self.error
                            if error:
                                LOGGER.error(
                                    "AWS Lambda error: %s, %s", self.name, error
                                )
                    else:
                        # TODO: are there some failures that could be recovered here?
                        LOGGER.error("AWS Lambda invoke failure: %s", self.name)

                    return self

                except botocore.exceptions.ClientError as err:
                    response = err.response
                    LOGGER.warning(
                        "AWS Lambda client error: %s, %s", self.name, response
                    )
                    error = response.get("Error", {})
                    if error.get("Code") in RETRY_EXCEPTIONS:
                        if tries < config.retries:
                            await jitter(
                                "lambda-retry", config.min_jitter, config.max_jitter
                            )
                            continue  # allow it to retry, if possible
                        else:
                            LOGGER.error(
                                "AWS Lambda too many retries: %s, %s",
                                self.name,
                                response,
                            )
                    else:
                        LOGGER.error(
                            "AWS Lambda client error: %s, %s", self.name, response
                        )
                        self.response = response
                        raise

            raise RuntimeError("AWS Lambda invoke exceeded retries")

    async def read_response(self):
        """
        Asynchronous coroutine to read a lambda response; this
        updates the ``data`` attribute.

        :raises: botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError
        """
        if self.response and response_success(self.response):
            response_payload = self.response.get("Payload")
            if response_payload:
                async with response_payload as stream:
                    self.data = await stream.read()


async def run_lambda_functions(lambda_functions: List[AWSLambdaFunction]):
    """Use some default config settings to run lambda functions

    .. code-block::

        lambda_funcs = []
        for i in range(5):
            event = {"i": i}
            payload = json.dumps(event).encode()
            func = AWSLambdaFunction(name="lambda_dev", payload=payload)
            lambda_funcs.append(func)
        asyncio.run(run_lambda_functions(lambda_funcs))
        for func in lambda_funcs:
            assert response_success(func.response)

    :return: it returns nothing, the lambda function will contain the
        results of any lambda response in func.response and func.data
    """
    config = AioAWSConfig(
        retries=0,
        aws_region=os.getenv("AWS_DEFAULT_REGION", "us-west-2"),
        min_jitter=0.2,
        max_jitter=0.8,
        max_pool_connections=MAX_POOL_CONNECTIONS,
    )
    lambda_tasks = []
    async with config.create_client("lambda") as lambda_client:
        for lambda_func in lambda_functions:
            lambda_task = asyncio.create_task(lambda_func.invoke(config, lambda_client))
            lambda_tasks.append(lambda_task)
        await asyncio.gather(*lambda_tasks)


async def run_lambda_function_thread_pool(
    lambda_functions: List[AWSLambdaFunction], n_tasks: int = 4
):
    config = AioAWSConfig(
        retries=0,
        aws_region=os.getenv("AWS_DEFAULT_REGION", "us-west-2"),
        min_jitter=0.2,
        max_jitter=0.8,
        max_pool_connections=MAX_POOL_CONNECTIONS,
    )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        lambda_tasks = []

        async with config.create_client("lambda") as lambda_client:
            for lambda_func in lambda_functions:
                func = partial(
                    lambda_func.invoke,
                    config=config,
                    lambda_client=lambda_client,
                )
                lambda_task = await loop.run_in_executor(executor=executor, func=func)
                lambda_tasks.append(lambda_task)

                # Limit concurrency to some extent
                if len(lambda_tasks) == n_tasks:
                    await asyncio.gather(*lambda_tasks)
                    lambda_tasks = []

            # collect any remaining tasks in the context of the thread pool
            await asyncio.gather(*lambda_tasks)


if __name__ == "__main__":

    # This __main__ code can be useful to test against AWS Lambda
    # because the moto pytest suite is not an exact replica.  This
    # requires a few minutes to setup a live AWS Lambda function
    # called 'lambda_dev' - see the test suite for a sample handler.

    from pprint import pprint

    print()
    print("Test async lambda functions")
    start = time.perf_counter()
    N_lambdas = int(os.environ.get("N_LAMBDAS", 1))

    lambda_funcs = []
    for i in range(N_lambdas):
        event = {"i": i}
        # event = {"action": "too-large"}
        # event = {"action": "runtime-error"}
        payload = json.dumps(event).encode()
        func = AWSLambdaFunction(name="lambda_dev", payload=payload)
        lambda_funcs.append(func)

    asyncio.run(run_lambda_functions(lambda_funcs))
    # # Note: a thread pool executor is not faster than asyncio alone
    # asyncio.run(run_lambda_function_thread_pool(lambda_funcs, n_tasks=N_lambdas))

    responses = []
    for func in lambda_funcs:
        assert response_success(func.response)
        if N_lambdas < 3:
            print()
            print("Params:")
            pprint(func.params)
            print("Response:")
            pprint(func.response)
            print("Data:")
            pprint(func.data)
            print("JSON:")
            pprint(func.json)
            print("Error:")
            pprint(func.error)
            print("Logs")
            pprint(func.logs)
        elif N_lambdas < 20:
            responses.append(func.json)

    if responses:
        # print(json.dumps(responses, indent=2))
        pprint(responses)

    end = time.perf_counter() - start
    print()
    print(f"{N_lambdas} lambdas finished in {end:0.2f} seconds.\n")
