#! /usr/bin/env python3

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

The `aio_aws_lambda.py` module has a main script to call this simple lambda function.  This
lambda function easily runs within a 128 Mb memory allocation and it runs in less time than the
minimal billing period, so it's as cheap as it can be and could fit within a monthly free quota.

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
import json
import os
import time
from json import dumps
from typing import Any
from typing import Dict

import botocore.exceptions
from dataclasses import dataclass

from aio_aws import AioAWSConfig
from aio_aws import jitter
from aio_aws import LOGGER
from aio_aws import response_success


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
    content: Any = None
    error: Any = None
    logs: Any = None

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

    async def invoke(self, config: AioAWSConfig) -> Dict:
        """
        Asynchronous coroutine to invoke a lambda function; this
        updates the ``response`` and calls the py:meth:`.read_response`
        method to handle the response.

        :param config: aio session and client settings
        :return: a lambda response
        :raises: botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError
        """
        async with config.create_client("lambda") as lambda_client:
            tries = 0
            while tries < config.retries:
                tries += 1
                try:
                    LOGGER.debug("AWS Lambda params: %s", self.params)
                    response = await lambda_client.invoke(**self.params)
                    LOGGER.debug("AWS Lambda response: %s", response)

                    self.response = response

                    if response_success(response):
                        await self.read_response()  # updates self.content
                        if self.content:
                            LOGGER.info("AWS Lambda (%s) invoked OK", self.name)
                        elif self.error:
                            LOGGER.error(
                                "AWS Lambda (%s) error: ", self.name, self.error
                            )
                    else:
                        # TODO: are there some failures that could be recovered here?
                        LOGGER.error("AWS Lambda (%s) invoke failure.", self.name)
                    return response

                except botocore.exceptions.ClientError as err:
                    error = err.response.get("Error", {})
                    if error.get("Code") == "TooManyRequestsException":
                        if tries < config.retries:
                            # add an extra random sleep period to avoid API throttle
                            await jitter(
                                "lambda-invoke", config.min_jitter, config.max_jitter
                            )
                        continue  # allow it to retry, if possible
                    else:
                        raise
            else:
                raise RuntimeError("AWS Lambda invoke exceeded retries")

    async def read_response(self):
        """
        Asynchronous coroutine to read a lambda response; this
        updates the ``content`` or the ``error`` values.

        :raises: botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError
        """

        if self.response and response_success(self.response):

            log_result = self.response.get("LogResult")
            if log_result:
                self.logs = base64.b64decode(log_result)

            response_payload = self.response.get("Payload")
            if response_payload:

                async with response_payload as stream:
                    data = await stream.read()

                body = data.decode()

                if self.response.get("FunctionError"):
                    self.error = body
                else:
                    self.content = json.loads(body)


if __name__ == "__main__":

    print()
    print("Test async lambda functions")
    start = time.perf_counter()
    N_lambdas = int(os.environ.get("N_LAMBDAS", 1))

    # Create an event loop for the aio processing
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    try:

        aio_config = AioAWSConfig(
            aws_region="us-west-2",
            max_pool_connections=80,  # a large connection pool
            min_jitter=0.2,
            max_jitter=0.8,
        )

        lambda_funcs = []
        lambda_tasks = []
        for i in range(N_lambdas):
            event = {"i": i}
            payload = json.dumps(event).encode()
            func = AWSLambdaFunction(name="lambda_dev", payload=payload)
            lambda_funcs.append(func)
            lambda_task = loop.create_task(func.invoke(aio_config))
            lambda_tasks.append(lambda_task)

        loop.run_until_complete(asyncio.gather(*lambda_tasks))

        for func in lambda_funcs:
            assert response_success(func.response)
            # TODO: parse and gather all responses
            if N_lambdas < 5:
                print()
                print(func.params)
                print()
                print(func.response)
                print()
                print(func.content)
                print()
                print(func.logs)

    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.stop()
        loop.close()

    end = time.perf_counter() - start
    print(f"{N_lambdas} lambdas finished in {end:0.2f} seconds.\n")
