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
Aio AWS Lambda
--------------

"""


import asyncio
from json import dumps
from typing import Dict

import botocore.exceptions
from dataclasses import dataclass

from aio_aws import AioAWSConfig
from aio_aws import jitter
from aio_aws import LOGGER
from aio_aws import response_success


@dataclass
class LambdaFunc:
    """

    For an example of a ClientContext JSON, see PutEvents in the Amazon Mobile
    Analytics API Reference and User Guide. The ClientContext JSON must be
    base64-encoded and has a maximum size of 3583 bytes.

    Payload (bytes or seekable file-like object) -- JSON that you want to
    provide to your Lambda function as input.

    Qualifier (string) --
    You can use this optional parameter to specify a Lambda function version
    or alias name. If you specify a function version, the API uses the qualified
    function ARN to invoke a specific Lambda function. If you specify an alias
    name, the API uses the alias ARN to invoke the Lambda function version to
    which the alias points. If you don't provide this parameter, then the API
    uses unqualified function ARN which results in invocation of the $LATEST version.

    """
    name: str
    type: str = "RequestResponse"  # or 'Event' or 'DryRun'
    log_type: str = "None"  # 'None' or 'Tail'
    context: str = None
    payload: bytes = b""  # or a file
    qualifier: str = None
    response: Dict = None

    def __post_init__(self):

        self.name = self.name[:64]

        if self.type != "RequestResponse":
            self.log_type = "None"

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


async def aio_lambda_invoke(
    func: LambdaFunc, config: AioAWSConfig,
) -> Dict:
    """
    Asynchronous coroutine to invoke a lambda function

    :param func: A lambda function
    :param config: aio session and client settings
    :return: a lambda response
    :raises: botocore.exceptions.ClientError, botocore.exceptions.ParamValidationError
    """
    async with config.create_client("lambda") as lambda_client:
        tries = 0
        while tries < config.retries:
            tries += 1
            try:
                LOGGER.debug("AWS Lambda params: %s", func.params)
                response = await lambda_client.invoke(**func.params)
                LOGGER.debug("AWS Lambda response: %s", response)

                func.response = response
                if response_success(response):
                    LOGGER.info("AWS Lambda (%s) invoked OK", func.name)
                else:
                    # TODO: are there some failures that could be recovered here?
                    LOGGER.error("AWS Lambda (%s) invoke failure.", func.name)
                return response

            except botocore.exceptions.ClientError as err:
                error = err.response.get("Error", {})
                if error.get("Code") == "TooManyRequestsException":
                    if tries < config.retries:
                        # add an extra random sleep period to avoid API throttle
                        await jitter("lambda-invoke", config.min_jitter, config.max_jitter)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS Lambda invoke exceeded retries")


if __name__ == "__main__":

    print()
    print("Test async lambda functions")

    # Create an event loop for the aio processing
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    try:

        aio_config = AioAWSConfig(
            aws_region="us-west-2",
            max_pool_connections=10,
            min_jitter=0.2,
            max_jitter=0.8,
        )

        func = LambdaFunc("lambda_dev")
        loop.run_until_complete(aio_lambda_invoke(func, config=aio_config))

        print(func.response)

    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.stop()
        loop.close()
