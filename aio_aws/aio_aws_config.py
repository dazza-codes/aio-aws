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
AioAWS Settings
---------------

Using asyncio for AWS services requires the `aiobotocore`_ library, which wraps a
release of `botocore`_ to patch it with features for async coroutines using
`asyncio`_ and `aiohttp`_.  To avoid issuing too many concurrent requests (DOS attack),
the async approach should use a client connection limiter, based on ``asyncio.Semaphore()``.
It's recommended to use a single session and a single client with a connection pool.
Although there are context manager patterns, it's also possible to manage closing the client
after everything is done.

.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://www.mathewmarcus.com/blog/asynchronous-aws-api-requests-with-asyncio.html

.. _aioboto3: https://github.com/terrycain/aioboto3
.. _aiobotocore: https://github.com/aio-libs/aiobotocore
.. _aiohttp: https://aiohttp.readthedocs.io/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _botocore: https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
.. _TinyDB: https://tinydb.readthedocs.io/en/latest/intro.html
"""


import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiobotocore.client
import aiobotocore.config
import aiobotocore.session
import botocore.endpoint

from aio_aws.logger import LOGGER

#: max_pool_connections for AWS clients (10 by default)
MAX_POOL_CONNECTIONS = botocore.endpoint.MAX_POOL_CONNECTIONS

#: a semaphore to limit requests to the max client connections
CLIENT_SEMAPHORE = asyncio.Semaphore(MAX_POOL_CONNECTIONS)

#: AWS asyncio session config;
#: see https://github.com/boto/botocore/blob/develop/botocore/config.py
#: a default client config with py:const:`MAX_POOL_CONNECTIONS`
AIO_AWS_CONFIG = aiobotocore.config.AioConfig(max_pool_connections=MAX_POOL_CONNECTIONS)

#: a default session with py:const:`AIO_AWS_CONFIG`
AIO_AWS_SESSION = aiobotocore.get_session()
AIO_AWS_SESSION.user_agent_name = "aio-aws"
AIO_AWS_SESSION.set_default_client_config(AIO_AWS_CONFIG)
# AIO_AWS_SESSION.full_config
# AIO_AWS_SESSION.get_config_variable('region')
# AIO_AWS_SESSION.get_scoped_config() is the same as:
#     configs = AIO_AWS_SESSION.full_config
#     configs['profiles'][AIO_AWS_SESSION.profile]

#: batch job startup pause (seconds)
BATCH_STARTUP_PAUSE: float = 30

#: Minimum task pause
MIN_PAUSE: float = 5

#: Maximum task pause
MAX_PAUSE: float = 30

#: Minimum API request jitter
MIN_JITTER: float = 1

#: Maximum API request jitter
MAX_JITTER: float = 10


def aio_aws_session(
    aio_aws_config: aiobotocore.config.AioConfig = AIO_AWS_CONFIG,
) -> aiobotocore.session.AioSession:
    """
    Get an asyncio AWS session

    :param aio_aws_config: an aiobotocore.config.AioConfig
            (default :py:const:`AIO_AWS_CONFIG`)
    :return: aiobotocore.session.AioSession
    """
    session = aiobotocore.get_session()
    session.user_agent_name = "aio-aws"
    # session.set_stream_logger("aio-aws")  # for debugging
    session.set_default_client_config(aio_aws_config)
    return session


async def aio_aws_client(
    service_name: str,
    aio_aws_config: aiobotocore.config.AioConfig = AIO_AWS_CONFIG,
    **kwargs
):
    """
    Yield an asyncio AWS client with an option to provide a client-specific config; this is a
    thin wrapper on ``AIO_AWS_SESSION.create_client()`` and the additional
    kwargs as passed through to ``AIO_AWS_SESSION.create_client(**kwargs)``.

    :param service_name: an AWS service for a client, like "s3", try
            :py:meth:`AIO_AWS_SESSION.get_available_services()`
    :param aio_aws_config: an aiobotocore.config.AioConfig
            (default :py:const:`AIO_AWS_CONFIG`)
    :yield: aiobotocore.client.AioBaseClient
    """
    async with AIO_AWS_SESSION.create_client(
        service_name, config=aio_aws_config, **kwargs
    ) as client:
        yield client


# TODO: consider ways to auto-wrap client services?
#       - explore function decorators for:
#         - using a client semaphore
#         - to handle client exceptions for too-many-requests
# AIO_AWS_SESSION.get_available_services()  # iterate on these
# model = AIO_AWS_SESSION.get_service_model('batch')
# model.operation_names  # like 'DescribeJobs'
# op = model.operation_model('DescribeJobs')
# >>> op.input_shape
# <StructureShape(DescribeJobsRequest)>
# >>> op.output_shape
# <StructureShape(DescribeJobsResponse)>
# >>> op.input_shape.members
# OrderedDict([('jobs', <ListShape(StringList)>)])
# >>> op.input_shape.required_members
# ['jobs']


@dataclass
class AioAWSConfig:
    #: an optional AWS region name
    aws_region: str = None
    #: a number of retries for an AWS client request/response
    retries: int = 5
    #: an asyncio.sleep for ``random.uniform(min_pause, max_pause)``
    min_pause: float = MIN_PAUSE
    #: an asyncio.sleep for ``random.uniform(min_pause, max_pause)``
    max_pause: float = MAX_PAUSE
    #: an asyncio.sleep for ``random.uniform(min_jitter, max_jitter)``
    min_jitter: float = MIN_JITTER
    #: an asyncio.sleep for ``random.uniform(min_jitter, max_jitter)``
    max_jitter: float = MAX_JITTER
    #: defines a limit to the number of client connections
    max_pool_connections: int = MAX_POOL_CONNECTIONS
    #: an asyncio.Semaphore to limit the number of concurrent clients;
    #: this should be equivalent to the session client connection pool
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE
    #: an aiobotocore.session.AioSession
    session: aiobotocore.session.AioSession = AIO_AWS_SESSION

    def __post_init__(self):
        client_config = self.session.get_default_client_config()
        if client_config.region_name != self.aws_region:
            client_config.region_name = self.aws_region

        client_config.max_pool_connections = self.max_pool_connections
        self.sem = asyncio.Semaphore(self.max_pool_connections)
        self.session.set_default_client_config(client_config)

    @asynccontextmanager
    async def create_client(self, service: str) -> aiobotocore.client.AioBaseClient:
        """
        Create and yield a new client using the ``AioAWSConfig.session``;
        the number of clients (each using one connection) is limited by
        the ``AioAWSConfig.max_connection_pool``

        .. code-block::

            config = AioAWSConfig()
            async with config.create_client("s3") as client:
                response = await s3_client.head_bucket(Bucket=bucket_name)

        :yield: an aiobotocore.client.AioBaseClient for AWS service
        """
        async with self.sem:
            async with self.session.create_client(service) as client:
                yield client


async def delay(
    task_id: str, min_pause: float = MIN_PAUSE, max_pause: float = MAX_PAUSE
) -> float:
    """
    Await a random pause between :py:const:`MIN_PAUSE` and :py:const:`MAX_PAUSE`

    :param task_id: the ID for the task awaiting this pause
    :param min_pause: defaults to :py:const:`MIN_PAUSE`
    :param max_pause: defaults to :py:const:`MAX_PAUSE`
    :return: random interval for pause
    """
    rand_pause = random.uniform(min_pause, max_pause)
    LOGGER.debug("Task %s - await a sleep for %.2f", task_id, rand_pause)
    try:
        await asyncio.sleep(rand_pause)
        LOGGER.debug("Task %s - done with sleep for %.2f", task_id, rand_pause)
        return rand_pause

    except asyncio.CancelledError:
        LOGGER.error("Task %s - cancelled", task_id)
        raise


async def jitter(
    task_id: str = "jitter",
    min_jitter: float = MIN_JITTER,
    max_jitter: float = MAX_JITTER,
) -> float:
    """
    Await a random pause between `min_jitter` and `max_jitter`

    :param task_id: an optional ID for the task awaiting this jitter
    :param min_jitter: defaults to :py:const:`MIN_JITTER`
    :param max_jitter: defaults to :py:const:`MAX_JITTER`
    :return: random interval for pause
    """
    jit = await delay(task_id, min_jitter, max_jitter)
    return jit


def response_code(response) -> int:
    return int(response.get("ResponseMetadata", {}).get("HTTPStatusCode"))


def response_success(response) -> bool:
    code = response_code(response)
    if code:
        return 200 <= code < 300
    else:
        return False
