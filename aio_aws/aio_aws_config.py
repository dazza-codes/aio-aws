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
import botocore.client
import botocore.endpoint

from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)

#: max_pool_connections for AWS clients (10 by default)
MAX_POOL_CONNECTIONS = botocore.endpoint.MAX_POOL_CONNECTIONS

#: Minimum task pause
MIN_PAUSE: float = 5

#: Maximum task pause
MAX_PAUSE: float = 30

#: Minimum API request jitter
MIN_JITTER: float = 1

#: Maximum API request jitter
MAX_JITTER: float = 10

#: Common exception codes that are retried
RETRY_EXCEPTIONS = ["TooManyRequestsException", "ThrottlingException"]


def asyncio_default_semaphore() -> asyncio.Semaphore:
    """
    a default semaphore to limit creation of clients;
    it defaults to 2 * py:const:`MAX_POOL_CONNECTIONS`

    .. seealso::
        https://github.com/boto/botocore/blob/develop/botocore/config.py

    :return: aiobotocore.config.AioConfig
    """
    return asyncio.Semaphore(MAX_POOL_CONNECTIONS * 2)


def aio_aws_default_config() -> aiobotocore.config.AioConfig:
    """
    Get a default asyncio AWS config using a default
    py:const:`MAX_POOL_CONNECTIONS`

    .. seealso::
        https://github.com/boto/botocore/blob/develop/botocore/config.py

    :return: aiobotocore.config.AioConfig
    """
    return aiobotocore.config.AioConfig(max_pool_connections=MAX_POOL_CONNECTIONS)


def aio_aws_default_session() -> aiobotocore.session.AioSession:
    """
    Get a default asyncio AWS session with a default config from
    :py:func:`aio_aws_default_config`

    :return: aiobotocore.session.AioSession
    """
    aio_config = aio_aws_default_config()
    aio_session = aiobotocore.get_session()
    aio_session.user_agent_name = "aio-aws"
    aio_session.set_default_client_config(aio_config)
    # aio_session.full_config
    # aio_session.get_config_variable('region')
    # aio_session.get_scoped_config() is the same as:
    #     configs = aio_session.full_config
    #     configs['profiles'][aio_session.profile]
    return aio_session


def aio_aws_session(
    aio_aws_config: aiobotocore.config.AioConfig = None,
) -> aiobotocore.session.AioSession:
    """
    Get an asyncio AWS session with an 'aio-aws' user agent name

    :param aio_aws_config: an aiobotocore.config.AioConfig
            (default :py:func:`aio_aws_default_config`)
    :return: aiobotocore.session.AioSession
    """
    if aio_aws_config is None:
        aio_aws_config = aio_aws_default_config()
    session = aiobotocore.get_session()
    session.user_agent_name = "aio-aws"
    # session.set_stream_logger("aio-aws")  # for debugging
    session.set_default_client_config(aio_aws_config)
    return session


@asynccontextmanager
async def aio_aws_client(
    service_name: str, *args, **kwargs
) -> aiobotocore.client.AioBaseClient:
    """
    Yield an asyncio AWS client with an option to provide a client-specific config; this is a
    thin wrapper on ``aiobotocore.get_session().create_client()`` and the additional
    kwargs as passed through to ``session.create_client(**kwargs)``.

    It is possible to pass through additional args and kwargs
    including `config: aiobotocore.config.AioConfig`
    (the default is :py:func:`aio_aws_default_config`)

    .. code-block::

        s3_endpoint = "http://localhost:5555"
        client_config = botocore.client.Config(
            read_timeout=120,
            max_pool_connections=50,
        )
        async with aio_aws_client(
            "s3", endpoint_url=s3_endpoint, config=client_config
        ) as client:
            assert "aiobotocore.client.S3" in str(client)
            assert isinstance(client, aiobotocore.client.AioBaseClient)

    :param service_name: an AWS service for a client, like "s3", try
            :py:meth:`session.get_available_services()`
    :yield: aiobotocore.client.AioBaseClient

    .. seealso::
        - https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """
    session = aio_aws_session()
    async with session.create_client(service_name, *args, **kwargs) as client:
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
    retries: int = 4
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
    # An asyncio.Semaphore to limit the number of concurrent clients;
    # this defaults to the session client connection pool
    sem: int = MAX_POOL_CONNECTIONS
    #: an aiobotocore.session.AioSession
    session: aiobotocore.session.AioSession = None

    @classmethod
    def get_default_config(cls):
        # TODO: explore memoized default config
        return cls()

    def __post_init__(self):

        # see also aiobotocore.endpoint.AioEndpointCreator.create_endpoint
        # for all the options that config details can provide for aiohttp session

        if self.session is None:
            self.session = aio_aws_default_session()

        default_config = self.default_client_config

        # self.session.get_default_client_config() is Optional[botocore.client.Config]
        client_config = self.session.get_default_client_config()
        if client_config:
            client_config = client_config.merge(default_config)
        else:
            client_config = default_config

        self.session.set_default_client_config(client_config)

        # Lazy init for an asyncio instance
        self._semaphore = None

    @property
    def default_client_config(self) -> botocore.client.Config:
        # botocore/config.py lists all the options
        config = botocore.client.Config(
            connect_timeout=20,
            read_timeout=120,
            max_pool_connections=self.max_pool_connections,
        )
        if self.aws_region:
            config.region_name = self.aws_region
        return config

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """
        An asyncio.Semaphore to limit the number of concurrent clients;
        this defaults to the session client connection pool
        """
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.sem)
        return self._semaphore

    @asynccontextmanager
    async def create_client(
        self, service: str, *args, **kwargs
    ) -> aiobotocore.client.AioBaseClient:
        """
        Create and yield a new client using the ``AioAWSConfig.session``;
        the clients are configured with a default limit on the size of
        the connection pool defined by ``AioAWSConfig.max_pool_connections``

        .. code-block::

            config = AioAWSConfig()
            async with config.create_client("s3") as client:
                response = await s3_client.head_bucket(Bucket=bucket_name)

        It is possible to pass through additional args and kwargs
        including `config=botocore.client.Config`.

        :yield: an aiobotocore.client.AioBaseClient for AWS service

        .. seealso::
            - https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
        """
        async with self.session.create_client(service, *args, **kwargs) as client:
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
