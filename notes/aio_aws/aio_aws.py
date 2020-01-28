#! /usr/bin/env python3
# pylint: disable=bad-continuation

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
Async AWS Settings
------------------

.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
"""

import asyncio
import random

import aiobotocore.client  # type: ignore
import aiobotocore.config  # type: ignore
import aiobotocore.session  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.session  # type: ignore

from notes.aio_aws.logger import LOGGER

#: max_pool_connections for AWS clients (10 by default)
MAX_POOL_CONNECTIONS = botocore.endpoint.MAX_POOL_CONNECTIONS

#: AWS asyncio session config
#: ..seealso:: https://github.com/boto/botocore/blob/develop/botocore/config.py
AIO_AWS_CONFIG = aiobotocore.config.AioConfig(max_pool_connections=MAX_POOL_CONNECTIONS)

#: AWS asyncio session
AIO_AWS_SESSION = aiobotocore.get_session()
AIO_AWS_SESSION.user_agent_name = "aio-aws"
AIO_AWS_SESSION.set_default_client_config(AIO_AWS_CONFIG)

#: a semaphore to limit requests to the max client connections
CLIENT_SEMAPHORE = asyncio.Semaphore(MAX_POOL_CONNECTIONS)

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
    :param aio_aws_config: an aiobotocore.config.AioConfig (default ``.AIO_AWS_CONFIG``)
    :param loop: an asyncio.AbstractEventLoop (defaults to the asyncio default loop)
    :return: aiobotocore.session.AioSession
    """
    session = aiobotocore.get_session()
    session.user_agent_name = "aiobotocore"
    session.set_default_client_config(aio_aws_config)
    return session


async def delay(
    task_id: str, min_pause: float = MIN_PAUSE, max_pause: float = MAX_PAUSE,
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
    task_id: str = "jitter", min_jitter: float = MIN_JITTER, max_jitter: float = MAX_JITTER,
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


def response_code(response):
    return response.get("ResponseMetadata", {}).get("HTTPStatusCode")


def response_success(response):
    return response_code(response) == 200


if __name__ == "__main__":

    # pylint: disable=C0103
    loop = asyncio.get_event_loop()

    try:
        LOGGER.setLevel("DEBUG")

        delay_task = loop.create_task(delay("delay_task", 0.1, 0.5))
        jitter_task = loop.create_task(jitter("jitter_task", 0.1, 0.5))

        loop.run_until_complete(delay_task)
        print("Check delay task")
        assert delay_task.done()
        pause = delay_task.result()
        assert 0.1 <= pause <= 0.5

        loop.run_until_complete(jitter_task)
        print("Check jitter task")
        assert jitter_task.done()
        pause = jitter_task.result()
        assert 0.1 <= pause <= 0.5

    finally:
        loop.stop()
        loop.close()
