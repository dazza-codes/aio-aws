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
Test the aio_aws.aio_aws_config module
"""
from types import ModuleType

import pytest

import aio_aws.aio_aws_config


def test_aio_aws_package():
    assert isinstance(aio_aws.aio_aws_config, ModuleType)


@pytest.mark.asyncio
async def test_async_delay():
    min_delay = 0.0
    max_delay = 0.5
    pause = await aio_aws.aio_aws_config.delay("delay_task", min_delay, max_delay)
    assert isinstance(pause, float)
    assert min_delay <= pause <= max_delay


@pytest.mark.asyncio
async def test_async_jitter():
    min_jitter = 0.0
    max_jitter = 0.5
    pause = await aio_aws.aio_aws_config.jitter("jitter_task", min_jitter, max_jitter)
    assert isinstance(pause, float)
    assert min_jitter <= pause <= max_jitter


@pytest.mark.skip("Skip slower default delay")
@pytest.mark.asyncio
async def test_async_delay_defaults():
    pause = await aio_aws.aio_aws_config.delay("delay_task")
    assert isinstance(pause, float)
    assert aio_aws.aio_aws_config.MIN_PAUSE <= pause <= aio_aws.aio_aws_config.MAX_PAUSE


@pytest.mark.skip("Skip slower default jitter")
@pytest.mark.asyncio
async def test_async_jitter_defaults():
    pause = await aio_aws.aio_aws_config.jitter("jitter_task")
    assert isinstance(pause, float)
    assert aio_aws.aio_aws_config.MIN_JITTER <= pause <= aio_aws.aio_aws_config.MAX_JITTER
