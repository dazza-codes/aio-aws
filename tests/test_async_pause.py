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
Test Asyncio Pause

"""

import inspect

import pytest

from notes import async_pause


def test_async_pause():
    assert inspect.ismodule(async_pause)


@pytest.mark.asyncio
async def test_async_delay():
    min_delay = 0.0
    max_delay = 0.5
    pause = await async_pause.delay("delay_task", min_delay, max_delay)
    assert isinstance(pause, float)
    assert min_delay <= pause <= max_delay


@pytest.mark.asyncio
async def test_async_jitter():
    min_jitter = 0.0
    max_jitter = 0.5
    pause = await async_pause.jitter("jitter_task", min_jitter, max_jitter)
    assert isinstance(pause, float)
    assert min_jitter <= pause <= max_jitter


@pytest.mark.skip("Skip slower default delay")
@pytest.mark.asyncio
async def test_async_delay_defaults():
    pause = await async_pause.delay("delay_task")
    assert isinstance(pause, float)
    assert async_pause.MIN_PAUSE <= pause <= async_pause.MAX_PAUSE


@pytest.mark.skip("Skip slower default jitter")
@pytest.mark.asyncio
async def test_async_jitter_defaults():
    pause = await async_pause.jitter("jitter_task")
    assert isinstance(pause, float)
    assert async_pause.MIN_JITTER <= pause <= async_pause.MAX_JITTER
