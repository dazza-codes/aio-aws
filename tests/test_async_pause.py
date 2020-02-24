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
