"""
Test the notes.concurrency_async module
"""
from types import ModuleType

import pytest

from notes import concurrency_async


# @pytest.fixture
# def event_loop():
#     loop = asyncio.get_event_loop()
#     try:
#         yield loop
#     finally:
#         loop.stop()
#         loop.close()


def test_concurrency_module():
    assert isinstance(concurrency_async, ModuleType)


# def test_async_delay(event_loop, mocker):
#     # min_mock = mocker.mock(concurrency_async.MIN_PAUSE)
#     # max_mock = mocker.mock(concurrency_async.MAX_PAUSE)
#     coro = concurrency_async.delay(0)
#     # assert isinstance(coro, coroutine)
#     task = event_loop.create_task(coro)
#     event_loop.run_until_complete(task)
#     assert task.done()
#     assert task.exception() is None
#     pause = task.result()
#     assert concurrency_async.MIN_PAUSE <= pause <= concurrency_async.MAX_PAUSE


@pytest.mark.asyncio
async def test_async_delay(monkeypatch):
    monkeypatch.setattr(concurrency_async, "MIN_PAUSE", 0.0)
    monkeypatch.setattr(concurrency_async, "MAX_PAUSE", 0.5)
    pause = await concurrency_async.delay(0)
    assert isinstance(pause, float)
    assert concurrency_async.MIN_PAUSE == 0.0
    assert concurrency_async.MAX_PAUSE == 0.5
    assert concurrency_async.MIN_PAUSE <= pause <= concurrency_async.MAX_PAUSE
