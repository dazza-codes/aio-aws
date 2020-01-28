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
Asyncio Code
------------
"""
import asyncio
import logging
import time
from asyncio import AbstractEventLoop
from asyncio import Future
from asyncio import Task
from typing import List

import click

from notes import async_pause
from notes.async_pause import delay

LOGGER = logging.getLogger("concurrency_asyncio")
LOGGER.setLevel(logging.INFO)


def delay_future(task_id: str) -> Future:
    """Create asyncio future"""
    # each call to delay() returns a coroutine object
    coro = delay(task_id)
    # coroutine objects can be wrapped in an async future;
    # the event loop creates the task, but does not start it until
    # this creation coroutine is awaited (run by event loop).
    return asyncio.ensure_future(coro)


def delay_task(task_id: str, async_loop: AbstractEventLoop = None) -> Task:
    """Create asyncio task"""
    if async_loop is None:
        async_loop = asyncio.get_event_loop()
    # each call to delay() returns a coroutine object
    coro = delay(task_id)
    # coroutine objects can be wrapped in an async task (future);
    # the event loop creates the task, but does not start it until
    # this creation coroutine is awaited (run by event loop).
    return async_loop.create_task(coro)


async def create_futures(task_count: int) -> List[Future]:
    """Create asyncio futures"""
    return [delay_future(str(task_id)) for task_id in range(task_count)]


def create_tasks(task_count: int, async_loop: AbstractEventLoop = None) -> List[Task]:
    """Create asyncio tasks"""
    if async_loop is None:
        async_loop = asyncio.get_event_loop()
    # Each task has accessor methods to retrieve the future result;
    # so it's sufficient to return the tasks here.
    return [delay_task(str(task_id), async_loop) for task_id in range(task_count)]


async def run_tasks(async_tasks: List[Task], collection_method: str = "gather") -> List[Task]:
    """
    Run a collection of asyncio tasks and return the collection
    of completed tasks.

    :param async_tasks: a list of asyncio tasks
    :param collection_method: an option for asyncio execution
    :return: a list of asyncio task futures (they should be all done)

    .. seealso::
        - https://pymotw.com/3/asyncio/control.html
    """
    if collection_method == "gather":
        LOGGER.warning("Waiting on tasks to gather results")
        _results = await asyncio.gather(*async_tasks)

    elif collection_method == "wait":
        LOGGER.warning("Waiting on tasks")
        # wait can also use a timeout parameter
        _completed, _pending = await asyncio.wait(async_tasks)
        # pending tasks after a timeout can be cancelled

    elif collection_method == "as-complete":
        LOGGER.warning("Waiting on tasks as they complete")
        for task_done in asyncio.as_completed(async_tasks):
            task_result = await task_done
            LOGGER.warning(" - next task complete %s with result %.2f", task_done, task_result)

    elif collection_method == "cancelled":
        LOGGER.warning("Cancelling tasks")
        for task in async_tasks:
            task.cancel()

        LOGGER.warning("Waiting on cancelled tasks")
        try:
            _completed, _pending = await asyncio.wait(async_tasks)
        except asyncio.CancelledError:
            LOGGER.error("Task cancelled")

    else:
        LOGGER.error("Unknown collection method")

    # Each task has accessor methods to retrieve the future result;
    # so it's sufficient to return the tasks here.
    LOGGER.warning("Returning tasks")
    return async_tasks


COLLECTION_METHODS = ("gather", "wait", "as-complete", "cancelled")


@click.command()
@click.option(
    "--task-count", default=5, show_default=True, type=int, help="A number of tasks to run",
)
@click.option(
    "--collection-method",
    default=COLLECTION_METHODS[0],
    show_default=True,
    type=click.Choice(COLLECTION_METHODS),
    help="type of asyncio task collection",
)
def main(task_count, collection_method):
    """Run simple asyncio example"""

    # get the event loop for the main thread
    loop = asyncio.new_event_loop()
    loop.set_debug(enabled=True)
    async_pause.LOGGER.setLevel("DEBUG")

    try:
        # Run a small set of simple asyncio tasks that pause up to 10 sec
        start = time.perf_counter()
        LOGGER.warning("Creating tasks")
        tasks = create_tasks(task_count, loop)

        loop.run_until_complete(run_tasks(tasks, collection_method))
        end = time.perf_counter() - start
        print(f"{len(tasks):d} async tasks finished in {end:0.2f} seconds.")

        # Explore the asyncio.Task API a little
        start = time.perf_counter()
        for task in tasks:
            assert task.done()
            if collection_method == "cancelled":
                # https://docs.python.org/3.6/library/asyncio-task.html#asyncio.Future.exception
                # The exception (or None if no exception was set) is returned only if the
                # future is done. If the future has been cancelled, raises CancelledError.
                # If the future isn’t done yet, raises InvalidStateError.
                try:
                    task.exception()
                except asyncio.CancelledError as err:
                    assert isinstance(err, asyncio.CancelledError)
            else:
                assert task.exception() is None
                task_pause = task.result()
                assert isinstance(task_pause, float)
        end = time.perf_counter() - start
        print(f"{len(tasks):d} async tasks validated in {end:0.2f} seconds.")

    finally:
        loop.stop()
        loop.close()


if __name__ == "__main__":

    main()  # pylint: disable=no-value-for-parameter
