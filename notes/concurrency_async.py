#! /usr/bin/env python3

"""
Asyncio Code
------------
"""
import asyncio
import logging
import random
import time
from asyncio import AbstractEventLoop
from asyncio import Future
from asyncio import Task
from typing import List

import click

LOGGER = logging.getLogger("concurrency_asyncio")
LOGGER.setLevel(logging.INFO)

#: Minimum task pause
MIN_PAUSE: int = 1

#: Maximum task pause
MAX_PAUSE: int = 10


async def delay(task_id: int) -> float:
    """
    Await a random pause between :py:const:`MIN_PAUSE` and :py:const:`MAX_PAUSE`

    :param task_id: the ID for the asyncio.Task awaiting this pause
    :return: random interval for pause
    """
    pause = random.uniform(MIN_PAUSE, MAX_PAUSE)
    LOGGER.warning("Task %d - await a sleep for %.2f", task_id, pause)
    try:
        await asyncio.sleep(pause)
        LOGGER.warning("Task %d - done with sleep for %.2f", task_id, pause)
        return pause

    except asyncio.CancelledError:
        LOGGER.error("Task %d - cancelled", task_id)
        raise


async def create_futures(task_count: int) -> List[Future]:
    async_tasks = []
    for task_id in range(task_count):
        # each call to delay() returns a coroutine object
        coro = delay(task_id)
        # coroutine objects can be wrapped in an async future;
        # the task is not started yet.
        async_task = asyncio.ensure_future(coro)
        async_tasks.append(async_task)
    return async_tasks


async def create_tasks(task_count: int, async_loop: AbstractEventLoop) -> List[Task]:
    async_tasks = []
    for task_id in range(task_count):
        # each call to delay() returns a coroutine object
        coro = delay(task_id)
        # coroutine objects can be wrapped in an async task (future);
        # the event loop creates the task, but does not start it until
        # this creation coroutine is awaited (run by event loop).
        async_task = async_loop.create_task(coro)
        async_tasks.append(async_task)
    # Each task has accessor methods to retrieve the future result;
    # so it's sufficient to return the tasks here.
    return async_tasks


async def run_tasks(
    task_count: int, async_loop: AbstractEventLoop, collection_method: str = "gather"
) -> List[Task]:
    """
    Run a collection of asyncio tasks and return the collection
    of completed tasks.

    :param task_count: the number of asyncio tasks to run
    :param async_loop: an asyncio event loop
    :param collection_method: an option for asyncio execution
    :return: a list of asyncio task futures (they should be all done)

    .. seealso::
        - https://pymotw.com/3/asyncio/control.html
    """
    LOGGER.warning("Creating tasks")
    async_tasks = await create_tasks(task_count, async_loop)

    if collection_method == "gather":
        LOGGER.warning("Waiting on tasks to gather results")
        results = await asyncio.gather(*async_tasks)

    elif collection_method == "wait":
        LOGGER.warning("Waiting on tasks")
        # wait can also use a timeout parameter
        completed, pending = await asyncio.wait(async_tasks)
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
            completed, pending = await asyncio.wait(async_tasks)
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

    # get the event loop for the main thread
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    try:
        # Run a small set of simple asyncio tasks that pause up to 10 sec
        start = time.perf_counter()
        tasks = loop.run_until_complete(run_tasks(task_count, loop, collection_method))
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

    main()
