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
from asyncio import Task
from typing import List

LOGGER = logging.getLogger("asyncio")

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
    await asyncio.sleep(pause)
    LOGGER.warning("Task %d - done with sleep for %.2f", task_id, pause)
    return pause


async def main(task_count: int, async_loop: AbstractEventLoop) -> List[Task]:
    """
    Run a collection of asyncio tasks and return the collection.

    :param task_count: the number of asyncio tasks to run
    :param async_loop: an asyncio event loop
    :return: a list of asyncio task futures (they should be all done)
    """
    async_tasks = []
    for task_id in range(task_count):
        # each call to delay() returns a coroutine object
        coro = delay(task_id)
        # coroutine objects can be wrapped in an async task (future)
        async_task = async_loop.create_task(coro)
        async_tasks.append(async_task)
    # Up until this point, nothing is running;
    # to execute all the coroutines concurrently:
    await asyncio.gather(*async_tasks)
    # Each task has accessor methods to retrieve the future result;
    # so it's sufficient to return the tasks here.
    return async_tasks


if __name__ == "__main__":
    # pylint: disable=invalid-name

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--task-count", nargs="?", const=5, default=5, type=int)
    args = parser.parse_args()
    TASK_COUNT = args.task_count

    # get the event loop for the main thread
    LOOP = asyncio.get_event_loop()
    LOOP.set_debug(enabled=True)

    try:
        # Run a small set of simple asyncio tasks that pause up to 10 sec
        start = time.perf_counter()
        TASKS = LOOP.run_until_complete(main(TASK_COUNT, LOOP))  # blocking call
        end = time.perf_counter() - start
        print(f"{len(TASKS):d} async tasks finished in {end:0.2f} seconds.")

        # Explore the asyncio.Task API a little
        start = time.perf_counter()
        for task in TASKS:
            assert task.done()
            assert task.exception() is None
            task_pause = task.result()
            assert isinstance(task_pause, float)
        end = time.perf_counter() - start
        print(f"{len(TASKS):d} async tasks validated in {end:0.2f} seconds.")

    finally:
        LOOP.stop()
        LOOP.close()
