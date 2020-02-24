#! /usr/bin/env python3
# pylint: disable=bad-continuation
"""
Async Pauses
------------

Convenience functions for asyncio.sleep with
random.uniform pauses.
"""

import asyncio
import random

from notes.logger import LOGGER

#: Minimum task pause, in seconds
MIN_PAUSE: float = 5

#: Maximum task pause, in seconds
MAX_PAUSE: float = 10

#: Minimum jitter, in seconds
MIN_JITTER: float = 1

#: Maximum jitter, in seconds
MAX_JITTER: float = 5


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
