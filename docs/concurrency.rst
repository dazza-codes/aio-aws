=======================
Concurrency in Python 3
=======================

Asyncio Examples
================

Developing with asyncio in python 3.6
    - https://docs.python.org/3.6/library/asyncio.html
    - https://docs.python.org/3.6/library/asyncio-dev.html
    - https://docs.python.org/3.6/library/asyncio-eventloop.html#tasks
    - https://docs.python.org/3.6/library/asyncio-task.html#asyncio.Task
    - https://docs.python.org/3.6/library/asyncio-task.html#asyncio.Future

Typing and testing asyncio code
    - https://mypy.readthedocs.io/en/latest/more_types.html#async-and-await
    - https://github.com/pytest-dev/pytest-asyncio
    - https://medium.com/ideas-at-igenius/testing-asyncio-python-code-with-pytest-a2f3628f82bc

Resources
    - https://asyncio.readthedocs.io/en/latest/index.html
    - https://realpython.com/async-io-python/
    - https://realpython.com/python-gil/
    - https://pymotw.com/3/concurrency.html
    - https://pymotw.com/3/asyncio/index.html
        - https://pymotw.com/3/asyncio/coroutines.html
        - https://pymotw.com/3/asyncio/executors.html
    - https://snarky.ca/how-the-heck-does-async-await-work-in-python-3-5/
    - https://www.roguelynn.com/words/asyncio-we-did-it-wrong/
    - David Beazley
        - http://www.dabeaz.com/generators/
        - http://www.dabeaz.com/coroutines/

A simple example:

.. code-block:: python

    import asyncio
    import random
    from asyncio import AbstractEventLoop
    from asyncio import Task
    from typing import List

    LOGGER = logging.getLogger("asyncio")

    MIN_PAUSE = 1
    MAX_PAUSE = 10

    async def delay(task_n: int) -> float:
        pause = random.uniform(MIN_PAUSE, MAX_PAUSE)
        LOGGER.warning("Task %d - await a sleep for %.2f", task_n, pause)
        await asyncio.sleep(pause)
        LOGGER.warning("Task %d - done with sleep for %.2f", task_n, pause)
        return pause

    async def main(
        task_count: int,
        async_loop: AbstractEventLoop
    ) -> List[Task]:
        async_tasks = []
        for n in range(task_count):
            # each call to delay() returns a coroutine object
            coro = delay(n)
            # coroutine objects can be wrapped in an async task (future)
            async_task = async_loop.create_task(coro)
            async_tasks.append(async_task)
        # Up until this point, nothing is running;
        # to execute all the coroutines concurrently:
        await asyncio.gather(*async_tasks)
        # Each task has accessor methods to retrieve the future result;
        # so it's sufficient to return the tasks here.
        return async_tasks

    # This will run the main coroutine in an asyncio event loop.
    # The python 3.6 approach has become `asyncio.run` in later versions.
    # First get the event loop for the main thread, with debug enabled.
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)

    try:
        # run 5 concurrent tasks within 10 sec
        TASK_COUNT = 5
        tasks = loop.run_until_complete(main(TASK_COUNT, loop))
    finally:
        loop.stop()
        loop.close()

Below is an example of running a small set of tasks:

.. code-block:: shell

    $ ./notes/concurrency_async.py
    Task 0 - await a sleep for 7.66
    Task 1 - await a sleep for 8.52
    Task 2 - await a sleep for 6.90
    Task 3 - await a sleep for 2.24
    Task 4 - await a sleep for 7.36
    Task 3 - done with sleep for 2.24
    Task 2 - done with sleep for 6.90
    Task 4 - done with sleep for 7.36
    Task 0 - done with sleep for 7.66
    Task 1 - done with sleep for 8.52
    5 async tasks finished in 8.53 seconds.
    5 async tasks validated in 0.00 seconds.

    # for more execution details, try:
    $ /usr/bin/time -v ./notes/concurrency_async.py

At some point, it is possible to overload the concurrency by
saturating the number of tasks that can run at random intervals
within a 10 sec period, e.g. the following runs take at least
10 sec to complete.

.. code-block:: shell

    $ ./notes/concurrency_async.py --task-count 1000
    ... snipped ...
    1000 async tasks finished in 10.18 seconds.
    1000 async tasks validated in 0.00 seconds.

    $ ./notes/concurrency_async.py --task-count 10000
    ... snipped ...
    10000 async tasks finished in 12.37 seconds.
    10000 async tasks validated in 0.00 seconds.



.. automodule:: notes.concurrency_async
   :members:
   :undoc-members:
   :show-inheritance:

.. literalinclude:: ../notes/concurrency_async.py
   :linenos:
   :language: python
