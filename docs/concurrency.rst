=======================
Concurrency in Python 3
=======================


General resources on concurrency
    - https://realpython.com/python-gil/
    - https://pymotw.com/3/concurrency.html
    - David Beazley
        - http://www.dabeaz.com/talks.html


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

Asyncio ecosystem libraries
    - databases
        - https://www.encode.io/databases/
            - https://pypi.org/project/databases/
            - https://github.com/magicstack/asyncpg
            - https://magic.io/blog/asyncpg-1m-rows-from-postgres-to-python/

Resources
    - https://asyncio.readthedocs.io/en/latest/index.html
    - https://realpython.com/async-io-python/
    - https://pymotw.com/3/asyncio/index.html
    - https://snarky.ca/how-the-heck-does-async-await-work-in-python-3-5/
    - https://www.roguelynn.com/words/asyncio-we-did-it-wrong/
    - David Beazley
        - http://www.dabeaz.com/generators/
        - http://www.dabeaz.com/coroutines/

Code for some simple examples is listed below.  You can run the examples
if you clone this repo and setup the virtualenv for it.  It's similar
to examples in https://pymotw.com/3/asyncio/index.html (although it
was not derived directly from those examples).

This example runs a small set of tasks:

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



.. literalinclude:: ../notes/concurrency_async.py
   :linenos:
   :language: python
