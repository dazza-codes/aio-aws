
aio-aws
=======

A python package for asynchronous functions and utilities for AWS services.

- https://github.com/dazza-codes/aio-aws

This project has a very limited focus.  For general client solutions, see
`aioboto3`_ and `aiobotocore`_, which wrap `botocore`_ to patch it with
features for async coroutines using `asyncio`_ and `aiohttp`_. This project
is not published as a pypi package because there is no promise
to support or develop it extensively, at this time.  For the curious, it
can be used directly from a github tag.  Note that any 0.x releases are
likely to have breaking API changes.

.. toctree::
    :maxdepth: 2
    :caption: Contents

    about
    aio_aws

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _aioboto3: https://github.com/terrycain/aioboto3
.. _aiobotocore: https://github.com/aio-libs/aiobotocore
.. _aiohttp: https://aiohttp.readthedocs.io/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _botocore: https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
.. _TinyDB: https://tinydb.readthedocs.io/en/latest/intro.html
