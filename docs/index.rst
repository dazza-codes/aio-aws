
aio-aws
=======

A python package for asynchronous access to AWS services.

- https://github.com/dazza-codes/aio-aws

This project has a very limited focus.  For general client solutions, see
`aioboto3`_ and `aiobotocore`_.

.. toctree::
    :maxdepth: 3
    :caption: Contents

    aio_aws
    contributing

Install
=======

.. code-block:: shell

    pip install -U aio-aws[all]
    pip check

To add a project dependency using poetry_:

.. code-block:: shell

    poetry add aio-aws --extras all

License
=======

    Copyright 2019-2022 Darren Weber

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

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
.. _poetry: https://python-poetry.org
