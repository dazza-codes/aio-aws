# aio-aws

[![Build Status](https://github.com/dazza-codes/aio-aws/actions/workflows/python-test.yml/badge.svg)](https://github.com/dazza-codes/aio-aws/actions/workflows/python-test.yml)
[![Documentation Status](https://readthedocs.org/projects/aio-aws/badge/?version=latest)](https://aio-aws.readthedocs.io/en/latest/?badge=latest)

[![PyPI version](https://img.shields.io/pypi/v/aio-aws.svg)](https://pypi.org/project/aio-aws)
[![Python versions](https://img.shields.io/pypi/pyversions/aio-aws.svg)](https://pypi.org/project/aio-aws)

Asynchronous functions and tools for AWS services.  There is a
limited focus on s3 and AWS Batch and Lambda.  Additional services could be
added, but this project is likely to retain a limited focus.
For general client solutions, see
[aioboto3](https://github.com/terrycain/aioboto3) and
[aiobotocore](https://github.com/aio-libs/aiobotocore), which wrap
[botocore](https://botocore.amazonaws.com/v1/documentation/api/latest/index.html)

The API documentation is at [readthedocs](https://aio-aws.readthedocs.io/)

# Install

This project has a very limited focus.  For general client solutions, see
[aioboto3](https://github.com/terrycain/aioboto3) and
[aiobotocore](https://github.com/aio-libs/aiobotocore), which wrap
[botocore](https://botocore.amazonaws.com/v1/documentation/api/latest/index.html)
to patch it with features for async coroutines using
[aiohttp](https://aiohttp.readthedocs.io/en/latest/) and
[asyncio](https://docs.python.org/3/library/asyncio.html).

This project is alpha-status with a 0.x.y API version that could break.
There is no promise to support or develop it extensively, at this time.

## pip

```shell
pip install -U aio-aws[all]
pip check  # pip might not guarantee consistent packages
```

## poetry

poetry will try to guarantee consistent packages or fail.

```shell
# with optional extras
poetry add aio-aws --extras all
```

```toml
# pyproject.toml snippet

[tool.poetry.dependencies]
python = "^3.7"

# with optional extras
aio-aws = {version = "^0.1.0", extras = ["all"]}

# or, to make it an optional extra
aio-aws = {version = "^0.1.0", extras = ["all"], optional = true}
[tool.poetry.extras]
aio-aws = ["aio-aws"]

```

# Contributing

To use the source code, it can be cloned directly. To
contribute to the project, first fork it and clone the forked repository.

The following setup assumes that
[miniconda3](https://docs.conda.io/en/latest/miniconda.html) and
[poetry](https://python-poetry.org/docs/) are installed already
(and `make` 4.x).

- https://docs.conda.io/en/latest/miniconda.html
    - recommended for creating virtual environments with required versions of python
    - see https://github.com/dazza-codes/conda_container/blob/master/conda_venv.sh
- https://python-poetry.org/docs/
    - recommended for managing a python project with pip dependencies for
      both the project itself and development dependencies

```shell
git clone https://github.com/dazza-codes/aio-aws
cd aio-aws
conda create -n aio-aws python=3.7
conda activate aio-aws
make init  # calls poetry install
make test
```

# License

```text
Copyright 2019-2021 Darren Weber

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

# Notices

Inspiration for this project comes from various open source projects that use
the Apache 2 license, including but not limited to:
- Apache Airflow: https://github.com/apache/airflow
- aiobotocore: https://github.com/aio-libs/aiobotocore
- botocore: https://github.com/boto/botocore
