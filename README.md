[![pipeline status](https://gitlab.com/dazza-codes/aio-aws/badges/master/pipeline.svg)](https://gitlab.com/dazza-codes/aio-aws/-/commits/master)
[![coverage report](https://gitlab.com/dazza-codes/aio-aws/badges/master/coverage.svg)](https://gitlab.com/dazza-codes/aio-aws/-/commits/master)

# aio-aws

Asynchronous functions and tools for AWS services.  There is a
limited focus on s3 and AWS Batch.  Additional services could be
added, but this project is likely to retain a limited focus.
For general client solutions, see:
- aioboto3: https://github.com/terrycain/aioboto3
- aiobotocore: https://github.com/aio-libs/aiobotocore

The API documentation is published as gitlab pages at:
- http://dazza-codes.gitlab.io/aio-aws

# Getting Started

To use the source code, it can be cloned directly. To
contribute to the project, first fork it and clone the forked repository.

The following setup assumes that
[miniconda3](https://docs.conda.io/en/latest/miniconda.html) and
[poetry](https://python-poetry.org/docs/) are installed already (and `make`
4.x).

- https://docs.conda.io/en/latest/miniconda.html
    - recommended for creating virtual environments with required versions of python
- https://python-poetry.org/docs/
    - recommended for managing a python project with pip dependencies for
      both the project itself and development dependencies

```shell
git clone https://gitlab.com/dazza-codes/aio-aws
cd aio-aws
conda create -n aio-aws python=3.6
conda activate aio-aws
make init  # calls poetry install
```

# License

```text
Copyright 2019-2020 Darren Weber

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

This project is inspired by and uses various open source projects that use
the Apache 2 license, including but not limited to:
- Apache Airflow: https://github.com/apache/airflow
- aiobotocore: https://github.com/aio-libs/aiobotocore
- botocore: https://github.com/boto/botocore
