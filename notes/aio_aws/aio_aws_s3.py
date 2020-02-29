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
Asyncio AWS S3
--------------

Example code for async AWS S3 services. The `aiobotocore`_ library wraps a
release of `botocore`_ to patch it with features for async coroutines using
`asyncio`_ and `aiohttp`_. The simple example code iterates on a list of
buckets to issue a HEAD request, as a crude way to determine whether bucket
access is permitted. The example compares an async approach (using
aiobotocore) vs a sequential blocking approach (using botocore). The async
approach is arbitarily limited to 50 client connections using
``asycio.Semaphore(50)``.

.. code-block:: shell

    $ ./notes/async_aws.py

    async-aws queried 65 buckets
    async-aws found 62 accessible buckets
    async-aws finished in 3.65 seconds.

    sync-aws queried 65 buckets
    sync-aws found 62 accessible buckets
    sync-aws finished in  50.40 seconds.

.. seealso::
    - https://aiobotocore.readthedocs.io/en/latest/
    - https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
    - https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
    - https://docs.python.org/3/library/asyncio.html
    - https://www.mathewmarcus.com/blog/asynchronous-aws-api-requests-with-asyncio.html

.. _aiobotocore: https://aiobotocore.readthedocs.io/en/latest/
.. _aiohttp: https://aiohttp.readthedocs.io/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _botocore: https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
"""

import asyncio
import time
from typing import Dict
from typing import Tuple

import aiobotocore.config  # type: ignore
import boto3
import botocore.client  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore
from dataclasses import dataclass

from notes.aio_aws.aio_aws import AIO_AWS_SESSION
from notes.aio_aws.aio_aws import CLIENT_SEMAPHORE
from notes.aio_aws.logger import LOGGER


@dataclass(frozen=True)
class S3Parts:
    protocol: str
    bucket: str
    key: str

    @staticmethod
    def parse_s3_uri(uri: str) -> "S3Parts":
        uri = str(uri).strip()
        protocol = ""
        bucket = ""
        key = ""
        if uri.startswith("s3://"):
            protocol = "s3://"
            uri_path = uri.replace("s3://", "")
            uri_paths = uri_path.split("/")
            bucket = uri_paths.pop(0)
            key = "/".join(uri_paths)

        return S3Parts(protocol, bucket, key)


def aws_s3_client(session: botocore.session.Session = None,) -> botocore.client.BaseClient:
    """
    Sequential, synchronous approach to list all s3 buckets
    and issue a HEAD request to check if access is allowed.

    :return: dict of {bucket_name: str, access: bool}
    """
    if session is None:
        # use a default session
        session = botocore.session.get_session()
    return session.create_client("s3")


def run_aws_s3_seq_buckets(s3_client: botocore.client.BaseClient = None,) -> Dict[str, bool]:
    """
    Sequential, synchronous approach to list all s3 buckets
    and issue a HEAD request to check if access is allowed.

    :return: dict of {bucket_name: str, access: bool}
    """
    if s3_client is None:
        s3_client = aws_s3_client()

    access_buckets = {}
    response = s3_client.list_buckets()
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            try:
                head_response = s3_client.head_bucket(Bucket=bucket_name)
                if head_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    access_buckets[bucket_name] = True
                else:
                    access_buckets[bucket_name] = False
            except botocore.exceptions.ClientError as err:
                err_code = int(err.response["Error"]["Code"])
                if err_code == 403:
                    access_buckets[bucket_name] = False
                    continue
                raise

    return access_buckets


async def async_s3_object_head(
    s3_uri: str,
    session: aiobotocore.AioSession = AIO_AWS_SESSION,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 object.

    :param s3_uri: an s3 URI
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        async with session.create_client("s3") as s3_client:
            s3_parts = S3Parts.parse_s3_uri(s3_uri)
            return await s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)


async def async_s3_object_access(
    s3_uri: str,
    session: aiobotocore.AioSession = AIO_AWS_SESSION,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 object access is allowed.

    :param s3_uri: an s3 URI
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (s3_uri: str, access: bool)
    """
    try:

        head_response = await async_s3_object_head(s3_uri, session, sem)

        if head_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return s3_uri, True

        return s3_uri, False

    except botocore.exceptions.ClientError as err:
        err_code = int(err.response["Error"]["Code"])
        if err_code == 401:
            LOGGER.warning(f"GET HEAD {s3_uri} -> 401: invalid credentials")
            return s3_uri, False
        if err_code == 403:
            LOGGER.warning(f"GET HEAD {s3_uri} -> 403: permission denied")
            return s3_uri, False
        if err_code == 404:
            LOGGER.warning(f"GET HEAD {s3_uri} -> 404: object missing")
            return s3_uri, False
        raise


async def async_s3_bucket_head(
    bucket_name: str,
    session: aiobotocore.AioSession = AIO_AWS_SESSION,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 bucket.

    :param bucket_name: an s3 bucket name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        async with session.create_client("s3") as s3_client:
            return await s3_client.head_bucket(Bucket=bucket_name)


async def async_s3_bucket_access(
    bucket_name: str,
    session: aiobotocore.AioSession = AIO_AWS_SESSION,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 bucket access is allowed.

    :param bucket_name: an s3 bucket name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (bucket_name: str, access: bool)
    """
    try:
        head_response = await async_s3_bucket_head(bucket_name, session, sem)

        if head_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return bucket_name, True

        return bucket_name, False

    except botocore.exceptions.ClientError as err:
        err_code = int(err.response["Error"]["Code"])
        if err_code == 401:
            LOGGER.warning(f"GET HEAD {bucket_name} -> 401: invalid credentials")
            return bucket_name, False
        if err_code == 403:
            LOGGER.warning(f"GET HEAD {bucket_name} -> 403: permission denied")
            return bucket_name, False
        if err_code == 404:
            LOGGER.warning(f"GET HEAD {bucket_name} -> 404: object missing")
            return bucket_name, False
        raise


async def run_async_aws_s3_buckets(
    session: aiobotocore.AioSession = AIO_AWS_SESSION,
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param session: an aiobotocore session object
    :return: dict of {bucket_name: str, access: bool}
    """
    loop = asyncio.get_event_loop()

    async with session.create_client("s3") as s3_client:
        response = await s3_client.list_buckets()

    bucket_tasks = []
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            bucket_task = loop.create_task(
                async_s3_bucket_access(bucket_name, session, CLIENT_SEMAPHORE)
            )
            bucket_tasks.append(bucket_task)

    await asyncio.gather(*bucket_tasks)

    access_buckets = {}
    for bucket_task in bucket_tasks:
        try:
            bucket_task.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError) as err:
            print(err)
            continue

        if bucket_task.done():
            name, access = bucket_task.result()
            access_buckets[name] = access

    return access_buckets


async def run_async_aws_s3_objects(
    bucket_name: str, bucket_prefix: str, session: aiobotocore.AioSession = AIO_AWS_SESSION
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param session: an aiobotocore session object
    :return: dict of {bucket_name: str, access: bool}
    """
    loop = asyncio.get_event_loop()

    async with session.create_client("s3") as s3_client:
        response = await s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)

    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    # TODO
    import ipdb

    ipdb.set_trace()

    object_tasks = []
    # if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
    #     for bucket in response["Buckets"]:
    #         bucket_name = bucket["Name"]
    #         bucket_task = loop.create_task(
    #             async_s3_object_access(bucket_name, session)
    #         )
    #         object_tasks.append(bucket_task)

    await asyncio.gather(*object_tasks)

    objects_data = {}
    for object_task in object_tasks:
        try:
            object_task.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError) as err:
            print(err)
            continue

        if object_task.done():
            name, access = object_task.result()
            objects_data[name] = access

    return objects_data


if __name__ == "__main__":

    # pylint: disable=C0103

    def summary_accessible_buckets(buckets):
        accessible_buckets = [k for k, v in buckets.items() if v]
        print()
        print(f"queried {len(buckets)} buckets")
        print(f"found {len(accessible_buckets)} accessible buckets")

    # https://registry.opendata.aws/noaa-goes/
    noaa_goes_bucket = "noaa-goes16"

    loop = asyncio.get_event_loop()

    try:
        aio_session = AIO_AWS_SESSION

        # aio_session.full_config
        # aio_session.get_config_variable('region')

        # aio_session.get_scoped_config() is the same as:
        # configs = aio_session.full_config
        # configs['profiles'][aio_session.profile]

        print()
        print("Try an s3 object that exists")
        s3_uri = f"s3://{noaa_goes_bucket}/index.html"
        head_response = loop.run_until_complete(async_s3_object_head(s3_uri, aio_session))
        print(f"GET HEAD {s3_uri} -> {head_response}\n")

        print()
        print("Try an s3 object that is missing; it raises.")
        try:
            s3_uri = f"s3://{noaa_goes_bucket}/missing.html"
            loop.run_until_complete(async_s3_object_head(s3_uri, aio_session))
        except botocore.exceptions.ClientError as err:
            err_code = int(err.response["Error"]["Code"])
            if err_code == 401:
                print(f"GET HEAD {s3_uri} -> 401: invalid credentials\n")
            if err_code == 403:
                print(f"GET HEAD {s3_uri} -> 403: permission denied\n")
            if err_code == 404:
                print(f"GET HEAD {s3_uri} -> 404: object missing\n")

        print()
        print("async-aws checks for bucket access.")
        start = time.perf_counter()
        buckets = loop.run_until_complete(run_async_aws_s3_buckets(aio_session))
        end = time.perf_counter() - start
        summary_accessible_buckets(buckets)
        print(f"async-aws finished in {end:0.2f} seconds.\n")

    finally:
        loop.stop()
        loop.close()

    # print()
    # print("Run sequential bucket access checks.")
    # start = time.perf_counter()
    # buckets = run_aws_s3_seq_buckets()
    # end = time.perf_counter() - start
    # summary_accessible_buckets(buckets)
    # print(f"aws-sequential finished in  {end:0.2f} seconds.\n")

    print()
    print("Run sequential collection for all objects in a bucket-prefix")
    prefix = "ABI-L2-ADPC/2020"
    start = time.perf_counter()
    s3_bucket = boto3.resource("s3").Bucket(noaa_goes_bucket)
    objects = s3_bucket.objects.filter(Prefix=prefix)
    uris = []
    for s3_summary in objects:
        uri = f"s3://{s3_summary.bucket_name}/{s3_summary.key}"
        uris.append(uri)
    end = time.perf_counter() - start
    print(f"found {len(uris)} s3 objects")
    print(f"finished in {end:0.2f} seconds.\n")
