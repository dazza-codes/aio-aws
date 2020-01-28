#! /usr/bin/env python3
# pylint: disable=bad-continuation
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

.. _aiobotocore: https://aiobotocore.readthedocs.io/en/latest/
.. _aiohttp: https://aiohttp.readthedocs.io/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _botocore: https://botocore.amazonaws.com/v1/documentation/api/latest/index.html
"""

from dataclasses import dataclass
from typing import Dict
from typing import Optional
from typing import Tuple

import asyncio
import time

import botocore.session  # type: ignore
import aiobotocore  # type: ignore

from notes.logger import LOGGER


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


def run_sync_aws_s3() -> Dict[str, bool]:
    """
    Sequential, synchronous approach to list all s3 buckets
    and issue a HEAD request to check if access is allowed.

    :return: dict of {bucket_name: str, access: bool}
    """
    access_buckets = {}
    session = botocore.session.get_session()
    s3_client = session.create_client("s3", region_name="us-west-2")
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
    s3_uri: str, s3_region: str, session: aiobotocore.AioSession, sem: asyncio.Semaphore
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 object.

    :param s3_uri: an s3 URI
    :param s3_region: an s3 region name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        # asyncio.Semaphore limits the number of client connections
        async with session.create_client("s3", region_name=s3_region) as s3_client:
            s3_parts = S3Parts.parse_s3_uri(s3_uri)
            return await s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)


async def async_s3_object_access(
    s3_uri: str, s3_region: str, session: aiobotocore.AioSession, sem: asyncio.Semaphore
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 object access is allowed.

    :param s3_uri: an s3 URI
    :param s3_region: an s3 region name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (s3_uri: str, access: bool)
    """
    try:

        head_response = await async_s3_object_head(s3_uri, s3_region, session, sem)

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
    bucket_name: str, s3_region: str, session: aiobotocore.AioSession, sem: asyncio.Semaphore
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 bucket.

    :param bucket_name: an s3 bucket name
    :param s3_region: an s3 region name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        # asyncio.Semaphore limits the number of clients and connections
        async with session.create_client("s3", region_name=s3_region) as s3_client:
            return await s3_client.head_bucket(Bucket=bucket_name)


async def async_s3_bucket_access(
    bucket_name: str, s3_region: str, session: aiobotocore.AioSession, sem: asyncio.Semaphore
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 bucket access is allowed.

    :param bucket_name: an s3 bucket name
    :param s3_region: an s3 region name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (bucket_name: str, access: bool)
    """
    try:
        head_response = await async_s3_bucket_head(bucket_name, s3_region, session, sem)

        if head_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return (bucket_name, True)

        return (bucket_name, False)

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


async def run_async_aws_s3(s3_region: str, session: aiobotocore.AioSession) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param s3_region: an s3 region name
    :param session: an aiobotocore session object
    :return: dict of {bucket_name: str, access: bool}
    """

    client_limits = asyncio.Semaphore(20)  # limit client connections

    # session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3", region_name=s3_region) as s3_client:
        response = await s3_client.list_buckets()

    bucket_tasks = []
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            bucket_task = loop.create_task(
                async_s3_bucket_access(bucket_name, s3_region, session, client_limits)
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


if __name__ == "__main__":

    # pylint: disable=C0103

    # https://registry.opendata.aws/noaa-goes/
    noaa_goes_bucket = "noaa-goes16"
    aws_region = "us-east-1"

    loop = asyncio.get_event_loop()

    try:
        async_session = aiobotocore.get_session(loop=loop)

        async def s3_object_head(s3_uri: str) -> Dict:

            async with async_session.create_client("s3", region_name=aws_region) as s3_client:
                s3_parts = S3Parts.parse_s3_uri(s3_uri)
                return await s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)

        print()
        print("Try an s3 object that exists")
        s3_uri = f"s3://{noaa_goes_bucket}/index.html"
        head_response = loop.run_until_complete(s3_object_head(s3_uri))
        print(f"GET HEAD {s3_uri} -> {head_response}\n")

        print()
        print("Try an s3 object that is missing; it raises.")
        try:
            s3_uri = f"s3://{noaa_goes_bucket}/missing.html"
            loop.run_until_complete(s3_object_head(s3_uri))
        except botocore.exceptions.ClientError as err:
            err_code = int(err.response["Error"]["Code"])
            if err_code == 401:
                print(f"GET HEAD {s3_uri} -> 401: invalid credentials\n")
            if err_code == 403:
                print(f"GET HEAD {s3_uri} -> 403: permission denied\n")
            if err_code == 404:
                print(f"GET HEAD {s3_uri} -> 404: object missing\n")

        print()
        print("Async checks for access to all available buckets.")
        start = time.perf_counter()
        buckets = loop.run_until_complete(run_async_aws_s3(aws_region, async_session))
        accessible_buckets = [k for k, v in buckets.items() if v]
        print()
        print(f"async-aws queried {len(buckets)} buckets")
        print(f"async-aws found {len(accessible_buckets)} accessible buckets")
        end = time.perf_counter() - start
        print(f"async-aws finished in {end:0.2f} seconds.\n")

    finally:
        loop.stop()
        loop.close()

    print()
    print("Sync checks for access to all available buckets.")
    start = time.perf_counter()
    buckets = run_sync_aws_s3()
    accessible_buckets = [k for k, v in buckets.items() if v]
    print()
    print(f"sync-aws queried {len(buckets)} buckets")
    print(f"sync-aws found {len(accessible_buckets)} accessible buckets")
    end = time.perf_counter() - start
    print(f"sync-aws finished in  {end:0.2f} seconds.\n")
