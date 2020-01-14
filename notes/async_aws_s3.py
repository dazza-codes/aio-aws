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

from typing import Dict
from typing import Tuple

import asyncio
import time

import botocore.session  # type: ignore
import aiobotocore  # type: ignore


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


async def async_s3_bucket_access(
    bucket_name: str, session: aiobotocore.AioSession, sem: asyncio.Semaphore
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 bucket access is allowed.

    :param bucket_name: an s3 bucket name
    :param session: an aiobotocore session object
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (bucket_name: str, access: bool)
    """
    async with sem:
        # asyncio.Semaphore limits the number of clients and connections
        async with session.create_client("s3", region_name="us-west-2") as s3_client:

            try:
                head_response = await s3_client.head_bucket(Bucket=bucket_name)
                if head_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    return (bucket_name, True)

                return (bucket_name, False)

            except botocore.exceptions.ClientError as err:
                err_code = int(err.response["Error"]["Code"])
                if err_code == 403:
                    return (bucket_name, False)
                raise


async def run_async_aws_s3(session: aiobotocore.AioSession) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param session: an aiobotocore session object
    :return: dict of {bucket_name: str, access: bool}
    """

    client_limits = asyncio.Semaphore(50)  # limit to 50 clients/connections

    # session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3", region_name="us-west-2") as s3_client:
        response = await s3_client.list_buckets()

    bucket_tasks = []
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            bucket_task = loop.create_task(
                async_s3_bucket_access(bucket_name, session, client_limits)
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

    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    try:
        async_session = aiobotocore.get_session(loop=loop)
        buckets = loop.run_until_complete(run_async_aws_s3(async_session))
        accessible_buckets = [k for k, v in buckets.items() if v]
        print(f"async-aws queried {len(buckets)} buckets")
        print(f"async-aws found {len(accessible_buckets)} accessible buckets")
    finally:
        loop.stop()
        loop.close()
    end = time.perf_counter() - start
    print(f"async-aws finished in {end:0.2f} seconds.")

    start = time.perf_counter()
    buckets = run_sync_aws_s3()
    accessible_buckets = [k for k, v in buckets.items() if v]
    print(f"sync-aws queried {len(buckets)} buckets")
    print(f"sync-aws found {len(accessible_buckets)} accessible buckets")
    end = time.perf_counter() - start
    print(f"sync-aws finished in  {end:0.2f} seconds.")
