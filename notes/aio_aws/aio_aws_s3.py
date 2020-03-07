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
buckets or objects to issue a HEAD request, as a simple way to determine
whether access is permitted. The example compares an async approach (using
aiobotocore) vs a sequential blocking approach (using botocore). The async
approach uses a client connection limiter, based on ``asyncio.Semaphore(20)``.

.. code-block:: shell

    $ ./notes/aio_aws/aio_aws_s3.py

    aio-aws check an s3 object that exists
    GET HEAD s3://noaa-goes16/index.html -> {'ResponseMetadata': {'RequestId': 'A2F4DF282C84341B', 'HostId': 'Rt45GH1taNRVUAPTg8XJfs4KLvri0OKos/6Wohp5tAfQa+moUT/9mC/0Wa9cYVQZcMAWKtIYfkE=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'Rt45GH1taNRVUAPTg8XJfs4KLvri0OKos/6Wohp5tAfQa+moUT/9mC/0Wa9cYVQZcMAWKtIYfkE=', 'x-amz-request-id': 'A2F4DF282C84341B', 'date': 'Tue, 03 Mar 2020 19:22:21 GMT', 'last-modified': 'Wed, 12 Feb 2020 19:14:27 GMT', 'etag': '"8f9810548f75fd8b8a16431c74ad54ac"', 'content-encoding': 'text/html', 'accept-ranges': 'bytes', 'content-type': 'text/html', 'content-length': '32356', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2020, 2, 12, 19, 14, 27, tzinfo=tzutc()), 'ContentLength': 32356, 'ETag': '"8f9810548f75fd8b8a16431c74ad54ac"', 'ContentEncoding': 'text/html', 'ContentType': 'text/html', 'Metadata': {}}

    aio-aws check an s3 object that is missing; it raises.
    GET HEAD s3://noaa-goes16/missing.html -> 404: object missing

    aio-aws find all buckets that allow access.
    queried 72 buckets
    found 69 accessible buckets
    finished in 2.42 seconds.

    aio-aws collection of all objects in a bucket-prefix.
    found 9015 s3 objects
    finished in 4.43 seconds.

    aws-sequential find all buckets that allow access.
    queried 72 buckets
    found 69 accessible buckets
    finished in 31.11 seconds.

    aws-sequential collection for all objects in a bucket-prefix
    found 9015 s3 objects
    finished in 35.72 seconds.

    Checking equality of collections for aio-aws vs. aws-sync
    The collections for aio-aws vs. aws-sync match OK


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
from typing import List
from typing import Optional
from typing import Tuple

import aiobotocore.client  # type: ignore
import aiobotocore.config  # type: ignore
import boto3
import botocore.client  # type: ignore
import botocore.endpoint  # type: ignore
import botocore.exceptions  # type: ignore
import botocore.session  # type: ignore
from dataclasses import dataclass

from notes.aio_aws.aio_aws import aio_aws_session
from notes.aio_aws.aio_aws import CLIENT_SEMAPHORE
from notes.aio_aws.aio_aws import jitter
from notes.aio_aws.aio_aws import MAX_JITTER
from notes.aio_aws.aio_aws import MIN_JITTER
from notes.aio_aws.aio_aws import response_success
from notes.aio_aws.logger import LOGGER


@dataclass(frozen=True)
class S3Parts:
    bucket: str
    key: str

    PROTOCOL: str = "s3://"
    DELIMITER: str = "/"

    @property
    def s3_uri(self):
        return f"{self.PROTOCOL}{self.bucket}{S3Parts.DELIMITER}{self.key}"

    @staticmethod
    def parse_s3_uri(uri: str) -> "S3Parts":
        uri = str(uri).strip()
        if not uri.startswith(S3Parts.PROTOCOL):
            raise ValueError(f"S3 URI must start with '{S3Parts.PROTOCOL}'")
        uri_path = uri.replace(S3Parts.PROTOCOL, "")
        uri_paths = uri_path.split(S3Parts.DELIMITER)
        bucket = uri_paths.pop(0)
        key = S3Parts.DELIMITER.join(uri_paths)

        return S3Parts(bucket, key)


def handle_head_error_code(
    error: botocore.exceptions.ClientError, item: str = None
) -> Optional[bool]:
    err_code = error.response.get("Error", {}).get("Code")
    if err_code == "401":
        LOGGER.debug("GET HEAD 401: invalid credentials: %s", item)
        return False
    if err_code == "403":
        LOGGER.debug("GET HEAD 403: permission denied: %s", item)
        return False
    if err_code == "404":
        LOGGER.debug("GET HEAD 404: object missing: %s", item)
        return False
    return None


def aws_s3_buckets_access(s3_client: botocore.client.BaseClient) -> Dict[str, bool]:
    """
    Sequential, synchronous approach to list all s3 buckets
    and issue a HEAD request to check if access is allowed.

    :param s3_client: botocore.client for s3
    :return: dict of {bucket_name: str, access: bool}
    """
    access_buckets = {}
    response = s3_client.list_buckets()
    if response_success(response):
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            try:
                head = s3_client.head_bucket(Bucket=bucket_name)
                if response_success(head):
                    access_buckets[bucket_name] = True
                else:
                    access_buckets[bucket_name] = False

            except botocore.exceptions.ClientError as err:
                access = handle_head_error_code(err, bucket_name)
                if access is False:
                    access_buckets[bucket_name] = False
                    continue
                raise

    return access_buckets


async def aio_s3_bucket_head(
    bucket_name: str,
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 bucket.

    :param bucket_name: an s3 bucket name
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1

            try:
                LOGGER.debug("AWS S3 bucket poke: %s", bucket_name)
                response = await s3_client.head_bucket(Bucket=bucket_name)
                LOGGER.debug("AWS S3 bucket poke: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                LOGGER.debug("AWS S3 bucket poke error: %s", err.response)
                err_code = err.response.get("Error", {}).get("Code")
                if err_code == "TooManyRequestsException":
                    if tries < 10:
                        await jitter("s3-bucket-poke", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS S3 bucket poke exceeded retries")


async def aio_s3_bucket_access(
    bucket_name: str,
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 bucket access is allowed.

    :param bucket_name: an s3 bucket name
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (bucket_name: str, access: bool)
    """
    try:
        head = await aio_s3_bucket_head(bucket_name, s3_client, sem)

        if response_success(head):
            return bucket_name, True

        return bucket_name, False

    except botocore.exceptions.ClientError as err:
        access = handle_head_error_code(err, bucket_name)
        if access is False:
            return bucket_name, False
        raise


async def aio_s3_buckets_list(
    s3_client: aiobotocore.client.AioBaseClient, sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to list all buckets.

    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a ListBuckets request
    """
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1

            try:
                LOGGER.debug("AWS S3 buckets list")
                response = await s3_client.list_buckets()
                LOGGER.debug("AWS S3 buckets list: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                LOGGER.debug("AWS S3 buckets list error: %s", err.response)
                err_code = err.response.get("Error", {}).get("Code")
                if err_code == "TooManyRequestsException":
                    if tries < 10:
                        await jitter("s3-buckets-list", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS S3 buckets list exceeded retries")


async def aio_s3_buckets_access(
    buckets: List[str],
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param buckets: a list of bucket names to check; the default is
                    to issue a request for all buckets in the session region
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: dict of {bucket_name: str, access: bool}
    """
    access_coroutines = [aio_s3_bucket_access(bucket, s3_client, sem) for bucket in buckets]
    results = await asyncio.gather(*access_coroutines)

    bucket_access = {}
    for result in results:
        bucket, access = result
        bucket_access[bucket] = access

    return bucket_access


async def aio_s3_object_head(
    s3_uri: str,
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 object.

    :param s3_uri: an s3 URI
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    s3_parts = S3Parts.parse_s3_uri(s3_uri)
    async with sem:
        tries = 0
        while tries < 10:
            tries += 1

            try:
                LOGGER.debug("AWS S3 URI poke: %s", s3_uri)
                response = await s3_client.head_object(Bucket=s3_parts.bucket, Key=s3_parts.key)
                LOGGER.debug("AWS S3 URI poke: %s", response)
                return response

            except botocore.exceptions.ClientError as err:
                LOGGER.debug("AWS S3 URI poke error: %s", err.response)
                err_code = err.response.get("Error", {}).get("Code")
                if err_code == "TooManyRequestsException":
                    if tries < 10:
                        await jitter("s3-poke", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS S3 poke exceeded retries")


async def aio_s3_object_access(
    s3_uri: str,
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 object access is allowed.

    :param s3_uri: an s3 URI
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: a tuple of (s3_uri: str, access: bool)
    """
    try:

        head = await aio_s3_object_head(s3_uri, s3_client, sem)

        if response_success(head):
            return s3_uri, True

        return s3_uri, False

    except botocore.exceptions.ClientError as err:
        access = handle_head_error_code(err, s3_uri)
        if access is False:
            return s3_uri, False
        raise


async def aio_s3_objects_list(
    bucket_name: str,
    bucket_prefix: str,
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> List[Dict]:
    """
    Asynchronous coroutine to collect all objects in a bucket prefix.

    :param bucket_name: param passed to s3_client.list_objects_v2 'Bucket'
    :param bucket_prefix: param passed to s3_client.list_objects_v2 'Prefix'
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: dict of {bucket_name: str, access: bool}

    .. seealso:
        - https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    """

    async with sem:
        tries = 0
        while tries < 10:
            tries += 1

            try:
                LOGGER.debug(
                    "AWS S3 list objects, get first page: %s/%s", bucket_name, bucket_prefix
                )

                response = await s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=bucket_prefix
                )
                LOGGER.debug("AWS S3 list objects OK? %s", response_success(response))

                s3_objects = []
                while True:
                    if response_success(response):
                        s3_objects += response["Contents"]

                        if response["IsTruncated"]:
                            LOGGER.debug(
                                "AWS S3 list objects, get next page: %s/%s",
                                bucket_name,
                                bucket_prefix,
                            )
                            next_token = response["NextContinuationToken"]
                            response = await s3_client.list_objects_v2(
                                Bucket=bucket_name,
                                Prefix=bucket_prefix,
                                ContinuationToken=next_token,
                            )
                            LOGGER.debug(
                                "AWS S3 list objects OK? %s", response_success(response),
                            )
                            continue
                        else:
                            break
                    else:
                        break

                return s3_objects

            except botocore.exceptions.ClientError as err:
                LOGGER.debug("AWS S3 list objects error: %s", err.response)
                err_code = err.response.get("Error", {}).get("Code")
                if err_code == "TooManyRequestsException":
                    if tries < 10:
                        await jitter("s3-list-objects", MIN_JITTER, MAX_JITTER)
                    continue  # allow it to retry, if possible
                else:
                    raise
        else:
            raise RuntimeError("AWS S3 list objects exceeded retries")


async def aio_s3_objects_access(
    s3_uris: List[str],
    s3_client: aiobotocore.client.AioBaseClient,
    sem: asyncio.Semaphore = CLIENT_SEMAPHORE,
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 objects to check if each s3_object allows access.

    :param s3_uris: a list of s3 uris in the form 's3://bucket-name/key'
    :param s3_client: an aiobotocore.client.AioBaseClient for s3
    :param sem: an asyncio.Semaphore to limit the number of active client connections
    :return: dict of {s3_uri: str, access: bool}
    """
    access_coroutines = [aio_s3_object_access(s3_uri, s3_client, sem) for s3_uri in s3_uris]
    results = await asyncio.gather(*access_coroutines)

    object_access = {}
    for result in results:
        s3_uri, access = result
        object_access[s3_uri] = access

    return object_access


if __name__ == "__main__":

    # pylint: disable=C0103

    #
    # Integration tests against NOAA public data on S3
    #

    def summary_accessible_buckets(buckets):
        accessible_buckets = [k for k, v in buckets.items() if v]
        print(f"queried {len(buckets)} buckets")
        print(f"found {len(accessible_buckets)} accessible buckets")

    # https://registry.opendata.aws/noaa-goes/
    noaa_goes_bucket = "noaa-goes16"
    noaa_prefix = "ABI-L2-ADPC/2019"  # use a prior year for stable results

    main_loop = asyncio.get_event_loop()

    # Use a single client with a larger connection pool.
    # Manage closing the client only after everything is done.
    # Passing a client to functions makes unit tests easier.
    MAX_CONNECTIONS = 20
    aio_semaphore = asyncio.Semaphore(MAX_CONNECTIONS)
    aio_config = aiobotocore.config.AioConfig(max_pool_connections=MAX_CONNECTIONS)
    aio_session = aio_aws_session(aio_config)
    aio_client = aio_session.create_client("s3")

    try:

        print()
        print("aio-aws check an s3 object that exists")
        s3_uri = f"s3://{noaa_goes_bucket}/index.html"
        head_response = main_loop.run_until_complete(
            aio_s3_object_head(s3_uri, s3_client=aio_client, sem=aio_semaphore)
        )
        print(f"GET HEAD {s3_uri} -> {head_response}\n")

        print()
        print("aio-aws check an s3 object that is missing; it raises.")
        try:
            s3_uri = f"s3://{noaa_goes_bucket}/missing.html"
            main_loop.run_until_complete(
                aio_s3_object_head(s3_uri, s3_client=aio_client, sem=aio_semaphore)
            )
        except botocore.exceptions.ClientError as err:
            err_code = err.response.get("Error", {}).get("Code")
            if err_code == "401":
                print(f"GET HEAD {s3_uri} -> 401: invalid credentials\n")
            if err_code == "403":
                print(f"GET HEAD {s3_uri} -> 403: permission denied\n")
            if err_code == "404":
                print(f"GET HEAD {s3_uri} -> 404: object missing\n")

        print()
        print("aio-aws find all buckets that allow access.")
        start = time.perf_counter()
        buckets_response = main_loop.run_until_complete(
            aio_s3_buckets_list(s3_client=aio_client, sem=aio_semaphore)
        )
        s3_buckets = [bucket["Name"] for bucket in buckets_response["Buckets"]]
        aio_bucket_access = main_loop.run_until_complete(
            aio_s3_buckets_access(s3_buckets, s3_client=aio_client, sem=aio_semaphore)
        )
        end = time.perf_counter() - start
        summary_accessible_buckets(aio_bucket_access)
        print(f"finished in {end:0.2f} seconds.\n")

        print()
        print("aio-aws collection of all objects in a bucket-prefix.")
        start = time.perf_counter()
        aio_s3_objects = main_loop.run_until_complete(
            aio_s3_objects_list(
                bucket_name=noaa_goes_bucket,
                bucket_prefix=noaa_prefix,
                s3_client=aio_client,
                sem=aio_semaphore,
            )
        )
        aio_s3_uris = [f"s3://{noaa_goes_bucket}/{obj['Key']}" for obj in aio_s3_objects]
        print(f"found {len(aio_s3_uris)} s3 objects")
        end = time.perf_counter() - start
        print(f"finished in {end:0.2f} seconds.\n")
        set_aio_s3_uris = set(aio_s3_uris)
        assert len(aio_s3_uris) == len(set_aio_s3_uris)  # are they unique?

        print()
        print("aio-aws checks for access to all objects in a bucket-prefix.")
        start = time.perf_counter()
        aio_s3_access = main_loop.run_until_complete(
            aio_s3_objects_access(s3_uris=aio_s3_uris, s3_client=aio_client, sem=aio_semaphore)
        )
        print(f"checked access to {len(aio_s3_access)} s3 objects")
        end = time.perf_counter() - start
        print(f"finished in {end:0.2f} seconds.\n")

    finally:
        main_loop.run_until_complete(aio_client.close())
        main_loop.stop()
        main_loop.close()

    run_seq = True
    if run_seq:
        print()
        print("aws-sequential find all buckets that allow access.")
        start = time.perf_counter()
        boto_session = botocore.session.get_session()
        boto_client = boto_session.create_client("s3")
        boto_bucket_access = aws_s3_buckets_access(boto_client)
        end = time.perf_counter() - start
        summary_accessible_buckets(boto_bucket_access)
        print(f"finished in {end:0.2f} seconds.\n")
        assert boto_bucket_access == aio_bucket_access

        print()
        print("aws-sequential collection for all objects in a bucket-prefix")
        start = time.perf_counter()
        s3_bucket = boto3.resource("s3").Bucket(noaa_goes_bucket)
        objects_collection = s3_bucket.objects.filter(Prefix=noaa_prefix)
        s3_uris = [
            f"s3://{s3_summary.bucket_name}/{s3_summary.key}"
            for s3_summary in objects_collection
        ]
        print(f"found {len(s3_uris)} s3 objects")
        end = time.perf_counter() - start
        print(f"finished in {end:0.2f} seconds.\n")

        print()
        print("Checking equality of collections for aio-aws vs. aws-sync")
        set_s3_uris = set(s3_uris)
        assert len(s3_uris) == len(set_s3_uris)  # are they unique?

        # same set of URIS?  Sometimes it's off-by-one or a few and
        # maybe that's due to s3-consistency and object additions or removal
        assert len(s3_uris) == len(aio_s3_uris)
        assert set_aio_s3_uris == set_s3_uris
        print("The collections for aio-aws vs. aws-sync match OK")
