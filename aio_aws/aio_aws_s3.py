#! /usr/bin/env python3
# pylint: disable=bad-continuation

# Copyright 2019-2021 Darren Weber
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
AioAWS S3
=========

Example code for async AWS S3 services. The `aiobotocore`_ library wraps a
release of `botocore`_ to patch it with features for async coroutines using
`asyncio`_ and `aiohttp`_. The simple example code iterates on a list of
buckets or objects to issue a HEAD request, as a simple way to determine
whether access is permitted. The example compares an async approach (using
aiobotocore) vs a sequential blocking approach (using botocore). The async
approach uses a client connection limiter, based on ``asyncio.Semaphore(20)``.

.. code-block:: shell

    $ ./aio_aws/aio_aws_s3.py

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

"""

import asyncio
import concurrent.futures
import time
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Tuple

from aiobotocore.client import AioBaseClient
from aiobotocore.config import AioConfig
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from aio_aws.aio_aws_config import RETRY_EXCEPTIONS
from aio_aws.aio_aws_config import AioAWSConfig
from aio_aws.aio_aws_config import aio_aws_session
from aio_aws.aio_aws_config import jitter
from aio_aws.logger import get_logger
from aio_aws.utils import handle_head_error_code
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)


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


def aws_s3_bucket_access(bucket_name: str, s3_client: BaseClient) -> bool:
    """
    Check access to an S3 bucket by issuing a HEAD request

    :param bucket_name: An s3 bucket name
    :param s3_client: botocore.client.BaseClient for s3
    :return: a tuple of ``(bucket_name: str, access: bool)``
    """
    try:
        head = s3_client.head_bucket(Bucket=bucket_name)
        if response_success(head):
            return True
        else:
            return False

    except ClientError as err:
        access = handle_head_error_code(err, bucket_name)
        if access is False:
            return False
        raise


def aws_s3_buckets_access(s3_client: BaseClient) -> Dict[str, bool]:
    """
    A concurrent.futures approach to list all s3 buckets
    and issue a HEAD request to check if access is allowed.  This
    is used in performance comparisons between botocore and
    aiobotocore functionality.

    :param s3_client: botocore.client.BaseClient for s3
    :return: a tuple of ``(bucket_name: str, access: bool)``
    """
    buckets = []
    response = s3_client.list_buckets()
    if response_success(response):
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            buckets.append(bucket_name)

    # This pattern is FAST
    buckets_access = {}
    max_workers = s3_client.meta.config.max_pool_connections
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        bucket_futures = {
            executor.submit(aws_s3_bucket_access, bucket, s3_client): bucket
            for bucket in buckets
        }
        for future in concurrent.futures.as_completed(bucket_futures):
            bucket = bucket_futures[future]
            access = future.result()
            buckets_access[bucket] = access
    return buckets_access


async def aio_s3_bucket_head(
    bucket_name: str, config: AioAWSConfig, s3_client: AioBaseClient
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 bucket.

    :param bucket_name: an s3 bucket name
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    for tries in range(config.retries + 1):
        try:
            LOGGER.debug("AWS S3 bucket HEAD: %s", bucket_name)
            response = await s3_client.head_bucket(Bucket=bucket_name)
            LOGGER.debug("AWS S3 bucket HEAD: %s", response)
            return response

        except ClientError as err:
            LOGGER.debug("AWS S3 bucket HEAD error: %s", err.response)
            error = err.response.get("Error", {})
            if error.get("Code") in RETRY_EXCEPTIONS:
                if tries < config.retries:
                    await jitter("s3-bucket-head", config.min_jitter, config.max_jitter)
            else:
                raise

    raise RuntimeError("AWS S3 bucket HEAD exceeded retries")


async def aio_s3_bucket_access(
    bucket_name: str, config: AioAWSConfig, s3_client: AioBaseClient
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 bucket access is allowed.

    :param bucket_name: an s3 bucket name
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a tuple of ``(bucket_name: str, access: bool)``
    """
    try:
        head = await aio_s3_bucket_head(bucket_name, config=config, s3_client=s3_client)
        if response_success(head):
            return bucket_name, True
        return bucket_name, False
    except ClientError as err:
        access = handle_head_error_code(err, bucket_name)
        if access is False:
            return bucket_name, False
        raise


async def aio_s3_buckets_list(config: AioAWSConfig, s3_client: AioBaseClient) -> Dict:
    """
    Asynchronous coroutine to list all buckets.

    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a response to a ListBuckets request
    """
    for tries in range(config.retries + 1):
        try:
            LOGGER.debug("AWS S3 buckets list")
            response = await s3_client.list_buckets()
            LOGGER.debug("AWS S3 buckets list: %s", response)
            return response

        except ClientError as err:
            LOGGER.debug("AWS S3 buckets list error: %s", err.response)
            error = err.response.get("Error", {})
            if error.get("Code") in RETRY_EXCEPTIONS:
                if tries < config.retries:
                    await jitter("s3-bucket-list", config.min_jitter, config.max_jitter)
            else:
                raise

    raise RuntimeError("AWS S3 buckets list exceeded retries")


async def aio_s3_buckets_access(
    buckets: List[str],
    config: AioAWSConfig,
    s3_client: AioBaseClient,
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 buckets to check if each bucket allows access.

    :param buckets: a list of bucket names to check; the default is
                    to issue a request for all buckets in the session region
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: dict of ``{bucket_name: str, access: bool}``
    """
    access_coroutines = [
        aio_s3_bucket_access(bucket, config=config, s3_client=s3_client)
        for bucket in buckets
    ]
    bucket_access = {}
    for task in asyncio.as_completed(access_coroutines):
        result = await task
        bucket, access = result
        bucket_access[bucket] = access

    return bucket_access


async def aio_s3_object_head(
    s3_uri: str, config: AioAWSConfig, s3_client: AioBaseClient
) -> Dict:
    """
    Asynchronous coroutine to issue a HEAD request for s3 object.

    :param s3_uri: an s3 URI
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a response to a HEAD request
    :raises: botocore.exceptions.ClientError
    """
    s3_parts = S3Parts.parse_s3_uri(s3_uri)
    for tries in range(config.retries + 1):
        try:
            LOGGER.debug("AWS S3 object HEAD: %s", s3_uri)
            response = await s3_client.head_object(
                Bucket=s3_parts.bucket, Key=s3_parts.key
            )
            LOGGER.debug("AWS S3 object HEAD: %s", response)
            return response

        except ClientError as err:
            LOGGER.debug("AWS S3 object HEAD error: %s", err.response)
            error = err.response.get("Error", {})
            if error.get("Code") in RETRY_EXCEPTIONS:
                if tries < config.retries:
                    await jitter("s3-object-head", config.min_jitter, config.max_jitter)
                continue  # allow it to retry, if possible
            else:
                raise

    raise RuntimeError("AWS S3 object HEAD exceeded retries")


async def aio_s3_object_access(
    s3_uri: str, config: AioAWSConfig, s3_client: AioBaseClient
) -> Tuple[str, bool]:
    """
    Asynchronous coroutine to issue a HEAD request to check if s3 object access is allowed.

    :param s3_uri: an s3 URI
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a tuple of ``(s3_uri: str, access: bool)``
    """
    try:
        head = await aio_s3_object_head(s3_uri, config=config, s3_client=s3_client)
        if response_success(head):
            return s3_uri, True
        return s3_uri, False
    except ClientError as err:
        access = handle_head_error_code(err, s3_uri)
        if access is False:
            return s3_uri, False
        raise


async def aio_s3_objects_list(
    bucket_name: str,
    bucket_prefix: str,
    config: AioAWSConfig,
    s3_client: AioBaseClient,
) -> List[Dict]:
    """
    Asynchronous coroutine to collect all objects in a bucket prefix.

    :param bucket_name: param passed to s3_client.list_objects_v2 'Bucket'
    :param bucket_prefix: param passed to s3_client.list_objects_v2 'Prefix'
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: a list of s3 object data, e.g.

        .. code-block::

            >>> aio_s3_objects[0]
            {'ETag': '"192e29f360ea8297b5876b33b8419741"',
             'Key': 'ABI-L2-ADPC/2019/337/13/OR_ABI-L2-ADPC-M6_G16_s20193371331167_e20193371333539_c20193371334564.nc',
             'LastModified': datetime.datetime(2019, 12, 3, 14, 27, 5, tzinfo=tzutc()),
             'Size': 892913,
             'StorageClass': 'INTELLIGENT_TIERING'}

    .. seealso:
        - https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    """
    for tries in range(config.retries + 1):
        try:
            LOGGER.debug(
                "AWS S3 list objects, get first page: %s/%s",
                bucket_name,
                bucket_prefix,
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
                            "AWS S3 list objects OK? %s", response_success(response)
                        )
                        continue
                    else:
                        break
                else:
                    break

            return s3_objects

        except ClientError as err:
            LOGGER.debug("AWS S3 list objects error: %s", err.response)
            error = err.response.get("Error", {})
            if error.get("Code") in RETRY_EXCEPTIONS:
                if tries < config.retries:
                    await jitter(
                        "s3-list-objects", config.min_jitter, config.max_jitter
                    )
                continue  # allow it to retry, if possible
            else:
                raise

    raise RuntimeError("AWS S3 list objects exceeded retries")


async def aio_s3_objects_access(
    s3_uris: List[str],
    config: AioAWSConfig,
    s3_client: AioBaseClient,
) -> Dict[str, bool]:
    """
    Asynchronous coroutine to issue HEAD requests on all available
    s3 objects to check if each s3_object allows access.

    :param s3_uris: a list of s3 uris in the form 's3://bucket-name/key'
    :param config: an AioAWSConfig
    :param s3_client: an AioBaseClient for s3
    :return: dict of ``{s3_uri: str, access: bool}``
    """
    access_coroutines = [
        aio_s3_object_access(s3_uri, config=config, s3_client=s3_client)
        for s3_uri in s3_uris
    ]
    object_access = {}
    for task in asyncio.as_completed(access_coroutines):
        result = await task
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

    async def aio_main():

        # Use a single session with a larger connection pool.
        # The AioAWSConfig can provide clients on-demand with
        # a context manager pattern.
        aio_session = aio_aws_session()
        aio_config = AioAWSConfig(
            max_pool_connections=40,
            session=aio_session,
            min_pause=0.2,
            max_pause=0.6,
            min_jitter=0.2,
            max_jitter=0.8,
        )

        client_config = aio_config.session.get_default_client_config()
        s3_config = AioConfig(signature_version=UNSIGNED)
        s3_config = client_config.merge(s3_config)

        async with aio_config.create_client("s3", config=s3_config) as s3_client:

            print()
            print("aio-aws check an s3 object that exists")
            s3_uri = f"s3://{noaa_goes_bucket}/index.html"
            head_response = await aio_s3_object_head(
                s3_uri, config=aio_config, s3_client=s3_client
            )
            print(f"GET HEAD {s3_uri} -> {head_response}\n")

            print()
            print("aio-aws check an s3 object that is missing; it raises.")
            try:
                s3_uri = f"s3://{noaa_goes_bucket}/missing.html"
                await aio_s3_object_head(s3_uri, config=aio_config, s3_client=s3_client)
            except ClientError as err:
                err_code = err.response.get("Error", {}).get("Code")
                if err_code == "401":
                    print(f"GET HEAD {s3_uri} -> 401: invalid credentials\n")
                if err_code == "403":
                    print(f"GET HEAD {s3_uri} -> 403: permission denied\n")
                if err_code == "404":
                    print(f"GET HEAD {s3_uri} -> 404: object missing\n")

            # # This fails with a strange HTML response and a boto3 error
            # # botocore.parsers.ResponseParserError - so maybe any
            # # UNSIGNED requests are not able to list-buckets?
            # print()
            # print("aio-aws find all buckets that allow access.")
            # start = time.perf_counter()
            # buckets_response = await aio_s3_buckets_list(
            #     config=aio_config, s3_client=s3_client
            # )
            # s3_buckets = [bucket["Name"] for bucket in buckets_response["Buckets"]]
            # aio_bucket_access = await aio_s3_buckets_access(
            #     s3_buckets, config=aio_config, s3_client=s3_client
            # )
            # end = time.perf_counter() - start
            # summary_accessible_buckets(aio_bucket_access)
            # print(f"finished in {end:0.2f} seconds.\n")

            print()
            print("aio-aws collection of all objects in a bucket-prefix.")
            start = time.perf_counter()
            aio_s3_objects = await aio_s3_objects_list(
                bucket_name=noaa_goes_bucket,
                bucket_prefix=noaa_prefix,
                config=aio_config,
                s3_client=s3_client,
            )
            aio_s3_uris = [
                f"s3://{noaa_goes_bucket}/{obj['Key']}" for obj in aio_s3_objects
            ]
            print(f"found {len(aio_s3_uris)} s3 objects")
            end = time.perf_counter() - start
            print(f"finished in {end:0.2f} seconds.\n")
            set_aio_s3_uris = set(aio_s3_uris)
            assert len(aio_s3_uris) == len(set_aio_s3_uris)  # are they unique?

            print()
            print("aio-aws checks for access to all objects in a bucket-prefix.")
            start = time.perf_counter()
            aio_s3_access = await aio_s3_objects_access(
                s3_uris=aio_s3_uris, config=aio_config, s3_client=s3_client
            )
            print(f"checked access to {len(aio_s3_access)} s3 objects")
            end = time.perf_counter() - start
            print(f"finished in {end:0.2f} seconds.\n")

    asyncio.run(aio_main())

    # run_seq = True
    # if run_seq:
    #     print()
    #     print("aws-sequential find all buckets that allow access.")
    #     start = time.perf_counter()
    #     boto_session = botocore.session.get_session()
    #     boto_client = boto_session.create_client("s3")
    #     boto_bucket_access = aws_s3_buckets_access(boto_client)
    #     end = time.perf_counter() - start
    #     summary_accessible_buckets(boto_bucket_access)
    #     print(f"finished in {end:0.2f} seconds.\n")
    #     assert boto_bucket_access == aio_bucket_access
    #
    #     print()
    #     print("aws-sequential collection for all objects in a bucket-prefix")
    #     start = time.perf_counter()
    #     s3_bucket = boto3.resource("s3").Bucket(noaa_goes_bucket)
    #     objects_collection = s3_bucket.objects.filter(Prefix=noaa_prefix)
    #     s3_uris = [
    #         f"s3://{s3_summary.bucket_name}/{s3_summary.key}"
    #         for s3_summary in objects_collection
    #     ]
    #     print(f"found {len(s3_uris)} s3 objects")
    #     end = time.perf_counter() - start
    #     print(f"finished in {end:0.2f} seconds.\n")
    #
    #     print()
    #     print("Checking equality of collections for aio-aws vs. aws-sync")
    #     set_s3_uris = set(s3_uris)
    #     assert len(s3_uris) == len(set_s3_uris)  # are they unique?
    #
    #     # same set of URIS?  Sometimes it's off-by-one or a few and
    #     # maybe that's due to s3-consistency and object additions or removal
    #     assert len(s3_uris) == len(aio_s3_uris)
    #     assert set_aio_s3_uris == set_s3_uris
    #     print("The collections for aio-aws vs. aws-sync match OK")
