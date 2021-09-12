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
S3 AIO
======

These functions provide non-blocking serialization to files
and s3 for GeoJSON, GeoJSONSeq, JSON, and YAML.

"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Thread
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import aiofiles
import yaml
from aiobotocore.client import AioBaseClient
from aiofiles.tempfile import NamedTemporaryFile
from botocore.exceptions import ClientError

from aio_aws.aio_aws_config import aio_aws_client
from aio_aws.logger import get_logger
from aio_aws.s3_uri import S3URI
from aio_aws.s3_uri import S3Info
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)


@asynccontextmanager
async def s3_aio_client(*args, **kwargs) -> AioBaseClient:
    """
    This creates a aiobotocore.client.AioBaseClient that uses aiohttp
    for asynchronous s3 requests

    :return: a aiobotocore.client.AioBaseClient for s3
    """
    if kwargs.get("endpoint_url") is None:
        # To make it easier to use alternative s3 endpoints,
        # especially for unit testing, check for an env-var.
        s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
        if s3_endpoint_url:
            kwargs["endpoint_url"] = s3_endpoint_url

    async with aio_aws_client("s3", *args, **kwargs) as s3_client:
        yield s3_client


async def s3_file_info(s3_uri: Union[S3URI, str], s3_client: AioBaseClient) -> S3Info:
    """
    Collect data from an S3 HEAD request for an S3URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: an S3Info object with HEAD data on success;
        on failure the S3Info object has no HEAD data
    """
    if isinstance(s3_uri, str):
        s3_uri = S3URI(s3_uri)

    s3_info = S3Info(s3_uri=s3_uri)
    try:
        s3_head = await s3_client.head_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
        if response_success(s3_head):
            # LastModified is a datetime.datetime
            s3_info.last_modified = s3_head["LastModified"]
            s3_info.s3_size = int(s3_head["ContentLength"])
            LOGGER.debug("Success S3URI info: %s", s3_uri)
    except ClientError as err:
        LOGGER.debug("Failed S3URI info: %s", s3_uri)
        LOGGER.debug(err)
    return s3_info


async def s3_files_info(
    s3_uris: List[Union[S3URI, str]], s3_client: AioBaseClient
) -> List[S3Info]:
    """
    Collect data from S3 HEAD requests for many S3URI

    :param s3_uris: a list of S3URI
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: a list of S3Info object with HEAD data on success;
        on failure the S3Info object has no HEAD data
    """
    s3_files = []
    s3_futures = []
    for s3_uri in s3_uris:
        s3_future = asyncio.ensure_future(s3_file_info(s3_uri, s3_client))
        s3_futures.append(s3_future)
    for future in asyncio.as_completed(s3_futures):
        try:
            s3_info = await future
            s3_files.append(s3_info)
        except Exception as err:
            LOGGER.error(err)
            pass

    return s3_files


async def get_s3_content(s3_uri: str, s3_client: AioBaseClient):
    """
    Read an s3 object

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the data from the s3 object
    """
    try:
        s3_uri = S3URI(s3_uri)
        LOGGER.info("Read S3URI: %s", s3_uri.s3_uri)
        content_object = await s3_client.get_object(
            Bucket=s3_uri.bucket, Key=s3_uri.key
        )
        file_content = await content_object["Body"].read()
        return file_content.decode("utf-8")
    except ClientError as err:
        LOGGER.error("Failed S3 GET for: %s", s3_uri)
        LOGGER.error(err)


async def put_s3_content(
    data_file: str, s3_uri: str, s3_client: AioBaseClient
) -> Optional[str]:
    """
    Write a file to an s3 object

    :param data_file: a data file
    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3_uri)
    try:
        async with aiofiles.open(data_file, "rb") as fd:
            file_bytes = await fd.read()
            response = await s3_client.put_object(
                Bucket=s3_uri.bucket, Key=s3_uri.key, Body=file_bytes
            )
            success = response_success(response)
            if success:
                exists_waiter = s3_client.get_waiter("object_exists")
                await exists_waiter.wait(Bucket=s3_uri.bucket, Key=s3_uri.key)
                return str(s3_uri)
    except ClientError as err:
        LOGGER.error("Failed S3 PUT to: %s", s3_uri)
        LOGGER.error(err)


async def json_dump(data: Any, file: Union[Path, str]) -> Optional[Union[Path, str]]:
    """
    Write JSON to a file

    :param data: any data compatible with json.dumps
    :param file: a file path to write
    :return: if the dump succeeds, return the file, or None
    """
    dump = json.dumps(data)
    async with aiofiles.open(file, mode="w") as dst:
        await dst.write(dump)
    file_path = Path(file)
    if file_path.is_file() and file_path.stat().st_size > 0:
        LOGGER.info("Saved JSON to %s", file)
        return file
    LOGGER.error("Failed to save JSON to %s", file)


async def json_s3_dump(
    json_data: Any, s3_uri: str, s3_client: AioBaseClient
) -> Optional[str]:
    """
    Write JSON to an s3 URI

    :param json_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3_uri)
    success = False
    tmp_file = None
    try:
        async with NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            async with aiofiles.open(o_file.name, "w") as fd:
                dumps = json.dumps(json_data)
                await fd.write(dumps)

        s3_obj = await put_s3_content(
            data_file=tmp_file, s3_uri=str(s3_uri), s3_client=s3_client
        )
        if s3_obj:
            success = True

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved S3URI: %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save S3URI: %s", str(s3_uri))


async def json_s3_load(s3_uri: str, s3_client: AioBaseClient) -> Any:
    """
    Read JSON data from an s3 object

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: data from the json load
    """
    file_content = await get_s3_content(s3_uri, s3_client=s3_client)
    json_data = json.loads(file_content)
    return json_data


async def geojson_s3_load(s3_uri: str, s3_client: AioBaseClient) -> Dict:
    """
    Read GeoJSON data from an s3 object

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: geojson data
    """
    file_content = await get_s3_content(s3_uri, s3_client=s3_client)
    geojson = json.loads(file_content)
    return geojson


async def geojson_s3_dump(
    geojson_data: Any, s3_uri: str, s3_client: AioBaseClient
) -> Optional[str]:
    """
    Write GeoJSON to an s3 URI

    :param geojson_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the s3 URI on success
    """
    return await json_s3_dump(
        json_data=geojson_data, s3_uri=s3_uri, s3_client=s3_client
    )


async def geojsons_s3_load(s3_uri: str, s3_client: AioBaseClient) -> List[Dict]:
    """
    Read GeoJSON Text Sequence data from an s3 object

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: geojson features

    .. seealso::

        - https://tools.ietf.org/html/rfc8142

    """
    file_content = await get_s3_content(s3_uri, s3_client=s3_client)
    geojsons = file_content.splitlines()
    # some of the geojsons lines could be empty
    features = []
    while geojsons:
        feature = geojsons.pop(0).strip()
        if feature:
            features.append(json.loads(feature))
    return features


async def geojsons_s3_dump(
    geojson_features: List[Dict], s3uri: str, s3_client: AioBaseClient
) -> Optional[str]:
    """
    Write GeoJSON Text Sequence files to an s3 URI

    [GeoJSON Text Sequences](https://tools.ietf.org/html/rfc8142) are
    lines of geojson features that are designed for streaming
    operations on large datasets. These files can be loaded by
    geopandas, using fiona `driver="GeoJSONSeq"`, which
    can be auto-detected.  For example:

    .. code-block::

        import geopandas as gpd

        s3_uri = "s3://your-bucket/prefix/input.geojsons"
        gdf = gpd.read_file(s3_uri)

    :param geojson_features: a list of geojson features; from any
        feature collection, this is geojson_collection["features"]
    :param s3uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3uri)
    success = False
    tmp_file = None
    try:
        async with NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            async with aiofiles.open(o_file.name, "w") as fd:
                for feature in geojson_features:
                    dumps = json.dumps(feature)
                    await fd.write(dumps + "\n")

        s3_obj = await put_s3_content(
            data_file=tmp_file, s3_uri=str(s3_uri), s3_client=s3_client
        )
        if s3_obj:
            success = True

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved GeoJSONSeq to %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save GeoJSONSeq to %s", s3_uri)


async def geojsons_dump(
    geojson_features: List[Dict], geojsons_file: Union[Path, str]
) -> Optional[Union[Path, str]]:
    """
    Write a GeoJSON Text Sequence file

    :param geojson_features: a list of geojson features; from any
        feature collection, this is geojson_collection["features"]
    :param geojsons_file: a file path to write
    :return: if the dump succeeds, return the geojsons_file, or None
    """
    async with aiofiles.open(geojsons_file, mode="w") as dst:
        for feature in geojson_features:
            dump = json.dumps(feature)
            await dst.write(dump + "\n")
    geojsons_path = Path(geojsons_file)
    if geojsons_path.is_file() and geojsons_path.stat().st_size > 0:
        LOGGER.info("Saved GeoJSONSeq to %s", geojsons_file)
        return geojsons_file
    LOGGER.error("Failed to save GeoJSONSeq to %s", geojsons_file)


async def yaml_s3_dump(
    yaml_data: Any, s3_uri: str, s3_client: AioBaseClient
) -> Optional[str]:
    """
    Write YAML to an s3 URI

    :param yaml_data: an object to yaml.dump
    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3_uri)
    success = False
    tmp_file = None
    try:
        async with NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            async with aiofiles.open(o_file.name, "w") as fd:
                dumps = yaml.dump(yaml_data)
                await fd.write(dumps)

        s3_obj = await put_s3_content(
            data_file=tmp_file, s3_uri=str(s3_uri), s3_client=s3_client
        )
        if s3_obj:
            success = True

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved S3URI: %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save S3URI: %s", str(s3_uri))


async def yaml_s3_load(s3_uri: str, s3_client: AioBaseClient) -> Any:
    """
    Read YAML data from an s3 object

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: data from the yaml load
    """
    file_content = await get_s3_content(s3_uri, s3_client=s3_client)
    yaml_data = yaml.safe_load(file_content)
    return yaml_data


async def yaml_dump(data: Any, file: Union[Path, str]) -> Optional[Union[Path, str]]:
    """
    Write YAML to a file

    :param data: any data compatible with yaml.safe_dump
    :param file: a file path to write
    :return: if the dump succeeds, return the file, or None
    """
    dump = yaml.safe_dump(data)
    async with aiofiles.open(file, mode="w") as dst:
        await dst.write(dump)
    file_path = Path(file)
    if file_path.is_file() and file_path.stat().st_size > 0:
        LOGGER.info("Saved YAML to %s", file)
        return file
    LOGGER.error("Failed to save YAML to %s", file)


async def s3_load_file(
    s3_uri: str, s3_client: AioBaseClient
) -> Tuple[str, Optional[Any]]:
    """
    Load various file types from s3; it supports files with a
    known file suffix, such as ".json", ".geojson", ".geojsons",
    ".yaml", ".yml"

    :param s3_uri: a fully qualified S3 URI for an s3 object
    :param s3_client: a required aiobotocore.client.AioBaseClient for s3
    :return: a Tuple[s3_uri, Optional[s3_data]] where the s3_data could
        be None if the file type is not recognized or a file read fails
    """
    data = None
    suffix = Path(s3_uri).suffix
    if suffix == ".json":
        data = await json_s3_load(s3_uri, s3_client)
    elif suffix == ".geojson":
        data = await geojson_s3_load(s3_uri, s3_client)
    elif suffix == ".geojsons":
        data = await geojsons_s3_load(s3_uri, s3_client)
    elif suffix in [".yaml", ".yml"]:
        data = await yaml_s3_load(s3_uri, s3_client)
    else:
        LOGGER.error("Unknown file type: %s", s3_uri)
    # This loader is required to return a tuple that
    # associates an s3 URI with it's data, which is required by
    # s3_load_files because futures cannot be used as dict keys
    # when using asyncio.as_completed
    return s3_uri, data


async def s3_load_files(
    s3_uris: List[str], *args, s3_client: AioBaseClient = None, **kwargs
) -> Dict[str, Optional[Any]]:
    """
    Collect data from S3 files in JSON or YAML formats

    :param s3_uris: a list of S3 URIs
    :param s3_client: an optional aiobotocore.client.AioBaseClient for s3
    :return: a Dict[s3_uri: s3_data] for all the s3 URIs
        that can be read successfully
    """

    async def _collect_content(_s3_uris, _s3_client):
        s3_files = {}
        s3_futures = []

        for s3_uri in _s3_uris:
            s3_files[s3_uri] = None
            s3_future = asyncio.ensure_future(s3_load_file(s3_uri, _s3_client))
            s3_futures.append(s3_future)

        for future in asyncio.as_completed(s3_futures):
            s3_uri, s3_data = await future
            s3_files[s3_uri] = s3_data

        return s3_files

    if s3_client is None:
        async with s3_aio_client(*args, **kwargs) as s3_client:
            return await _collect_content(s3_uris, s3_client)
    else:
        return await _collect_content(s3_uris, s3_client)


def run_s3_load_files(s3_uris: List[str], *args, **kwargs) -> Dict[str, Optional[Any]]:
    """
    Collect data from S3 files in JSON or YAML formats

    :param s3_uris: a list of S3 URIs
    :return: a Dict[s3_uri: s3_data] for all the s3 URIs
        that can be read successfully
    """

    def async_thread(*args, **kwargs):
        # Use asyncio.run on a clean thread to avoid
        # conflict with any existing event loop
        _result = kwargs.pop("result")
        dict_result = asyncio.run(s3_load_files(s3_uris, *args, **kwargs))
        _result.update(dict_result)

    result = {}
    kwargs["result"] = result
    thread = Thread(target=async_thread, args=args, kwargs=kwargs)
    thread.start()
    thread.join()
    return result
