import concurrent.futures
import json
import os
import tempfile
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import boto3
import yaml
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from aio_aws.utils import response_success
from aio_aws.logger import get_logger
from aio_aws.s3_uri import S3URI

LOGGER = get_logger(__name__)


def s3_file_info(s3_uri: Union[S3URI, str], s3_client: BaseClient) -> Dict:
    info = {"s3_uri": s3_uri, "s3_size": None, "last_modified": None}
    s3_file = S3URI(str(s3_uri))
    try:
        s3_head = s3_client.head_object(Bucket=s3_file.bucket, Key=s3_file.key)
        if response_success(s3_head):
            # LastModified is a datetime.datetime
            info["last_modified"] = s3_head["LastModified"].isoformat()
            info["s3_size"] = s3_head["ContentLength"]
            LOGGER.debug("Success S3URI info: %s", s3_uri)
    except ClientError as err:
        LOGGER.debug("Failed S3URI info: %s", s3_uri)
        LOGGER.debug(err)
    return info


def s3_files_info(s3_uris: List[Union[S3URI, str]]) -> List[Dict]:

    # this creates a botocore.client.BaseClient that uses urllib3,
    # which should be thread-safe, with a default connection pool.
    s3_client = boto3.client("s3")

    s3_files = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        s3_futures = {}
        for s3_uri in s3_uris:
            s3_uri = S3URI(str(s3_uri))
            s3_future = executor.submit(s3_file_info, s3_uri.s3_uri, s3_client)
            s3_futures[s3_future] = s3_uri
        for future in concurrent.futures.as_completed(s3_futures):
            try:
                s3_info = future.result()
                s3_files.append(s3_info)
            except Exception as err:
                LOGGER.error(err)
                pass

    return s3_files


def get_s3_content(s3_uri: str):
    s3_uri = S3URI(s3_uri)
    LOGGER.info("Read s3-uri: %s", s3_uri.s3_uri)
    config = Config(retries={"max_attempts": 3, "mode": "standard"})
    s3 = boto3.client("s3", config=config)
    content_object = s3.get_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
    file_content = content_object["Body"].read().decode("utf-8")
    return file_content


def json_s3_dump(json_data: Any, s3_uri: str) -> Optional[str]:
    """
    Write JSON to an s3 URI

    :param json_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3_uri)
    success = False
    tmp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            with open(o_file.name, "w") as fd:
                json.dump(obj=json_data, fp=fd)
            o_file.flush()

        config = Config(retries={"max_attempts": 3, "mode": "standard"})
        s3 = boto3.client("s3", config=config)

        with open(tmp_file, "rb") as fd:
            response = s3.put_object(Bucket=s3_uri.bucket, Key=s3_uri.key, Body=fd)
            success = response_success(response)
            if success:
                # Use a boto3 waiter to confirm it worked
                exists_waiter = s3.get_waiter("object_exists")
                exists_waiter.wait(Bucket=s3_uri.bucket, Key=s3_uri.key)

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved S3URI: %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save S3URI: %s", str(s3_uri))


def json_s3_load(s3_uri: str) -> Any:
    """
    Load JSON from an s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :return: data from the json load
    """
    file_content = get_s3_content(s3_uri)
    json_data = json.loads(file_content)
    return json_data


def geojson_s3_load(s3_uri: str) -> Dict:
    """
    Read GeoJSON data from an s3 object

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :return: geojson data
    """
    file_content = get_s3_content(s3_uri)
    geojson = json.loads(file_content)
    return geojson


def geojson_s3_dump(geojson_data: Any, s3_uri: str) -> Optional[str]:
    """
    Write GeoJSON to an s3 URI

    :param geojson_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :return: the s3 URI on success
    """
    return json_s3_dump(json_data=geojson_data, s3_uri=s3_uri)


def geojsons_s3_load(s3_uri: str) -> List[Dict]:
    """
    Read GeoJSON Text Sequence data from an s3 object

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :return: geojson features

    .. seealso::

        - https://tools.ietf.org/html/rfc8142

    """
    file_content = get_s3_content(s3_uri)
    geojsons = file_content.splitlines()
    # some of the geojsons lines could be empty
    features = []
    while geojsons:
        feature = geojsons.pop(0).strip()
        if feature:
            features.append(json.loads(feature))
    return features


def geojsons_s3_dump(geojson_features: List[Dict], s3uri: str) -> Optional[str]:
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
    :param s3uri: a fully qualified S3 URI for the s3 object to write
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3uri)

    success = False
    tmp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            with open(o_file.name, "w") as fd:
                for feature in geojson_features:
                    json.dump(feature, fd)
                    fd.write("\n")
            o_file.flush()

        config = Config(retries={"max_attempts": 3, "mode": "standard"})
        s3 = boto3.client("s3", config=config)

        with open(tmp_file, "rb") as fd:
            response = s3.put_object(Bucket=s3_uri.bucket, Key=s3_uri.key, Body=fd)
            success = response_success(response)
            if success:
                # Use a boto3 waiter to confirm it worked
                exists_waiter = s3.get_waiter("object_exists")
                exists_waiter.wait(Bucket=s3_uri.bucket, Key=s3_uri.key)

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved GeoJSONSeq to %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save GeoJSONSeq to %s", s3_uri)


def geojsons_dump(
    geojson_features: List[Dict], geojsons_file: Union[Path, str]
) -> Optional[Union[Path, str]]:
    """
    :param geojson_features: a list of geojson features; from any
        feature collection, this is geojson_collection["features"]
    :param geojsons_file: a file path to write
    :return: if the dump succeeds, return the geojsons_file, or None
    """
    with open(geojsons_file, "w") as dst:
        for feature in geojson_features:
            json.dump(feature, dst)
            dst.write("\n")
    geojsons_path = Path(geojsons_file)
    if geojsons_path.is_file() and geojsons_path.stat().st_size > 0:
        LOGGER.info("Saved GeoJSONSeq to %s", geojsons_file)
        return geojsons_file
    LOGGER.error("Failed to save GeoJSONSeq to %s", geojsons_file)


def yaml_s3_dump(yaml_data: Any, s3_uri: str) -> Optional[str]:
    """
    Write YAML to an s3 URI

    :param yaml_data: an object to yaml.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :return: the s3 URI on success
    """
    s3_uri = S3URI(s3_uri)

    success = False
    tmp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as o_file:
            tmp_file = o_file.name
            with open(o_file.name, "w") as fd:
                yaml.safe_dump(yaml_data, fd)
            o_file.flush()

        config = Config(retries={"max_attempts": 3, "mode": "standard"})
        s3 = boto3.client("s3", config=config)

        with open(tmp_file, "rb") as fd:
            response = s3.put_object(Bucket=s3_uri.bucket, Key=s3_uri.key, Body=fd)
            success = response_success(response)
            if success:
                # Use a boto3 waiter to confirm it worked
                exists_waiter = s3.get_waiter("object_exists")
                exists_waiter.wait(Bucket=s3_uri.bucket, Key=s3_uri.key)

    finally:
        if tmp_file:
            os.unlink(tmp_file)

    if success:
        LOGGER.info("Saved S3URI: %s", str(s3_uri))
        return str(s3_uri)
    else:
        LOGGER.error("Failed to save S3URI: %s", str(s3_uri))


def yaml_s3_load(s3_uri: str) -> Any:
    """
    Load YAML from an s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :return: data from the yaml load
    """
    file_content = get_s3_content(s3_uri)
    yaml_data = yaml.safe_load(file_content)
    return yaml_data
