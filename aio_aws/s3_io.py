# Copyright 2019-2022 Darren Weber
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
S3 IO
=====

These functions provide blocking serialization to files
and s3 for GeoJSON, GeoJSONSeq, JSON, and YAML.

"""

import concurrent.futures
import json
import os
import tempfile
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

import boto3
import botocore.client
import botocore.exceptions
import yaml
from botocore.client import BaseClient
from botocore.config import Config
from pydantic import BaseModel

from aio_aws.logger import get_logger
from aio_aws.s3_uri import S3URI
from aio_aws.s3_uri import S3Info
from aio_aws.utils import response_success

LOGGER = get_logger(__name__)


def s3_io_client(*args, **kwargs) -> BaseClient:
    """
    This creates a botocore.client.BaseClient that uses urllib3,
    which should be thread-safe, with a default connection pool.

    This uses a botocore.config.Config with 3 retries using a
    standard back off method

    :return: a botocore.client.BaseClient for s3
    """
    default_config = Config(
        retries={"max_attempts": 3, "mode": "standard"}, max_pool_connections=2
    )
    if "config" in kwargs:
        config = kwargs.pop("config")
    else:
        config = default_config
    return boto3.client("s3", *args, config=config, **kwargs)


def s3_file_info(s3_uri: Union[S3URI, str], s3_client: BaseClient = None) -> S3Info:
    """
    Collect data from an S3 HEAD request for an S3URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: an S3Info object with HEAD data on success;
        on failure the S3Info object has no HEAD data
    """
    if s3_client is None:
        s3_client = s3_io_client()

    if isinstance(s3_uri, str):
        s3_uri = S3URI(s3_uri)

    s3_info = S3Info(s3_uri=s3_uri)
    try:
        s3_head = s3_client.head_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
        if response_success(s3_head):
            # LastModified is a datetime.datetime
            s3_info.last_modified = s3_head["LastModified"]
            s3_info.s3_size = int(s3_head["ContentLength"])
            LOGGER.debug("Success S3URI info: %s", s3_uri)
    except botocore.exceptions.ClientError as err:
        LOGGER.debug("Failed S3URI info: %s", s3_uri)
        LOGGER.debug(err)
    return s3_info


def s3_files_info(
    s3_uris: Iterable[Union[S3URI, str]], s3_client: BaseClient = None
) -> List[S3Info]:
    """
    Collect data from an S3 HEAD request for an S3URI

    :param s3_uris: an iterable of fully qualified S3 URI for s3 objects
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: an S3Info object with HEAD data for each s3 object that exists;
        on failure the S3Info object has no HEAD data
    """

    if s3_client is None:
        config = Config(
            retries={"max_attempts": 3, "mode": "standard"}, max_pool_connections=20
        )
        s3_client = s3_io_client(config=config)

    s3_files = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        s3_futures = {}
        for s3_uri in s3_uris:
            s3_uri = S3URI(str(s3_uri))
            s3_future = executor.submit(s3_file_info, s3_uri, s3_client)
            s3_futures[s3_future] = s3_uri
        for future in concurrent.futures.as_completed(s3_futures):
            try:
                s3_info = future.result()
                s3_files.append(s3_info)
            except Exception as err:
                LOGGER.error(err)

    return s3_files


def s3_file_wait(
    s3_uri: Union[S3URI, str],
    delay: float = 2,
    max_attempts: int = 30,
    s3_client: BaseClient = None,
) -> Optional[S3URI]:
    """
    Wait for s3 object to exist.

    :param s3_uri: s3 object URI
    :param delay: time between polling the object existence
    :param max_attempts: number of times to poll object existence
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: the S3URI, if it exists
    :raises FileNotFoundError: if it does not exist
    :raises botocore.exceptions.WaiterError: for waiting errors or
        when an object does not exist
    :raises botocore.exceptions.ClientError: other client errors
    """
    if s3_client is None:
        s3_client = s3_io_client()

    s3_uri = S3URI(s3_uri)
    try:
        exists_waiter = s3_client.get_waiter("object_exists")
        exists_waiter.wait(
            Bucket=s3_uri.bucket,
            Key=s3_uri.key,
            WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts},
        )
        head = s3_client.head_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
        if response_success(head):
            LOGGER.info("Found file: %s", s3_uri)
            return s3_uri
        else:
            error = f"Missing file: {s3_uri}"
            LOGGER.error(error)
            raise FileNotFoundError(error)
    except botocore.exceptions.WaiterError as err:
        LOGGER.error("Failed S3 waiter for: %s", s3_uri)
        LOGGER.error(err)
        raise err
    except botocore.exceptions.ClientError as err:
        LOGGER.error("Failed S3 waiter for: %s", s3_uri)
        LOGGER.error(err)
        raise err


def get_s3_content(s3_uri: str, *args, s3_client: BaseClient = None, **kwargs):
    """
    Read s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: the data from the s3 object
    """
    if s3_client is None:
        s3_client = s3_io_client(*args, **kwargs)
    try:
        s3_uri = S3URI(s3_uri)
        LOGGER.info("Read s3-uri: %s", s3_uri)
        content_object = s3_client.get_object(Bucket=s3_uri.bucket, Key=s3_uri.key)
        file_content = content_object["Body"].read().decode("utf-8")
        return file_content
    except botocore.exceptions.ClientError as err:
        LOGGER.error("Failed S3 GetObject: %s", s3_uri)
        LOGGER.error(err)


def put_s3_content(
    data_file: str, s3_uri: str, s3_client: BaseClient = None
) -> Optional[str]:
    """
    Write a file to s3 URI

    :param data_file: a data file
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: the s3 URI on success
    """
    if s3_client is None:
        s3_client = s3_io_client()

    s3_uri = S3URI(s3_uri)

    try:
        with open(data_file, "rb") as fd:
            response = s3_client.put_object(
                Bucket=s3_uri.bucket, Key=s3_uri.key, Body=fd
            )
            success = response_success(response)
            if success:
                # Use a boto3 waiter to confirm it worked
                exists_waiter = s3_client.get_waiter("object_exists")
                exists_waiter.wait(
                    Bucket=s3_uri.bucket,
                    Key=s3_uri.key,
                    WaiterConfig={"Delay": 0.25, "MaxAttempts": 8},
                )
                return str(s3_uri)
    except botocore.exceptions.ClientError as err:
        LOGGER.error("Failed S3 PutObject to: %s", s3_uri)
        LOGGER.error(err)


def json_s3_dump(
    json_data: Any, s3_uri: str, s3_client: BaseClient = None
) -> Optional[str]:
    """
    Write JSON to s3 URI

    :param json_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: an optional botocore.client.BaseClient for s3
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

        s3_obj = put_s3_content(
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


def json_s3_load(s3_uri: str, *args, s3_client: BaseClient = None, **kwargs) -> Any:
    """
    Load JSON from s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: data from the json load
    """
    file_content = get_s3_content(s3_uri, *args, s3_client=s3_client, **kwargs)
    json_data = json.loads(file_content)
    return json_data


def geojson_s3_load(s3_uri: str, *args, s3_client: BaseClient = None, **kwargs) -> Dict:
    """
    Read GeoJSON data from s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: geojson data
    """
    return json_s3_load(s3_uri, *args, s3_client=s3_client, **kwargs)


def geojson_s3_dump(
    geojson_data: Any, s3_uri: str, s3_client: BaseClient = None
) -> Optional[str]:
    """
    Write GeoJSON to s3 URI

    :param geojson_data: an object to json.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: the s3 URI on success
    """
    return json_s3_dump(json_data=geojson_data, s3_uri=s3_uri, s3_client=s3_client)


def geojsons_s3_load(
    s3_uri: str, *args, s3_client: BaseClient = None, **kwargs
) -> List[Dict]:
    """
    Read GeoJSON Text Sequence data from s3 object

    :param s3_uri: a fully qualified S3 URI for the s3 object to read
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: geojson features

    .. seealso::

        - https://tools.ietf.org/html/rfc8142

    """
    file_content = get_s3_content(s3_uri, *args, s3_client=s3_client, **kwargs)
    geojsons = file_content.splitlines()
    # some geojsons lines could be empty
    features = []
    while geojsons:
        feature = geojsons.pop(0).strip()
        if feature:
            features.append(json.loads(feature))
    return features


def geojsons_s3_dump(
    geojson_features: List[Dict], s3uri: str, s3_client: BaseClient = None
) -> Optional[str]:
    """
    Write GeoJSON Text Sequence files to s3 URI

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
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: the s3 URI on success
    """
    if s3_client is None:
        s3_client = s3_io_client()

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

        s3_obj = put_s3_content(
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


def yaml_s3_dump(
    yaml_data: Any, s3_uri: str, s3_client: BaseClient = None
) -> Optional[str]:
    """
    Write YAML to s3 URI

    :param yaml_data: an object to yaml.dump
    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: an optional botocore.client.BaseClient for s3
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

        s3_obj = put_s3_content(
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


def yaml_s3_load(s3_uri: str, *args, s3_client: BaseClient = None, **kwargs) -> Any:
    """
    Load YAML from s3 URI

    :param s3_uri: a fully qualified S3 URI for the s3 object to write
    :param s3_client: an optional botocore.client.BaseClient for s3
    :return: data from the yaml load
    """
    file_content = get_s3_content(s3_uri, *args, s3_client=s3_client, **kwargs)
    yaml_data = yaml.safe_load(file_content)
    return yaml_data


class JsonBaseModel(BaseModel):
    def json_dict(self) -> Dict:
        return json.loads(self.json(), parse_float=lambda x: round(float(x), 6))

    def json_dumps(self) -> str:
        """Support JSON serialization methods"""
        return json.dumps(self.json_dict())

    @classmethod
    def json_loads(cls, json_str: str):
        """Support JSON serialization methods"""
        json_dict = json.loads(json_str)
        return cls.parse_obj(json_dict)

    def json_dump(self, json_file: Union[str, Path]) -> Optional[Path]:
        """Dump model dict to JSON file"""
        if isinstance(json_file, str):
            json_file = Path(json_file)
        if not json_file.parent.exists():
            json_file.parent.mkdir(parents=True, exist_ok=True)
        with open(json_file, "w") as json_fd:
            json.dump(self.json_dict(), json_fd)
        if json_file.exists() and json_file.stat().st_size > 0:
            LOGGER.info("Saved file: %s", str(json_file))
            return json_file

    @classmethod
    def json_load(cls, json_file: Union[str, Path]):
        """Load model from JSON file"""
        json_file = Path(json_file)
        if not json_file.exists():
            raise ValueError("File does not exist: %s", json_file)
        with open(json_file, "r") as config_fd:
            json_dict = json.load(config_fd)
            return cls.parse_obj(json_dict)

    def json_s3_dump(
        self, s3_uri: Union[S3URI, str], *args, **kwargs
    ) -> Optional[S3URI]:
        """Dump model dict to JSON file at S3 URI"""
        s3_uri = S3URI(str(s3_uri))
        if json_s3_dump(self.json_dict(), str(s3_uri), *args, **kwargs):
            return s3_uri

    @classmethod
    def json_s3_load(cls, s3_uri: Union[S3URI, str], *args, **kwargs):
        """Load model from JSON file at S3 URI"""
        json_dict = json_s3_load(s3_uri, *args, **kwargs)
        return cls.parse_obj(json_dict)


class YamlBaseModel(JsonBaseModel):
    @classmethod
    def load(cls, config_file: Union[Path, S3URI, str]):
        if isinstance(config_file, S3URI):
            return cls.load_s3(config_file)
        if isinstance(config_file, Path):
            config_file = str(config_file)
        if config_file.startswith("s3://"):
            return cls.load_s3(config_file)
        if config_file.endswith(".yaml") or config_file.endswith(".yml"):
            return cls.yaml_load(config_file)
        elif config_file.endswith(".json"):
            return cls.json_load(config_file)
        else:
            raise ValueError("Unknown file type, use .yaml, .yml or .json")

    @classmethod
    def load_s3(cls, s3_uri: Union[S3URI, str]):
        if isinstance(s3_uri, str):
            s3_uri = S3URI(s3_uri)
        if not isinstance(s3_uri, S3URI):
            raise ValueError("S3URI file must be a valid S3URI or str(S3URI)")
        if s3_uri.s3_uri.endswith(".yaml") or s3_uri.s3_uri.endswith(".yml"):
            return cls.yaml_s3_load(s3_uri)
        elif s3_uri.s3_uri.endswith(".json"):
            return cls.json_s3_load(s3_uri)
        else:
            raise ValueError("Unknown file type, use .yaml, .yml or .json")

    def yaml_dump(self, yaml_file: Union[str, Path]) -> Optional[Path]:
        """Dump model dict to YAML file"""
        if isinstance(yaml_file, str):
            yaml_file = Path(yaml_file)
        if not yaml_file.parent.exists():
            yaml_file.parent.mkdir(parents=True, exist_ok=True)
        with open(yaml_file, "w") as yaml_fd:
            yaml.dump(self.json_dict(), yaml_fd)
        if yaml_file.exists() and yaml_file.stat().st_size > 0:
            LOGGER.info("Saved file: %s", str(yaml_file))
            return yaml_file

    @classmethod
    def yaml_load(cls, yaml_file: Union[str, Path]):
        """Load model from YAML file"""
        yaml_file = Path(yaml_file)
        if not yaml_file.exists():
            raise ValueError("File does not exist: %s", yaml_file)
        with open(yaml_file, "r") as config_fd:
            yaml_settings = yaml.load(config_fd, Loader=yaml.SafeLoader)
            return cls.parse_obj(yaml_settings)

    def yaml_s3_dump(self, s3_uri: Union[S3URI, str]) -> Optional[S3URI]:
        """Dump model dict to YAML file at S3 URI"""
        s3_uri = S3URI(str(s3_uri))
        yaml_settings = self.json_dict()
        if yaml_s3_dump(yaml_settings, str(s3_uri)):
            return s3_uri

    @classmethod
    def yaml_s3_load(cls, s3_uri: Union[S3URI, str]):
        """Load model from YAML file at S3 URI"""
        yaml_dict = yaml_s3_load(s3_uri)
        return cls.parse_obj(yaml_dict)
