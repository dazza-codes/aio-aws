import asyncio
import tempfile
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict

import pytest

from aio_aws.aio_aws_config import aio_aws_client
from aio_aws.aio_aws_config import aio_aws_default_config
from aio_aws.s3_aio import geojson_s3_dump
from aio_aws.s3_aio import geojson_s3_load
from aio_aws.s3_aio import geojsons_dump
from aio_aws.s3_aio import geojsons_s3_dump
from aio_aws.s3_aio import geojsons_s3_load
from aio_aws.s3_aio import get_s3_content
from aio_aws.s3_aio import json_dump
from aio_aws.s3_aio import json_s3_dump
from aio_aws.s3_aio import json_s3_load
from aio_aws.s3_aio import run_s3_load_files
from aio_aws.s3_aio import s3_file_info
from aio_aws.s3_aio import s3_files_info
from aio_aws.s3_aio import s3_load_files
from aio_aws.s3_aio import yaml_dump
from aio_aws.s3_aio import yaml_s3_dump
from aio_aws.s3_aio import yaml_s3_load
from aio_aws.s3_uri import S3Info
from aio_aws.s3_uri import S3URI


@pytest.mark.asyncio
async def test_s3_aio_file_info(
    aio_aws_s3_client, aio_s3_object_uri, aio_s3_object_text
):
    s3_uri: str = aio_s3_object_uri
    s3_text: str = aio_s3_object_text
    s3_info = await s3_file_info(s3_uri, s3_client=aio_aws_s3_client)
    assert isinstance(s3_info, S3Info)
    assert s3_info.s3_uri == S3URI(s3_uri)
    assert s3_info.s3_size == len(s3_text)
    assert isinstance(s3_info.last_modified, datetime)
    s3_dict = s3_info.dict
    assert isinstance(s3_dict, Dict)
    assert s3_dict["s3_uri"] == s3_uri
    assert s3_dict["s3_size"] == len(s3_text)
    # last-modified is an iso8601 string
    assert isinstance(s3_dict["last_modified"], str)
    last_modified = datetime.fromisoformat(s3_dict["last_modified"])
    assert isinstance(last_modified, datetime)


@pytest.mark.asyncio
async def test_s3_aio_files_info(
    aio_aws_s3_client, aio_s3_object_uri, aio_s3_object_text
):
    s3_uri: str = aio_s3_object_uri
    s3_text: str = aio_s3_object_text
    s3_files = await s3_files_info([s3_uri], s3_client=aio_aws_s3_client)
    for s3_info in s3_files:
        assert isinstance(s3_info, S3Info)
        assert s3_info.s3_uri == S3URI(s3_uri)
        assert s3_info.s3_size == len(s3_text)
        assert isinstance(s3_info.last_modified, datetime)


@pytest.mark.asyncio
async def test_s3_aio_get_content(
    aio_aws_s3_client, aio_s3_object_uri, aio_s3_object_text
):
    s3_uri: str = aio_s3_object_uri
    s3_text: str = aio_s3_object_text
    object_data = await get_s3_content(s3_uri, s3_client=aio_aws_s3_client)
    assert object_data == s3_text


@pytest.mark.asyncio
async def test_s3_aio_geojson(
    geojson_feature_collection,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uri = S3URI(f"s3://{aio_s3_bucket}/tmp.geojson")
    result = await geojson_s3_dump(
        geojson_data=geojson_feature_collection,
        s3_uri=str(s3_uri),
        s3_client=aio_aws_s3_client,
    )
    assert result == s3_uri.s3_uri
    data = await geojson_s3_load(s3_uri=str(s3_uri), s3_client=aio_aws_s3_client)
    assert data == geojson_feature_collection


@pytest.mark.asyncio
async def test_s3_aio_geojsons(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uri = S3URI(f"s3://{aio_s3_bucket}/tmp.geojsons")
    result = await geojsons_s3_dump(
        geojson_features, str(s3_uri), s3_client=aio_aws_s3_client
    )
    assert result == s3_uri.s3_uri
    data = await geojsons_s3_load(str(s3_uri), s3_client=aio_aws_s3_client)
    assert data == geojson_features


@pytest.mark.asyncio
async def test_aio_json_dump(geojson_features):
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_path = Path(tmp_file.name)
        dump_path = await json_dump(geojson_features, tmp_path)
        assert dump_path == tmp_path
        assert tmp_path.exists()


@pytest.mark.asyncio
async def test_aio_geojsons_dump(geojson_features):
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_path = Path(tmp_file.name)
        dump_path = await geojsons_dump(geojson_features, tmp_path)
        assert dump_path == tmp_path
        assert tmp_path.exists()


@pytest.mark.asyncio
async def test_aio_yaml_dump(geojson_features):
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_path = Path(tmp_file.name)
        dump_path = await yaml_dump(geojson_features, tmp_path)
        assert dump_path == tmp_path
        assert tmp_path.exists()


@pytest.mark.asyncio
async def test_s3_aio_json_io(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uri = S3URI(f"s3://{aio_s3_bucket}/tmp.json")
    result = await json_s3_dump(
        geojson_features, str(s3_uri), s3_client=aio_aws_s3_client
    )
    assert result == s3_uri.s3_uri
    data = await json_s3_load(str(s3_uri), s3_client=aio_aws_s3_client)
    assert data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_yaml_io(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uri = S3URI(f"s3://{aio_s3_bucket}/tmp.json")
    result = await yaml_s3_dump(
        geojson_features, str(s3_uri), s3_client=aio_aws_s3_client
    )
    assert result == s3_uri.s3_uri
    data = await yaml_s3_load(str(s3_uri), s3_client=aio_aws_s3_client)
    assert data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_json_files(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.json").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await json_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    data = await s3_load_files(s3_uris, s3_client=aio_aws_s3_client)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_json_files_without_test_s3_client(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.json").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await json_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    # This must create a new aio-s3-client that must use the same
    # mock s3 endpoint used by aio_aws_s3_client above
    data = await s3_load_files(s3_uris, endpoint_url=aio_aws_s3_client._endpoint.host)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features


@pytest.mark.asyncio
async def test_run_s3_aio_json_files(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.json").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await json_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    # This will have to run it's own event loop on a new thread to avoid conflict
    # with the event loop already running for pytest-asyncio; and
    # this must create a new aio-s3-client that must use the same
    # mock s3 endpoint used by aio_aws_s3_client above
    data = run_s3_load_files(s3_uris, endpoint_url=aio_aws_s3_client._endpoint.host)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_geojson_files(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.geojson").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await geojson_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    data = await s3_load_files(s3_uris, s3_client=aio_aws_s3_client)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_geojsons_files(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.geojsons").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await geojsons_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    data = await s3_load_files(s3_uris, s3_client=aio_aws_s3_client)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features


@pytest.mark.asyncio
async def test_s3_aio_yaml_files(
    geojson_features,
    aio_aws_s3_client,
    aio_s3_bucket,
):
    # Since JSON is a subset of YAML, using GeoJSON features should work
    s3_uris = [
        S3URI(f"s3://{aio_s3_bucket}/tmp_{i:03d}.yaml").s3_uri for i in range(10)
    ]
    for s3_uri in s3_uris:
        result = await yaml_s3_dump(
            geojson_features, s3_uri, s3_client=aio_aws_s3_client
        )
        assert result == s3_uri

    data = await s3_load_files(s3_uris, s3_client=aio_aws_s3_client)
    assert sorted(data.keys()) == s3_uris
    for s3_uri, s3_data in data.items():
        assert s3_data == geojson_features
