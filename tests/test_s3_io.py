import tempfile
from pathlib import Path
from typing import Dict
from typing import List

import boto3
import pytest

from aio_aws.s3_io import geojson_s3_dump
from aio_aws.s3_io import geojson_s3_load
from aio_aws.s3_io import geojsons_dump
from aio_aws.s3_io import geojsons_s3_dump
from aio_aws.s3_io import geojsons_s3_load
from aio_aws.s3_io import get_s3_content
from aio_aws.s3_io import json_s3_dump
from aio_aws.s3_io import json_s3_load
from aio_aws.s3_io import yaml_s3_dump
from aio_aws.s3_io import yaml_s3_load
from aio_aws.s3_uri import S3URI
from tests.aws_fixtures import assert_bucket_200
from tests.aws_fixtures import assert_object_200


@pytest.fixture
def geojson_feature_collection() -> Dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [170.50251, -45.874003]},
                "properties": {"cityName": "Dunedin", "countryName": "New Zealand"},
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        -74.013402,
                        40.705619,
                    ],
                },
                "properties": {"cityName": "New York", "countryName": "USA"},
            },
        ],
    }


@pytest.fixture
def geojson_features(geojson_feature_collection) -> List[Dict]:
    return geojson_feature_collection["features"]


@pytest.fixture
def geojson_feature(geojson_features) -> Dict:
    return geojson_features[0]


def test_get_s3_content(aws_s3_client, s3_uri_object, s3_object_text, mocker):
    assert_bucket_200(s3_uri_object.bucket, aws_s3_client)
    assert_object_200(s3_uri_object.bucket, s3_uri_object.key, aws_s3_client)
    spy_client = mocker.spy(boto3, "client")
    spy_resource = mocker.spy(boto3, "resource")
    object_data = get_s3_content(s3_uri_object.s3_uri)
    assert object_data == s3_object_text
    # the s3 client is used once to get the s3 object data
    assert spy_client.call_count == 1
    assert spy_resource.call_count == 0


def test_geojson_io(geojson_feature_collection, aws_s3_client, s3_bucket, mocker):
    assert_bucket_200(s3_bucket, aws_s3_client)
    spy_client = mocker.spy(boto3, "client")
    spy_resource = mocker.spy(boto3, "resource")
    s3_uri = S3URI(f"s3://{s3_bucket}/tmp.geojson")
    result = geojson_s3_dump(geojson_feature_collection, s3_uri.s3_uri)
    assert result == s3_uri.s3_uri
    # the s3 client is used once to upload the s3 object data
    assert spy_client.call_count == 1
    assert spy_resource.call_count == 0
    assert_object_200(bucket=s3_bucket, key=s3_uri.key, s3_client=aws_s3_client)
    data = geojson_s3_load(s3_uri.s3_uri)
    assert data == geojson_feature_collection
    # the s3 client is used to read the s3 object data
    assert spy_client.call_count == 2
    assert spy_resource.call_count == 0


def test_geojsons_io(geojson_features, aws_s3_client, s3_bucket, mocker):
    assert_bucket_200(s3_bucket, aws_s3_client)
    spy_client = mocker.spy(boto3, "client")
    spy_resource = mocker.spy(boto3, "resource")
    s3_uri = S3URI(f"s3://{s3_bucket}/tmp.geojsons")
    result = geojsons_s3_dump(geojson_features, s3_uri.s3_uri)
    assert result == s3_uri.s3_uri
    # the s3 client is used once to upload the s3 object data
    assert spy_client.call_count == 1
    assert spy_resource.call_count == 0
    assert_object_200(bucket=s3_bucket, key=s3_uri.key, s3_client=aws_s3_client)
    data = geojsons_s3_load(s3_uri.s3_uri)
    assert data == geojson_features
    # the s3 client is used to read the s3 object data
    assert spy_client.call_count == 2
    assert spy_resource.call_count == 0


def test_geojsons_dump(geojson_features):
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_path = Path(tmp_file.name)
        dump_path = geojsons_dump(geojson_features, tmp_path)
        assert dump_path == tmp_path
        assert tmp_path.exists()


def test_json_io(geojson_feature_collection, aws_s3_client, s3_bucket, mocker):
    assert_bucket_200(s3_bucket, aws_s3_client)
    spy_client = mocker.spy(boto3, "client")
    spy_resource = mocker.spy(boto3, "resource")
    s3_uri = S3URI(f"s3://{s3_bucket}/tmp.json")
    result = json_s3_dump(geojson_feature_collection, s3_uri.s3_uri)
    assert result == s3_uri.s3_uri
    # the s3 client is used once to upload the s3 object data
    assert spy_client.call_count == 1
    assert spy_resource.call_count == 0
    assert_object_200(bucket=s3_bucket, key=s3_uri.key, s3_client=aws_s3_client)
    data = json_s3_load(s3_uri.s3_uri)
    assert data == geojson_feature_collection
    # the s3 client is used to read the s3 object data
    assert spy_client.call_count == 2
    assert spy_resource.call_count == 0


def test_yaml_io(geojson_feature_collection, aws_s3_client, s3_bucket, mocker):
    # Since JSON is a subset of YAML, this should work for GeoJSON data
    assert_bucket_200(s3_bucket, aws_s3_client)
    spy_client = mocker.spy(boto3, "client")
    spy_resource = mocker.spy(boto3, "resource")
    s3_uri = S3URI(f"s3://{s3_bucket}/tmp.yaml")
    result = yaml_s3_dump(geojson_feature_collection, s3_uri.s3_uri)
    assert result == s3_uri.s3_uri
    # the s3 client is used once to upload the s3 object data
    assert spy_client.call_count == 1
    assert spy_resource.call_count == 0
    assert_object_200(bucket=s3_bucket, key=s3_uri.key, s3_client=aws_s3_client)
    data = yaml_s3_load(s3_uri.s3_uri)
    assert data == geojson_feature_collection
    # the s3 client is used to read the s3 object data
    assert spy_client.call_count == 2
    assert spy_resource.call_count == 0
