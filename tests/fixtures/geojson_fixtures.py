from typing import Dict
from typing import List

import pytest


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
