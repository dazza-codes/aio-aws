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
