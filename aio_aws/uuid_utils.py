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

import concurrent.futures
import re
from typing import Iterator
from typing import List
from uuid import uuid4

# regex patterns are from
# https://stackoverflow.com/questions/11384589/what-is-the-correct-regex-for-matching-values-generated-by-uuid-uuid4-hex

RE_UUID4 = re.compile(
    r"^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z", re.I
)

RE_UUID4_HEX = re.compile(r"[0-9a-f]{32}\Z", re.I)


def get_uuid(_: int = None) -> str:
    """
    Get a UUID as a string value
    """
    return str(uuid4())


def get_hex_uuid(_: int = None) -> str:
    """
    Get a UUID as a hex string value
    """
    # Ensure that a uuid4().hex returns a validated UUID.
    return uuid4().hex


def valid_uuid4(uuid):
    match = RE_UUID4.match(uuid)
    return bool(match)


def valid_hex_uuid4(uuid):
    match = RE_UUID4_HEX.match(uuid)
    return bool(match)


def get_uuids(n_uuids: int) -> List[str]:
    """
    Get a list of UUIDs
    """
    return list(generate_uuids(n_uuids))


def generate_uuids(n_uuids: int) -> Iterator[str]:
    """
    Generate an iterator of UUIDs
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        uuids = executor.map(get_uuid, range(n_uuids))
        return uuids


def get_hex_uuids(n_uuids: int) -> List[str]:
    """
    Get a list of UUIDs
    """
    return list(generate_hex_uuids(n_uuids))


def generate_hex_uuids(n_uuids: int) -> Iterator[str]:
    """
    Generate an iterator of UUIDs
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        uuids = executor.map(get_hex_uuid, range(n_uuids))
        return uuids
