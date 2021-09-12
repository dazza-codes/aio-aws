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

from aio_aws.uuid_utils import get_hex_uuid
from aio_aws.uuid_utils import get_hex_uuids
from aio_aws.uuid_utils import get_uuid
from aio_aws.uuid_utils import get_uuids
from aio_aws.uuid_utils import valid_hex_uuid4
from aio_aws.uuid_utils import valid_uuid4


def test_get_uuid():
    uuid = get_uuid()
    assert valid_uuid4(uuid)


def test_get_hex_uuid():
    hex_uuid = get_hex_uuid()
    assert valid_hex_uuid4(hex_uuid)


def test_get_uuids():
    uuids = get_uuids(10)
    assert all([valid_uuid4(uuid) for uuid in uuids])


def test_get_hex_uuids():
    hex_uuids = get_hex_uuids(10)
    assert all([valid_hex_uuid4(uuid) for uuid in hex_uuids])
