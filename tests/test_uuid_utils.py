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
