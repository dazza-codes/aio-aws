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
from datetime import datetime
from datetime import timezone
from email.utils import format_datetime
from email.utils import formatdate
from email.utils import parsedate_to_datetime
from math import floor
from typing import Optional

from botocore.exceptions import ClientError

from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)


def handle_head_error_code(error: ClientError, item: str = None) -> Optional[bool]:
    err_code = error.response.get("Error", {}).get("Code")
    if err_code == "401":
        LOGGER.debug("GET HEAD 401: invalid credentials: %s", item)
        return False
    if err_code == "403":
        LOGGER.debug("GET HEAD 403: permission denied: %s", item)
        return False
    if err_code == "404":
        LOGGER.debug("GET HEAD 404: object missing: %s", item)
        return False
    return None


def response_code(response) -> int:
    return int(response.get("ResponseMetadata", {}).get("HTTPStatusCode"))


def response_success(response) -> bool:
    code = response_code(response)
    if code:
        return 200 <= code < 300
    else:
        return False


def utc_now() -> datetime:
    """UTC datetime - tz aware"""
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def utc_timestamp() -> float:
    """Unix timestamp - time since the epoch in seconds"""
    return utc_now().timestamp()


def utc_unix_milliseconds() -> int:
    """Unix timestamp - time since the epoch in milliseconds"""
    return floor(utc_timestamp() * 1e3)


def datetime_to_unix_milliseconds(dt: datetime) -> int:
    """datetime to unix timestamp - time since the epoch in milliseconds"""
    utc_dt = dt.astimezone(timezone.utc)
    return floor(utc_dt.timestamp() * 1e3)


def http_date_to_datetime(http_date: str) -> datetime:
    """
    Parse a HTTP date string into a datetime.

    :param http_date: e.g. "Mon, 23 Mar 2020 15:29:33 GMT"
    :return: a datetime
    """
    return parsedate_to_datetime(http_date)


def http_date_to_timestamp(http_date: str) -> float:
    """
    Parse a HTTP date string into a timestamp as
    seconds since the epoch.

    :param http_date: e.g. "Mon, 23 Mar 2020 15:29:33 GMT"
    :return: a timestamp (seconds since the epoch)
    """
    return parsedate_to_datetime(http_date).timestamp()


def datetime_to_http_date(dt: datetime) -> str:
    """
    Parse a datetime into an HTTP date string (using GMT).

    :param dt: a datetime
    :return: an HTTP date string, e.g. "Mon, 23 Mar 2020 15:29:33 GMT"
    """
    return format_datetime(dt, usegmt=True)


def timestamp_to_http_date(ts: float) -> str:
    """
    Parse a timestamp (seconds since the epoch)
    into an HTTP date string (using GMT).

    :param ts: a timestamp (seconds since the epoch)
    :return: an HTTP date string, e.g. "Mon, 23 Mar 2020 15:29:33 GMT"
    """
    return formatdate(ts, usegmt=True)


def datetime_from_unix_milliseconds(msec: float) -> datetime:
    return datetime.utcfromtimestamp(msec / 1e3).replace(tzinfo=timezone.utc)
