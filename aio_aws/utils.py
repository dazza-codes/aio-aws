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

from typing import Optional

from botocore.exceptions import ClientError

from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)


def response_code(response) -> int:
    return int(response.get("ResponseMetadata", {}).get("HTTPStatusCode"))


def response_success(response) -> bool:
    code = response_code(response)
    if code:
        return 200 <= code < 300
    else:
        return False


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
