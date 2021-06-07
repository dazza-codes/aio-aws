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
