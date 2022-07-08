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

"""
AioAWS logger
"""

import logging
import os
import sys
import time

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LOG_FORMAT = " | ".join(
    [
        "%(asctime)s.%(msecs)03dZ",
        "%(levelname)s",
        "%(name)s:%(funcName)s:%(lineno)d",
        "%(message)s",
    ]
)

LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"


def get_stderr_handler() -> logging.StreamHandler:
    logging.Formatter.converter = time.gmtime
    log_formatter = logging.Formatter(LOG_FORMAT, LOG_DATEFMT)
    handler = logging.StreamHandler(sys.stderr)
    handler.formatter = log_formatter
    return handler


def get_stdout_handler() -> logging.StreamHandler:
    logging.Formatter.converter = time.gmtime
    log_formatter = logging.Formatter(LOG_FORMAT, LOG_DATEFMT)
    handler = logging.StreamHandler(sys.stdout)
    handler.formatter = log_formatter
    return handler


def get_logger(
    name: str = "aio-aws",
    log_level: str = LOG_LEVEL,
    handler: logging.StreamHandler = None,
) -> logging.Logger:
    if handler is None:
        handler = get_stdout_handler()
    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(handler)
    logger.setLevel(log_level)
    logger.propagate = False
    return logger
