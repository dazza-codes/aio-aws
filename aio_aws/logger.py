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

"""
AioAWS logger
"""

import logging
import os
import sys
import time

logging.Formatter.converter = time.gmtime

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

LOG_FORMAT = " | ".join(
    [
        "%(asctime)s.%(msecs)03dZ",
        "%(levelname)s",
        "%(name)s:%(funcName)s:%(lineno)d",
        "%(message)s",
    ]
)
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMATTER = logging.Formatter(LOG_FORMAT, LOG_DATEFMT)

HANDLER = logging.StreamHandler(sys.stdout)
HANDLER.formatter = LOG_FORMATTER


def get_logger(name: str = "aio-aws") -> logging.Logger:
    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(HANDLER)
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False
    return logger
