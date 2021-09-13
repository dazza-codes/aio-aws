#! /usr/bin/env python3
# pylint: disable=bad-continuation

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
AioAWS Batch Models
===================

Batch Job metadata models.

"""

import enum
from dataclasses import dataclass
from functools import total_ordering
from typing import Dict
from typing import List
from typing import Optional

from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)


@total_ordering
class AWSBatchJobStatuses(enum.Enum):
    SUBMITTED = 1
    PENDING = 2
    RUNNABLE = 3
    STARTING = 4
    RUNNING = 5
    SUCCEEDED = 6
    FAILED = 7

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


@dataclass
class AWSBatchJobDescription:
    jobName: str = None
    jobId: str = None
    jobQueue: str = None
    status: str = None
    attempts: List[Dict] = None
    statusReason: str = None
    createdAt: int = None
    startedAt: int = None
    stoppedAt: int = None
    dependsOn: List[str] = None
    jobDefinition: str = None
    parameters: Dict = None
    container: Dict = None
    timeout: Dict = None


@dataclass
class AWSBatchJob:
    """
    AWS Batch job

    Creating an AWSBatchJob instance does not run anything, it's simply
    a dataclass to retain and track job attributes.  There are no instance
    methods to run a job, the instances are passed to async coroutine functions.

    Replace 'command' with 'container_overrides' dict for more options;
    do not use 'command' together with 'container_overrides', or understand
    that the construction will update the `container_overrides['command']`
    when the `command is not None`.

    :param job_name: A job jobName (truncated to 128 characters).
    :param job_definition: A job job_definition.
    :param job_queue: A batch queue.
    :param command: A container command.
    :param depends_on: list of dictionaries like:

        .. code-block::

            [
              {'jobId': 'abc123', ['type': 'N_TO_N' | 'SEQUENTIAL'] },
            ]

        type is optional, used only for job arrays

    :param container_overrides: a dictionary of container overrides.
        Overrides include 'vcpus', 'memory', 'instanceType',
        'environment', and 'resourceRequirements'. If the `command`
        parameter is defined, it overrides the `container_overrides['command']`

    :param max_tries: an optional limit to the number of job retries, which
        can apply in the job-manager function to any job with a SPOT failure
        only and it applies regardless of the job-definition settings.

    .. seealso::
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    """

    STATUSES = [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]

    job_name: str
    job_queue: str
    job_definition: str
    command: List[str] = None
    depends_on: List[Dict] = None
    container_overrides: Dict = None
    job_id: Optional[str] = None
    status: Optional[str] = None
    job_tries: List[str] = None
    num_tries: int = 0
    max_tries: int = 4
    job_submission: Optional[Dict] = None
    job_description: Optional[Dict] = None
    logs: Optional[List[Dict]] = None

    def __post_init__(self):

        self.job_name = self.job_name[:128]

        if self.job_tries is None:
            self.job_tries = []

        if self.depends_on is None:
            self.depends_on = []

        if self.container_overrides is None:
            self.container_overrides = {}

        if self.command:
            self.container_overrides.update({"command": self.command})

    @property
    def params(self):
        """AWS Batch parameters for job submission"""
        return {
            "jobName": self.job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": self.job_definition,
            "containerOverrides": self.container_overrides,
            "dependsOn": self.depends_on,
        }

    @property
    def db_data(self) -> Dict:
        """AWS Batch job data for state machine persistence"""
        # The job.logs are NOT included here, by design.
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "job_queue": self.job_queue,
            "job_definition": self.job_definition,
            "job_submission": self.job_submission,
            "job_description": self.job_description,
            "container_overrides": self.container_overrides,
            "command": self.command,
            "depends_on": self.depends_on,
            "status": self.status,
            "job_tries": self.job_tries,
            "num_tries": self.num_tries,
            "max_tries": self.max_tries,
        }

    @property
    def db_logs_data(self) -> Optional[Dict]:
        """AWS Batch job with logs persistence"""
        if self.logs:
            data = self.db_data
            data["logs"] = self.logs
            return data

    def allow_submit_job(self) -> bool:
        """
        - jobs with an existing job.job_id should skip
        submission to avoid resubmission for the same job

        - jobs with too many tries cannot be resubmitted

        The :py:meth:`AWSBatchJob.reset()` can be applied
        in order to resubmit any job that is no allowed.

        :return: True if it is allowed
        """
        if self.job_id is None:
            if self.num_tries < self.max_tries:
                return True
            else:
                LOGGER.warning(
                    "AWS Batch job (%s) exceeds retries: %d of %d",
                    self.job_name,
                    self.num_tries,
                    self.max_tries,
                )
        return False

    def reset(self):
        """Clear the job_id and all related job data"""
        self.job_id = None
        self.job_description = None
        self.job_submission = None
        self.status = None
        self.logs = None

    @property
    def created(self) -> Optional[int]:
        if self.job_description:
            return self.job_description.get("createdAt")

    @property
    def started(self) -> Optional[int]:
        if self.job_description:
            return self.job_description.get("startedAt")

    @property
    def stopped(self) -> Optional[int]:
        if self.job_description:
            return self.job_description.get("stoppedAt")

    @property
    def elapsed(self) -> Optional[int]:
        created = self.created
        stopped = self.stopped
        if stopped and created:
            return stopped - created

    @property
    def runtime(self) -> Optional[int]:
        started = self.started
        stopped = self.stopped
        if started and stopped:
            return stopped - started

    @property
    def spinup(self) -> Optional[int]:
        created = self.created
        started = self.started
        if started and created:
            return started - created
