# Copyright 2020 Darren Weber
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
AioAWS test fixtures

"""

import pytest

from aio_aws.aio_aws_batch import AWSBatchDB


pytest_plugins = [
    "tests.aws_fixtures",
    "tests.aiomoto_fixtures",
]


@pytest.fixture
def test_jobs_db(tmp_path) -> AWSBatchDB:
    jobs_db_file = str(tmp_path / "test_batch_jobs_db.json")
    logs_db_file = str(tmp_path / "test_batch_logs_db.json")
    batch_jobs_db = AWSBatchDB(jobs_db_file=jobs_db_file, logs_db_file=logs_db_file)
    assert batch_jobs_db.jobs_db.all() == []
    assert batch_jobs_db.logs_db.all() == []
    yield batch_jobs_db
