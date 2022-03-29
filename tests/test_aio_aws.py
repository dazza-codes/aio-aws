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
Test the aio_aws package
"""
from types import ModuleType

import aio_aws


def test_aio_aws_package():
    assert isinstance(aio_aws, ModuleType)


def test_aio_aws_version():
    assert aio_aws.version
    assert aio_aws.__version__
    assert aio_aws.VERSION
