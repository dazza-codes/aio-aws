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
S3URI
-----
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Dict
from typing import Iterable
from typing import Pattern
from typing import Union

import boto3
import botocore
import botocore.exceptions

from aio_aws.logger import get_logger

LOGGER = get_logger(__name__)

BUCKET_REGEX = re.compile(r"^[a-z0-9][a-z0-9.-]{2,62}$")  # type: Pattern[str]


def bucket_validate(bucket_name: str) -> bool:
    """
    For technical details on S3 buckets, refer to

    - https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

    The following are the rules for naming S3 buckets in all AWS Regions:

    - Bucket names must follow DNS-compliant naming conventions. Amazon S3 no
      longer supports creating bucket names that contain uppercase letters or
      underscores. This ensures that each bucket can be addressed using virtual
      host style addressing, such as https://myawsbucket.s3.amazonaws.com.
    - Bucket names must be unique across all existing bucket names in Amazon S3.
    - Bucket names must comply with DNS naming conventions.
    - Bucket names must be at least 3 and no more than 63 characters long.
    - Bucket names must not contain uppercase characters or underscores.
    - Bucket names must start with a lowercase letter or number.
    - Bucket names must be a series of one or more labels. Adjacent labels are
      separated by a single period (.). Bucket names can contain lowercase letters,
      numbers, and hyphens. Each label must start and end with a lowercase letter
      or a number.
    - Bucket names must not be formatted as an IP address (e.g., 192.168.5.4).
    - When you use virtual hosted–style buckets with Secure Sockets Layer (SSL),
      the SSL wildcard certificate only matches buckets that don't contain periods.
      To work around this, use HTTP or write your own certificate verification
      logic. We recommend that you do not use periods (".") in bucket names when
      using virtual hosted-style buckets.

    :param bucket_name:
    :return: True if the bucket_name is valid
    :raises: ValueError if the bucket_name is not valid
    """
    if BUCKET_REGEX.match(bucket_name):
        return True
    raise ValueError(f"The bucket_name is invalid: {bucket_name}")


@dataclass(frozen=True)
class S3Object:
    """Just the bucket_name and key for an :code:`s3.ObjectSummary`.

    This simple dataclass should work around problems with
    :code:`Pickle` for an :code:`s3.ObjectSummary`.

    .. code-block:: python

        from aio_aws.s3_uri import S3Object

        # assume obj is an s3.ObjectSummary
        S3Object(bucket=obj.bucket_name, key=obj.key)

    """

    bucket: str
    key: str

    @property
    def s3_uri(self):
        """s3_uri: str for :code:`s3://{bucket}/{key}`"""
        path = PurePosixPath(self.bucket) / PurePosixPath(self.key)
        return f"s3://{path}"


class S3Paths:
    """
    S3 URI components
    """

    def __init__(self, s3_uri: str = "", bucket: str = "", key: str = ""):
        """
        This class is usually instantiated by py:meth:`S3Paths.parse_s3_uri`
        rather than directly instantiated.  If it is instantiated directly,
        it takes either an s3_uri (which takes precedence) or it takes
        both a bucket and a key.

        :param s3_uri: the URI in the form of :code:`s3://{bucket}/{key}`
        :param bucket: the bucket in :code:`s3://{bucket}/{key}`
        :param key: the key in :code:`s3://{bucket}/{key}`
        """

        if s3_uri:
            s3_paths = S3Paths.parse_s3_uri(s3_uri)
            self._bucket = s3_paths.bucket
            self._key = s3_paths.key
        elif bucket:
            if not S3Paths.is_valid_bucket(bucket):
                raise ValueError(f"The bucket is invalid: {bucket}")
            self._bucket = bucket
            self._key = key
        else:
            raise ValueError("Failed to provide s3_uri or a valid bucket and key")

        self._key_path = ""
        self._key_file = ""

        if self._key:
            key = PurePosixPath(self._key)

            if key.suffix:
                self._key_path = str(key.parent)
                self._key_file = key.stem + key.suffix
            else:
                self._key_path = str(key)

    @property
    def protocol(self) -> str:
        return "s3://"

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def key(self) -> str:
        return self._key

    @property
    def key_path(self) -> str:
        return self._key_path

    @property
    def key_file(self) -> str:
        return self._key_file

    @property
    def s3_uri(self):
        """s3_uri: str for :code:`s3://{bucket}/{key}`"""
        path = PurePosixPath(self.bucket) / PurePosixPath(self.key)
        return f"s3://{path}"

    def as_uri(self, protocol: str = "s3://") -> str:
        """a URI for :code:`{protocol}{bucket}/{key}`"""
        path = PurePosixPath(self.bucket) / PurePosixPath(self.key)
        return f"{protocol}{path}"

    @staticmethod
    def is_valid_bucket(bucket: str) -> bool:
        if BUCKET_REGEX.match(bucket):
            return True
        return False

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> "S3Paths":
        """Parse an S3 URI into components; the delimiter must be '/'

        :param s3_uri:
        :return: :code:`S3Paths(bucket, key, key_path, key_file)`
        """
        s3_uri = str(s3_uri).strip()
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"The s3_uri is invalid: {s3_uri}")

        paths = s3_uri.replace("s3://", "").split("/")
        if not any(paths):
            raise ValueError(f"The s3_uri is invalid: {s3_uri}")

        bucket = paths.pop(0)
        if not BUCKET_REGEX.match(bucket):
            raise ValueError(f"The s3_uri is invalid: {s3_uri}")

        key = ""
        if paths:
            key = str(PurePosixPath(*paths))

        return S3Paths(bucket=bucket, key=key)

    def glob_pattern(self, glob_pattern: str = "**/*"):
        """
        Construct a glob pattern relative to the key_path.  The final
        glob pattern is: :code:`str(PurePosixPath(self.key_path) / glob_pattern)`

        :param glob_pattern: str defaults to :code:`'**/*'`
        :return: str
        """
        return str(PurePosixPath(self.key_path) / glob_pattern)

    def glob_file_pattern(self):
        """
        Construct a glob pattern relative to the key_path.  The final
        glob pattern is: :code:`{key_path}/**/{file_stem}*.*`

        :return: str
        """
        file_stem = PurePosixPath(self.key).stem
        return f"{self.key_path}/**/{file_stem}*.*"

    def __eq__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri == other.s3_uri

    def __ne__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri != other.s3_uri

    def __lt__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri < other.s3_uri

    def __le__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri <= other.s3_uri

    def __ge__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri >= other.s3_uri

    def __gt__(self, other):
        if not hasattr(other, "s3_uri"):
            return NotImplemented
        return self.s3_uri > other.s3_uri

    def __hash__(self):
        return hash(self.s3_uri)

    def __repr__(self):
        return "%s:%r" % (self.__class__, self.s3_uri)

    def __str__(self):
        return self.s3_uri


class S3URI(S3Paths):
    """S3 URI representation and parsing.

    At present, this class is designed to represent one s3 object.
    Although it has some utilities for working with general bucket and key
    paths, it is designed to work with one s3 object.

    For technical details on S3 Objects, refer to

    - https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    - https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

    The following are the rules for naming S3 buckets in all AWS Regions:

    - Bucket names must follow DNS-compliant naming conventions. Amazon S3 no
      longer supports creating bucket names that contain uppercase letters or
      underscores. This ensures that each bucket can be addressed using virtual
      host style addressing, such as https://myawsbucket.s3.amazonaws.com.
    - Bucket names must be unique across all existing bucket names in Amazon S3.
    - Bucket names must comply with DNS naming conventions.
    - Bucket names must be at least 3 and no more than 63 characters long.
    - Bucket names must not contain uppercase characters or underscores.
    - Bucket names must start with a lowercase letter or number.
    - Bucket names must be a series of one or more labels. Adjacent labels are
      separated by a single period (.). Bucket names can contain lowercase letters,
      numbers, and hyphens. Each label must start and end with a lowercase letter
      or a number.
    - Bucket names must not be formatted as an IP address (e.g., 192.168.5.4).
    - When you use virtual hosted–style buckets with Secure Sockets Layer (SSL),
      the SSL wildcard certificate only matches buckets that don't contain periods.
      To work around this, use HTTP or write your own certificate verification
      logic. We recommend that you do not use periods (".") in bucket names when
      using virtual hosted-style buckets.

    The following are the rules for naming S3 keys:

    - The name for a key is a sequence of Unicode characters whose UTF-8
      encoding is at most 1024 bytes long.

    :param bucket: str for bucket in :code:`s3://{bucket}/{key}`
    :param key: str for key in :code:`s3://{bucket}/{key}`
    """

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> "S3URI":
        s3_paths = S3Paths.parse_s3_uri(s3_uri)
        return S3URI(bucket=s3_paths.bucket, key=s3_paths.key)

    def s3_bucket(self) -> "s3.Bucket":
        """A :code:`boto3.resource('s3').Bucket('bucket')`"""
        return boto3.resource("s3").Bucket(self.bucket)

    def s3_derivatives(self) -> Iterable:
        """Use :code:`s3_objects(glob_file_pattern)` to find all the derivatives
        with matching file names (key_file*.*) below the path of this file.

        :return: Iterable[s3.ObjectSummary]
        """
        pattern = self.glob_file_pattern()
        return self.s3_objects(glob_pattern=pattern)

    def s3_exists(self) -> bool:
        """Check the s3 file summary to confirm a file exists.
        :return: bool True if the s3 file exists
        """
        if self.s3_last_modified():
            return True
        return False

    def s3_head_request(self) -> dict:
        """Issue an HTTP HEAD request to get the s3 URI summary data.

        :return: dict of HEAD response data or an empty dict if it fails.
        """
        try:
            # check that an s3 key exists with a head request
            client = boto3.client("s3")
            return client.head_object(Bucket=self.bucket, Key=self.key)

        except botocore.exceptions.ClientError as err:
            self._log_client_errors(err)

        return {}

    def s3_last_modified(self) -> Union[datetime, None]:
        try:
            return self.s3_object_summary().last_modified

        except botocore.exceptions.ClientError as err:
            self._log_client_errors(err)

        return None

    def s3_object_summary(self) -> "s3.ObjectSummary":
        """Create an s3.ObjectSummary for this S3URI;
        this does not check if the object exists.

        :return: s3.ObjectSummary
        """
        s3 = boto3.resource("s3")
        return s3.ObjectSummary(self.bucket, self.key)

    def s3_objects(self, glob_pattern: str = None, prefix: str = None) -> Iterable:
        """
        Search all the s3 objects at or below the s3_uri.bucket, using an
        optional filter prefix and/or a glob pattern (defaults to everything).

        Using the s3_uri.key_path as a filter prefix is most often critical
        to limit the number of bucket objects to process. The addition of a
        glob pattern is applied to the results of all objects found; the glob
        pattern match is applied to each s3.ObjectSummary.key.

        :param prefix: str to specify a Prefix filter based on glob patterns
        :param glob_pattern: str to specify a filter based on glob patterns
        :return: Iterable[s3.ObjectSummary]
        """
        s3_bucket = self.s3_bucket()
        if prefix:
            objects = s3_bucket.objects.filter(Prefix=prefix)
        else:
            objects = s3_bucket.objects.all()
        if glob_pattern:
            objects = (
                s3_obj
                for s3_obj in objects
                if PurePosixPath(s3_obj.key).match(glob_pattern)
            )
        return objects

    def _log_client_errors(self, err):
        # Note: this does _not_ use an instance variable for self.logger
        #       because the loggers cannot be pickled
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        errcode = err.response["Error"]["Code"]
        if status == 404:
            LOGGER.warning("Missing object, %s for %s", errcode, self.s3_uri)
        elif status == 403:
            LOGGER.error("Access denied, %s for %s", errcode, self.s3_uri)
        else:
            LOGGER.exception("Error in request, %s for %s", errcode, self.s3_uri)


@dataclass
class S3Info:
    s3_uri: S3URI
    s3_size: int = None
    last_modified: datetime = None

    @property
    def iso8601(self):
        if self.last_modified:
            return self.last_modified.isoformat()

    @property
    def dict(self) -> Dict:
        return {
            "s3_uri": str(self.s3_uri),
            "s3_size": self.s3_size,
            "last_modified": self.iso8601,
        }

    @property
    def json(self) -> str:
        return json.dumps(self.dict)
