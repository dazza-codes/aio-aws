# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

0.2.0 (2020-03-29)
------------------
Focus on aio-aws


0.3.0 (2020-04-05)
------------------
- Refactor aio-client patterns
  - create and release clients on-demand, which
    relies on the session client pool to
    maintain active connections that are
    allocated to clients as they are created


0.4.0 (2020-04-09)
------------------
- Read lambda response content
- poetry update
- Replace aio-aws-lambda code
- Update NOTICES
- Add starlette example app
- Update API docs


0.5.0 (2020-11-04)
------------------
- Add pages directory and make recipe
- Update README
- Fix moto-server backend resets
- Fix aiobotocore test for session credentials coroutine
- make recipe revisions
- poetry update
- Update AWS library dependencies
- Add test utility for moto-server
- Add AWS Batch shell utility scripts
- Enable travis-CI
- Revisions for gitlab -> github
- Allow more time for batch logs test
- Fix moto-s3-bucket creation with unique bucket names
- Fix aio-aws-lambda test
- poetry update
- Use unique fixtures for parallel tests


0.5.1 (2020-11-04)
------------------
- Minor revisions and black formatting


0.6.0 (2020-12-09)
------------------
- Update to python3.7 and prune dependencies
  - drop support for python 3.6
  - refactored aio config
- Remove aio-aws-app and dependencies
  - dropped app-server
- Revise boto* and moto dependencies


0.7.0 (2021-02-15)
------------------
- Breaking changes to decouple semaphores and clients
- Update README


0.8.0 (2021-03-08)
------------------
- Add create-client args and kwargs options
- Fix hyperlinks for sphinx docs


0.9.0 (2021-03-29)
------------------
- Revise lambda.invoke to return lambda


0.10.0 (2021-06-04)
-------------------
- Revise lambda logging and error handling
- Revise default retries and checks
- Modify aio-aws logger as a function


0.11.0 (2021-06-06)
-------------------
- Provide s3-client for s3-coroutines
- Update dependencies for boto3, botocore and aiobotocore
- Update docs
  - Add readthedocs
  - Enable travis deployment for github pages


0.11.1 (2021-06-06)
-------------------
- Fix read-the-docs builds


0.12.0 (2021-06-06)
-------------------
- Add lambda accessors using response and data properties
- Update docs


0.13.0 (2021-06-07)
-------------------
- Update docs with s3-io and s3-aio
- Add non-blocking s3-aio functions
- Add blocking s3-io functions
- Update aiofiles as a required dependency
- Consolidate fixtures into tests.fixtures
- Collect response parsing in aio_aws.utils



0.13.2 (2021-06-11)
-------------------
- Revise aio_aws_lambda logging
- poetry update


0.13.3 (2021-07-21)
-------------------
- Add s3 aio-reader for multiple files


0.13.4 (2021-08-02)
-------------------
- Add a wrapper to run aio-s3 file readers
  as a synchronous collection


0.13.5 (2021-08-11)
-------------------
- Add option to set s3 endpoint from S3_ENDPOINT_URL env-var
- Add utilities into the moto test fixtures


0.13.6 (2021-08-24)
-------------------
- Avoid breaking changes in aiobotocore >= 1.4.x


0.14.0 (2021-08-24)
-------------------
- Use relaxed versions for botocore with aio-botocore


0.15.0 (2021-09-11)
-------------------
- Update docs
- refactor aio-batch-db as optional integration
- add aio-redis option for batch-db
- Refactor common retry exception codes


0.16.0 (2021-09-12)
-------------------
- Refactor and enhance aio-batch utility functions


0.16.1 (2021-09-13)
-------------------
- Enhance and test batch-job filter utilities


0.16.2 (2021-09-14)
-------------------
- Use lazy aioredis connection


0.16.3 (2021-09-14)
-------------------
- Revise redis batch-db client details


0.16.4 (2021-09-20)
-------------------
- Revise jobs-db to fallback on job.submitted for latest job by name
- Add a job submission timestamp to AWSBatchJob
- Allow batch utils to provide a custom config


0.16.5 (2021-09-21)
-------------------
- Add batch jobs-db by-status tools


0.16.6 (2021-10-01)
-------------------
- Revise batch job timestamp and datetime values
- Add jobs-db methods for all jobs and jobs-ids
- Update docs


0.17.0 (2021-11-30)
-------------------
- Add compatible version of s3fs
- Fix aiobotocore.session.get_session()
- Update aiobotocore to 2.x releases
- Update tests for moto 2.x
- Bump moto to 2.x


0.17.1 (2021-12-03)
-------------------
- Relax version of aiobotocore to allow 2.x.y


0.17.2 (2022-02-23)
-------------------
- Add s3 utils to check existence of s3 URIs
- Add batch utils for updating jobs, jobs-db and logs
- Add options for more efficient retrieval of AWS Batch logs
- Revise AWSBatchConfig settings and client creation with args, kwargs
- Revise batch-db jobs_recovery with option to include logs
- Add pytest-aiomoto[s3fs] extra
- Update to pytest-aiomoto 0.2.x
- Update tests to use pytest-aiomoto fixtures
- Use pytest-aiomoto from pypi at 0.1.1
- Update github actions
- Create python-publish.yml


0.17.3 (2022-02-27)
-------------------
- Modify repr(S3URI)


0.17.4 (2022-03-11)
-------------------
- Add warning about missing job-name in jobs-db


0.18.0 (2022-03-11)
-------------------
- Update s3fs 2022.* and aiobotocore ~2.1.0

