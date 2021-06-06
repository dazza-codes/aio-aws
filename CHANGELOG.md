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

