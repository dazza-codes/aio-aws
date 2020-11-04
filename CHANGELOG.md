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

