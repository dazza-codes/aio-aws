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

