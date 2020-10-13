#!/usr/bin/env bash

curl -o jq-linux64 -sSL https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 \
    && mv jq-linux64 /usr/local/bin/jq \
    && chmod a+x /usr/local/bin/jq \
    && jq --version
