#!/bin/bash

usage() {
    cat <<USAGE
$0 [-h] [--run]

See also https://docs.aws.amazon.com/cli/latest/reference/logs/index.html

These environment variables must be set, e.g.

export AWS_DEFAULT_REGION=us-east-1
export LOG_GROUP_NAME='/aws/batch/job'
export LOG_STREAM_NAME_PREFIX='your-log-stream-name'

The LOG_STREAM_NAME_PREFIX must be set, it has no default.  The others
have the following defaults:

export AWS_DEFAULT_REGION=us-east-1
export LOG_GROUP_NAME='/aws/batch/job'

USAGE
}

cmd=$1
test -z "${cmd}" && usage && exit 1
test "${cmd}" == '-h' && usage && exit 1

export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:=us-east-1}
export LOG_GROUP_NAME=${LOG_GROUP_NAME:='/aws/batch/job'}
export LOG_STREAM_NAME_PREFIX=${LOG_STREAM_NAME_PREFIX}

test -z "${LOG_STREAM_NAME_PREFIX}" && usage && exit 1

cat <<SETTINGS
Using:

AWS_DEFAULT_REGION = ${AWS_DEFAULT_REGION}
LOG_GROUP_NAME = ${LOG_GROUP_NAME}
LOG_STREAM_NAME_PREFIX = ${LOG_STREAM_NAME_PREFIX}

SETTINGS

export BATCH_LOGS_PATH='/tmp/aws_batch_logs'
mkdir -p $BATCH_LOGS_PATH

LOG_STREAMS_FILE="${BATCH_LOGS_PATH}/${LOG_STREAM_NAME_PREFIX}-logstreams.json"
LOG_STREAM_NAMES_FILE="${BATCH_LOGS_PATH}/${LOG_STREAM_NAME_PREFIX}-logstream-names.txt"

aws logs describe-log-streams --log-group-name ${LOG_GROUP_NAME} \
    --log-stream-name-prefix "${LOG_STREAM_NAME_PREFIX}" \
    --no-paginate \
    > "${LOG_STREAMS_FILE}"

# Parse the batch job streams for names
# Uses https://stedolan.github.io/jq/
jq '.logStreams[].logStreamName' "${LOG_STREAMS_FILE}" | sed -e 's/"//g' > "${LOG_STREAM_NAMES_FILE}"

n_log_streams=$(grep -c logStreamName "${LOG_STREAMS_FILE}")
echo "Found $n_log_streams log streams"
echo

# Retrieve all the batch job logs
# Uses https://www.gnu.org/software/parallel/
fetch_batch_logs() {
    log_stream_name=$1
    log_stream_dir=$(dirname "${log_stream_name}")
    mkdir -p "${BATCH_LOGS_PATH}/$log_stream_dir"
    echo "fetching batch job logs:"
    echo "    $log_stream_name"
    aws logs get-log-events --log-group-name "${LOG_GROUP_NAME}" \
        --log-stream-name "${log_stream_name}" \
        --no-paginate \
        > "${BATCH_LOGS_PATH}/${log_stream_name}_events.json"
    echo "    saved to: ${BATCH_LOGS_PATH}/${log_stream_name}_events.json"
    # try to avoid HTTP throttling (sleep between 1 and 10 sec)
    sleep $(( ( RANDOM % 10 )  + 1 ))
}
export -f fetch_batch_logs

if test -f "${LOG_STREAM_NAMES_FILE}"; then
    # shellcheck disable=SC2002
    cat "${LOG_STREAM_NAMES_FILE}" | parallel fetch_batch_logs
fi
