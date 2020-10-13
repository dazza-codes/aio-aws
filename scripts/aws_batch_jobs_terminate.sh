#!/bin/bash

usage() {
    cat <<USAGE

TERMINATE JOBS

$0 [-h {job-id-file} {aws-region}]

-h issues this help

{job-id-file} defaults to "jobId.txt"

{aws-region} defaults to AWS_DEFAULT_REGION or us-east-1

Use 'aws_batch_job_status.sh' to get job_id_file outputs; then filter the
JSON for jobId values to terminate and put those into a {job-id-file} with
one jobId value per line.

USAGE
}

test "${1}" == '-h' && usage && exit

job_id_file=${1:-"jobId.txt"}
test -z "${job_id_file}" && usage && exit 1

default_region=${AWS_DEFAULT_REGION:-us-east-1}
AWS_REGION=${2:-$default_region}


terminator() {
    job_id=$1
    reason=${2:-"I'll be BACK"}
    echo "terminating batch job: $job_id"
    aws batch terminate-job --region "${AWS_REGION}" --reason "$reason" --job-id "$job_id"
}

if test -f "${job_id_file}"
then

    if which parallel > /dev/null
    then
        # Uses https://www.gnu.org/software/parallel/
        export -f terminator
        # shellcheck disable=SC2002
        cat "${job_id_file}" | parallel terminator
    else
        while read line
        do
            terminator $line
        done < "${job_id_file}"
    fi
else
    echo "Did not find ${job_id_file} file"
    echo "Place batch jobId values into '${job_id_file}', one jobId per line"
fi
