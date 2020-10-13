#!/bin/bash

usage() {
    cat <<USAGE

$0 [-h] [-l] {job-queue} [{aws-region}]

-h issues this help

-l lists batch queue names for {aws-region}

{job-queue} a jobQueueName is required; if the jobQueueName does not exist,
the download still works and dumps empty data files

{aws-region} defaults to AWS_DEFAULT_REGION or us-east-1

This utility uses the 'aws batch' CLI to download all job-status data for an
AWS batch job queue. The download format is JSON and downloads go to
/tmp/aws_batch_jobs

These outputs can be used to generate simple statistics for batch jobs and to
identify jobs to re-run.

USAGE
}

test "${1}" == '-h' && usage && exit

default_region=${AWS_DEFAULT_REGION:-us-east-1}
AWS_REGION=${2:-$default_region}

if test "${1}" == '-l'
then
    echo
    echo "AWS batch job-queues in ${AWS_REGION}:"
    echo
    aws batch --region ${AWS_REGION} describe-job-queues | grep jobQueueName | sort -u
    echo
    exit
fi

job_queue=$1
test -z "${job_queue}" && usage && exit 1

datestamp=$(date -u +"%Y-%m-%d")
job_path="/tmp/aws_batch_jobs/${datestamp}"
job_file="${job_path}/${job_queue}"
mkdir -p $job_path

echo
echo "Running: $0 ${job_queue} ${AWS_REGION} > ${job_file}*.*"
echo

declare -a JOB_STATUS_ARR=('PENDING' 'RUNNABLE' 'RUNNING' 'FAILED' 'SUCCEEDED')

for job_status in "${JOB_STATUS_ARR[@]}"
do
    status_file="${job_file}_${job_status}.json"

    aws batch list-jobs \
        --region ${AWS_REGION} \
        --no-paginate \
        --job-queue "${job_queue}" \
        --job-status "${job_status}" > "${status_file}"

    count=$(cat "${status_file}" | grep 'jobId' | wc -l)
    echo "${job_status}: ${count}"
    echo -e "  json file:\t${status_file}"

    if command -v jq > /dev/null
    then
        # install https://stedolan.github.io/jq/
        job_id_file="${job_file}_${job_status}_jobId.txt"
        job_name_file="${job_file}_${job_status}_jobName.txt"
        jq '.jobSummaryList[].jobId'   "${status_file}" | sed -e 's/"//g' > "${job_id_file}"
        jq '.jobSummaryList[].jobName' "${status_file}" | sed -e 's/"//g' > "${job_name_file}"
        echo -e "  job ids:\t${job_id_file}"
        echo -e "  job names:\t${job_name_file}"
    fi

done
