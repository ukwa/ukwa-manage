#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "cdx-index-job <warc-file-list> <cdx-collection-endpoint>"
    exit 1
fi

# Get the folder this script is in:
DIR="$(dirname "$(readlink -f "$0")")"

# If not already set, setup path to the job JAR:
if [[ -z "${JOB_JAR}" ]]; then
    JOB_JAR=${DIR}/warc-hadoop-recordreaders-job.jar
fi

# Set up job input/output
JOB_INPUT=/9_processing/warcs2cdx/warcs-list.txt
JOB_OUTPUT=/9_processing/warcs2cdx/output

# Remove old input/output:
hadoop fs -rm $JOB_INPUT
hadoop fs -rmr $JOB_OUTPUT

# Upload input file:
hadoop fs -put $1 $JOB_INPUT

# Run the job (using 5 reducers):
hadoop jar $JOB_JAR \
    uk.bl.wa.hadoop.mapreduce.cdx.ArchiveCDXGenerator \
    -Dmapred.compress.map.output=true \
    -Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -i $JOB_INPUT \
    -o $JOB_OUTPUT \
    -r 5 \
    -w \
    -h \
    -m "" \
    -t $2 \
    -c "CDX N b a m s k r M S V g"
        
