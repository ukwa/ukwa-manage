#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "index-solr-job <config.conf> <warc-file-list>"
    exit 1
fi

export JOB_INPUT=$2
export JOB_OUTPUT=/9_processing/warcs2solr2/output.jsonl

hadoop fs -rmr $JOB_OUTPUT

# Run the job (using 5 reducers):
hadoop jar /host/tasks/jars/warc-hadoop-indexer-3.1.0-SNAPSHOT-job.jar \
    uk.bl.wa.hadoop.indexer.WARCIndexerRunner \
    -Dpdfbox.fontcache=/lvdata/hadoop/tmp \
    -Dmapred.compress.map.output=true \
    -Dmapred.reduce.max.attempts=2 \
    -c $1 \
    -i $JOB_INPUT \
    -o $JOB_OUTPUT \
    -w
    #-a \
    #-files annotation_fpfile,whitelist_fpfile
