#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "generate-mdx-job <warc-file-list>"
    exit 1
fi

export  JOB_INPUT=$1
export JOB_OUTPUT=/9_processing/warcs2mdx/output.jsonl

hadoop fs -rmr $JOB_OUTPUT

# Run the job (using 5 reducers):
hadoop jar /host/tasks/jars/warc-hadoop-indexer-3.1.0-SNAPSHOT-job.jar \
    uk.bl.wa.hadoop.indexer.mdx.WARCMDXGenerator \
    -Dpdfbox.fontcache=/lvdata/hadoop/tmp \
    -Dmapred.compress.map.output=true \
    -Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -i $JOB_INPUT \
    -o $JOB_OUTPUT \
    -w
        
