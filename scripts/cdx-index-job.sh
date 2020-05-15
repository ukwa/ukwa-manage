#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "cdx-index-job <warc-file-list> <cdx-collection-endpoint>"
    exit 1
fi

export JOB_INPUT=/9_processing/warcs2cdx/warcs-list.txt
export JOB_OUTPUT=/9_processing/warcs2cdx/output

hadoop fs -rm $JOB_INPUT
hadoop fs -rmr $JOB_OUTPUT

# Upload as input:
hadoop fs -put $1 $JOB_INPUT

# Run the job (using 5 reducers):
hadoop jar /host/tasks/jars/warc-hadoop-recordreaders-3.1.0-SNAPSHOT-job.jar \
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
        
