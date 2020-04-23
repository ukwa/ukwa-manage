#!/bin/bash
#
# 

# Make a folder for the CSV dump:
mkdir w3act-db-csv
w3act-csv -H postgres get-csv

# Convert to JSON and GZIP:
w3act-csv csv-to-json
gzip w3act-db-csv.json

# Upload to HDFS, using the 'ingest' user account:
store put -u ingest w3act-db-csv.json.gz /9_processing/w3act
