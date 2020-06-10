#!/bin/sh
#
# Run with the correct environment, this will backup the JSON from W3ACT to HDFS.
#
# e.g.
#
#     docker run -ti -e W3ACT_PSQL_PASSWORD --network w3act_default ukwa/ukwa-manage w3act-to-hdfs.sh
#

# Exit when any (simple) command fails:
set -e

# Make a folder for the CSV dump:
mkdir w3act-db-csv

# Download the CSV dump of the W3ACT database:
w3act-csv -H postgres get-csv

# Convert to JSON:
w3act-csv csv-to-json

# ...and GZIP it:
gzip w3act-db-csv.json

# Upload to HDFS, using the 'ingest' user account:
store -u ingest put w3act-db-csv.json.gz /9_processing/w3act
