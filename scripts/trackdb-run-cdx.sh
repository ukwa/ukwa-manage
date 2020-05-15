#!/bin/bash

# Important line that stops the script continuing if part of the job fails!
set -e

# Parameters:
YEAR="2020"
STREAM="frequent"
CDX_COLLECTION="data-heritrix"
CDX_SERVER="http://cdx.dapi.wa.bl.uk/"
TRACKDB_URL="http://trackdb.dapi.wa.bl.uk/solr/tracking"

# Get the folder this script is in:
DIR="$(dirname "$(readlink -f "$0")")"

# Get a list of WARCs to process from the TrackDB:
echo "Listing WARCs..."
trackdb warcs --stream $STREAM --year $YEAR --field cdx_index_ss _NONE_ list --limit 1000 --ids-only > warcs-to-index.ids

# Run the Hadoop job using this list:
echo "Running Hadoop job..."
windex --cdx-service ${CDX_SERVER} --cdx-collection ${CDX_COLLECTION} cdx-index warcs-to-index.ids

# If all is well, update the TrackDB (skipping verification for now):
echo "Marking WARCs as indexed..."
cat warcs-to-index.ids | trackdb warcs update --set cdx_index_ss ${CDX_COLLECTION} -

