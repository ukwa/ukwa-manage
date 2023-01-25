import json
import requests

# Run as:
# python fetch_trackdb.py > trackdb_list.jsonl

SOLR_URL = "http://solr8.api.wa.bl.uk/solr/tracking/sql"

# This just fetches everything for analysis, but is a bit slow, taking about 30 seconds.
# But doing it this way makes complicated reporting much easier.
s = {
    # See https://lucene.apache.org/solr/guide/8_5/parallel-sql-interface.html
    "stmt": "SELECT collection_s, stream_s, kind_s, timestamp_dt, file_size_l, file_path_s, hdfs_service_id_s \
                    FROM tracking WHERE refresh_date_dt = '[NOW-1DAY TO *]' \
                    AND (kind_s = 'warcs' OR kind_s = 'crawl-logs' OR kind_s = 'viral')"
}

r = requests.post(SOLR_URL, data=s)

data = r.json()
docs = data['result-set']['docs']

if 'EXCEPTION' in docs[0]:
    # This should print to stderr:
    print(docs)

for doc in docs:
    print(json.dumps(doc))
