'''
Functions for interacting with Solr, as a Tracking Database.

NOTE requires Solr > 7.3 as uses 'add-distinct'

See https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html

'''
import requests
import logging
import json

logger = logging.getLogger(__name__)

HDFS_KINDS = ['files', 'warcs', 'logs'] # kinds of records that correspond to different HDFS files
HDFS_PREFIX = 'hdfs://' # Used to sanity-check HDFS IDs on import.

class SolrTrackDB():

    def __init__(self, trackdb_url, hadoop=None, kind='warcs', update_batch_size=1000):
        self.trackdb_url = trackdb_url
        self.hadoop = hadoop
        self.kind = kind
        self.batch_size = update_batch_size
        # Set up the update configuration:
        self.update_trackdb_url = self.trackdb_url + '/update?softCommit=true'

    def _jsonl_doc_generator(self, input_reader):
        for line in input_reader:
            item = json.loads(line)
            # Provide the kind, if not set already in the item:
            if not 'kind_s' in item:
                item['kind_s'] = self.kind
            # Enforce ID conventions for particular types:
            if self.kind == 'documents':
                if not 'id' in item:
                    item['id'] = 'document:document_url:%s' % item['document_url']
            elif self.kind in HDFS_KINDS:
                if not 'id' in item:
                    raise Exception("When importing files you must supply an id for each!")
                if not item['id'].startswith(HDFS_PREFIX):
                    raise Exception("When importing files the ID must start with '%s'!" % HDFS_PREFIX)
            else:
                raise Exception("Cannot import %s records yet!" % self.kind)
            # And return
            yield item

    def _send_batch(self, batch, as_updates=True):
        # Convert the plain dicts into Solr update documents:
        updates = []
        for item in batch:
            update_item = {}
            for key in item:
                if key == 'id':
                    update_item[key] = item[key]
                elif key == '_version_':
                    # Do nothing, as we don't want to send that, because it'll cause conflicts on import.
                    pass
                else:
                    # If we want to send updates, except those already arranged as updates (i.e. as dicts):
                    if as_updates and not isinstance(item[key], dict):
                        # Convert to 'set' updates:
                        update_item[key] = { 'set': item[key] }
                    else:
                        update_item[key] = item[key]
            # Add the item to the set:
            updates.append(update_item)

        # And post the batch as updates:
        self._send_update(updates)

    def import_jsonl_reader(self, input_reader):
        self.import_item_stream(self._jsonl_doc_generator(input_reader))

    def import_items(self, items):
        self._send_batch(items)

    def import_item_stream(self, item_generator):
        batch = []
        for item in item_generator:
            batch.append(item)
            if len(batch) > self.batch_size:
                self._send_batch(batch)
                batch = []
        # And send the final batch if there is one:
        if len(batch) > 0:
            self._send_batch(batch)

    def list(self, stream=None, year=None, field_value=None, sort='timestamp_dt desc', limit=100):
        # set solr search terms
        solr_query_url = self.trackdb_url + '/query'
        query_string = {
            'q':'kind_s:{}'.format(self.kind),
            'rows':limit,
            'sort':sort
        }
        # Add optional fields:
        if self.hadoop:
            query_string['q'] += ' AND hdfs_service_id_s:%s' % self.hadoop
        if stream:
            query_string['q'] += ' AND stream_s:%s' % stream
        if year:
            query_string['q'] += ' AND year_i:%s' % year
        if field_value:
            if field_value[1] == '_NONE_':
                query_string['q'] += ' AND -{}:[* TO *]'.format(field_value[0])
            else:
                query_string['q'] += ' AND {}:{}'.format(field_value[0], field_value[1])
        # gain tracking_db search response
        logger.info("SolrTrackDB.list: %s %s" %(solr_query_url, query_string))
        r = requests.post(url=solr_query_url, data=query_string)
        if r.status_code == 200:
            response = r.json()['response']
            # return hits, if any:
            if response['numFound'] > 0:
                return response['docs']
            else:
                return []
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def get(self, id):
        # set solr search terms
        solr_query_url = self.trackdb_url + '/query'
        query_string = {
            'q':'kind_s:{} AND id:"{}"'.format(self.kind, id)
        }
        # gain tracking_db search response
        logger.info("SolrTrackDB.get: %s %s" %(solr_query_url, query_string))
        r = requests.post(url=solr_query_url, data=query_string)
        if r.status_code == 200:
            response = r.json()['response']
            # return hits, if any:
            if response['numFound'] == 1:
                return response['docs'][0]
            else:
                return None
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def _send_update(self, post_data):
        # Covert the list of docs to JSONLines:
        #post_data = ""
        #for item in docs:
        #    post_data += ("%s\n" % json.dumps(item))
        # Set up the POST and check it worked
        post_headers = {'Content-Type': 'application/json'}
        logger.info("SolrTrackDB.update: %s %s" %(self.update_trackdb_url, str(post_data)[0:1000]))
        r = requests.post(url=self.update_trackdb_url, headers=post_headers, json=post_data)
        if r.status_code == 200:
            response = r.json()
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def _update_generator(self, ids, field, value, action):
        for id in ids:
            # Update TrackDB record for records based on ID:
            yield { 'id': id, field: { action: value } } 
        
    def update(self, ids, field, value, action='add-distinct'):
        self.import_item_stream(self._update_generator(ids, field, value, action))

