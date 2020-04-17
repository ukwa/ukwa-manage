'''
Functions for interacting with Solr, as a Tracking Database.

NOTE requires Solr > 7.3 as uses 'add-distinct'

See https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html

'''
import requests
import logging
import json

logger = logging.getLogger(__name__)

class SolrTrackDB():

    def __init__(self, trackdb_url, kind='warcs', update_batch_size=1000):
        self.trackdb_url = trackdb_url
        self.kind = kind
        self.batch_size = update_batch_size
        # Set up the update configuration:
        self.update_trackdb_url = self.trackdb_url + '/update?softCommit=true'

    def _jsonl_doc_generator(self, input_reader):
        for line in input_reader:
            item = json.loads(line)
            item['kind_s'] = self.kind
            if self.kind == 'documents':
                item['id'] = 'document:document_url:%s' % item['document_url']
            else:
                raise Exception("Cannot import %s records yet!" % self.kind)
            # And return
            yield item

    def _send_as_updates(self, batch):
        # Convert the plain dicts into Solr update documents:
        as_updates = []
        for item in batch:
            update_item = {}
            for key in item:
                if key == 'id':
                    update_item[key] = item[key]
                else:
                    update_item[key] = { 'set': item[key] }
            as_updates.append(update_item)

        # And post the batch:
        self._send_update(batch)

    def import_jsonl(self, input_reader):
        batch = []
        for item in self._jsonl_doc_generator(input_reader):
            batch.append(item)
            if len(batch) > self.batch_size:
                self._send_as_updates(batch)
                batch = []
        # And send the final batch if there is one:
        if len(batch) > 0:
            self._send_as_updates(batch)

    def list(self, stream=None, year=None, field_value=None, sort='timestamp_dt desc', limit=100):
        # set solr search terms
        solr_query_url = self.trackdb_url + '/query'
        query_string = {
            'q':'kind_s:{}'.format(self.kind),
            'rows':limit,
            'sort':sort
        }
        # Add optional fields:
        if stream:
            query_string['q'] += ' AND stream_s:%s' % stream
        if year:
            query_string['q'] += ' AND year_i:%s' % year
        if field_value:
            query_string['q'] += ' AND {}'.format(field_value)
        # gain tracking_db search response
        logger.warn("SolrTrackDB.list: %s %s" %(solr_query_url, query_string))
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

    def update(self, id, field, value, action='add-distinct'):
        # Update trackdb record for warc
        post_data = [{ 'id': id, field: { action: value} }]
        self._send_update(post_data)
