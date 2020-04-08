'''
Functions for interacting with Solr, as a Tracking Database.

NOTE requires Solr > 7.3 as uses 'add-distinct'

See https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html

'''
import requests
import logging

logger = logging.getLogger(__name__)

class SolrTrackDB():

    def __init__(self, trackdb_url, kind='warcs'):
        self.trackdb_url = trackdb_url
        self.kind = kind
        # Set up the update configuration:
        self.update_trackdb_url = self.trackdb_url + '/update?softCommit=true&commitWithin=5000'
        

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

    def update(self, id, field, value, action='add-distinct'):
        # Update trackdb record for warc
        post_headers = {'Content-Type': 'application/json'}
        post_data = { 'id': id, field: { action: value} }

        # gain tracking_db search response
        logger.info("SolrTrackDB.update: %s %s" %(self.update_trackdb_url, post_data))
        r = requests.post(url=self.update_trackdb_url, headers=post_headers, json=[post_data])
        if r.status_code == 200:
            response = r.json()
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))
