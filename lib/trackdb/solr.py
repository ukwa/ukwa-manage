'''
Functions for interacting with Solr, as a Tracking Database.

NOTE requires Solr > 7.3 as uses 'add-distinct'

See https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html

'''
import requests
import logging

logger = logging.getLogger(__name__)

class SolrTrackDB():

    # Some optional filters:
    stream = None # Could filter to e.g. 'frequent'
    year = None # Could filter to e.g. 2020

    def __init__(self, trackdb_url, kind='warcs'):
        self.trackdb_url = trackdb_url
        self.kind = kind

    def list(self, stream, year, field, value, sort='timestamp_dt desc', limit=100):
        # set solr search terms
        solr_query_url = self.trackdb_url + '/query'
        query_string = {
            'q':'kind_s:{} AND {}:{}'.format(
                self.kind, field, value),
            'rows':limit,
            'sort':sort
        }
        # Add optional fields:
        if self.stream:
            query_string['q'] += ' AND stream_s:"%s"' % self.stream
        if self.year:
            query_string['q'] += ' AND year_i:"%s"' % self.year
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

    def update(self, id, field, value):
        # Update trackdb record for warc
        update_trackdb_url = self.trackdb_url + '/update?commitWithin=1000'
        post_headers = {'Content-Type': 'application/json'}
        post_data = { 'id': id, field: {'add-distinct': value} }

        # gain tracking_db search response
        r = requests.post(url=update_trackdb_url, headers=post_headers, json=[post_data])
        if r.status_code == 200:
            response = r.json()
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))
