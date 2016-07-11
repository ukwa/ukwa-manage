# If on Python 2.X
from __future__ import print_function
import pysolr
from datetime import datetime

# Setup a Solr instance. The timeout is optional.
solr = pysolr.Solr('http://localhost:8983/solr/crawl_state', timeout=10)


def update_job_state(crawl_stream, job_id, state):
    """
    Submits an update on the crawl state to the crawl-status Solr instance.

    :param crawl_stream:
    :param job_id:
    :param state:
    :return:
    """

    # solr.add([
    #     {
    #         "id": job_id,
    #         "type": "job",
    #         "state": state,
    #     },
    # ])

    timestamp = datetime.utcnow()

    doc = {
        'id': job_id,
        'status_s': state,
        'crawl_stream_s': crawl_stream,
        'last_updated_tdt': timestamp,
        '%s_at_tdt' % state.lower(): timestamp,
        'states_ss': "%s@%s" % (state, timestamp)
    }
    solr.add([doc], fieldUpdates={
        'status_s': 'set',
        'crawl_stream_s': 'set',
        'last_updated_tdt': 'set',
        '%s_at_tdt' % state.lower(): 'set',
        'states_ss': 'add'
    })


def check_state():
    # Later, searching is easy. In the simple case, just a plain Lucene-style
    # query is fine.
    results = solr.search('bananas')

    # The ``Results`` object stores total results found, by default the top
    # ten most relevant results and any additional data like
    # facets/highlighting/spelling/etc.
    print("Saw {0} result(s).".format(len(results)))

    # Just loop over it to access the results.
    for result in results:
        print("The title is '{0}'.".format(result['title']))
