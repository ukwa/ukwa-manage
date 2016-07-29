# If on Python 2.X
from __future__ import print_function
from __future__ import absolute_import

import pysolr
from datetime import datetime

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


# def check_state():
#     # Later, searching is easy. In the simple case, just a plain Lucene-style
#     # query is fine.
#     results = solr.search('bananas')
#
#     # The ``Results`` object stores total results found, by default the top
#     # ten most relevant results and any additional data like
#     # facets/highlighting/spelling/etc.
#     print("Saw {0} result(s).".format(len(results)))
#
#     # Just loop over it to access the results.
#     for result in results:
#         print("The title is '{0}'.".format(result['title']))
#

@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def update_job_status(stream, job_launch_id, status):
    """
    Submits an update on the crawl state to the crawl-status Solr instance.

    Can be gathered together using field collapsing/group queries, like this:

    http://localhost:8983/solr/crawl_state/select?group.field=job_launch_id_s&group.limit=10&group=true&indent=on&q=*:*&sort=at_tdt%20desc&wt=json

    :param crawl_stream:
    :param job_launch_id:
    :param state:
    :return:
    """
    try:
        # Setup a Solr instance. The timeout is optional.
        solr = pysolr.Solr(cfg.get('solr','crawl_state_solr'), timeout=10)

        timestamp = datetime.utcnow()

        # Add each event as distinct document:
        doc = {
            'type_s' : "crawl-job-state",
            'crawl_stream_s': stream,
            'job_launch_id_s': job_launch_id,
            'status_s': status,
            'at_tdt': timestamp,
            '%s_at_tdt' % status.lower(): timestamp,
        }
        solr.add([doc])
        # As updates to a single document (clumsier overall):
        # doc = {
        #     'id': job_launch_id,
        #     'status_s': state,
        #     'crawl_stream_s': crawl_stream,
        #     'last_updated_tdt': timestamp,
        #     '%s_at_tdt' % state.lower(): timestamp,
        #     'states_ss': "%s@%s" % (state, timestamp)
        # }
        # solr.add([doc], fieldUpdates={
        #     'status_s': 'set',
        #     'crawl_stream_s': 'set',
        #     'last_updated_tdt': 'set',
        #     '%s_at_tdt' % state.lower(): 'set',
        #     'states_ss': 'add'
        # })

        return "Updated job status for job %s in stream %s to %s" % (job_launch_id, stream, status)
    except Exception as e:
        logger.exception(e)
        #update_job_status.retry(exc=e)

