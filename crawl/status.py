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
    Logs the updated status of this job

    :param crawl_stream:
    :param job_launch_id:
    :param state:
    :return:
    """

    logger.info("Crawl stream %s and launch ID %s just updated with status: %s" % (stream, job_launch_id, status))

