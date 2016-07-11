"""
crawl/tasks.py:

This series of tasks form a chain, from job (re)starting through to indexing.

"""

from __future__ import absolute_import

from crawl.celery import cfg

import os
import traceback
from datetime import datetime
import dateutil.parser
import json
import pprint
import crawl.h3.hapyx as hapyx
from lib.agents.w3act import w3act
from crawl.w3act.job import W3actJob
from crawl.w3act.job import remove_action_files
from crawl.celery import HERITRIX_ROOT
from crawl.celery import HERITRIX_JOBS
import crawl.status

# import the Celery app context
from crawl.celery import app
# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def restart_job(frequency, start=datetime.utcnow()):
    """Restarts the job for a particular frequency."""
    try:
        logger.info("RS")
        logger.info("Restarting %s at %s" % (frequency, start))

        w = w3act(cfg.get('act','url'),cfg.get('act','username'),cfg.get('act','password'))

        targets = w.get_ld_export(frequency)
        #logger.info("Found %s Targets in export." % len(export))
        #    targets = [t for t in export if (t["startDate"] is None or t["startDate"] < start) and (t["endDateISO"] is None or t["crawlEndDateISO"] > start)]
        logger.debug("Found %s Targets in date range." % len(targets))
        h = hapyx.HapyX("https://%s:%s" % (cfg.get('h3','host'), cfg.get('h3','port')), username=cfg.get('h3','username'), password=cfg.get('h3','password'))
        if frequency in h.list_jobs() and h.status(frequency) != "":
            """Stops a running job, notifies RabbitMQ and cleans up the directory."""
            launch_id = h.get_launch_id(frequency)
            job = W3actJob.from_directory(w, "%s/%s" % (HERITRIX_JOBS, frequency), heritrix=h)
            job.stop()
            remove_action_files(frequency)
            crawl.status.update_job_status.delay(job.name, "%s/%s" % (job.name, launch_id), "STOPPED")

            # Pass on to the next step in the chain:
            logger.info("Requesting indexing for QA for: %s/%s" % (frequency, launch_id))
            validate_job.delay(frequency,launch_id)

        job = W3actJob(w, targets, frequency, heritrix=h)
        logger.info("Starting job %s..." % job.name)
        job.start()
        launch_id = h.get_launch_id(frequency)
        crawl.status.update_job_status.delay(job.name, "%s/%s" % (job.name, launch_id), "LAUNCHED" )
        logger.info("Launched job %s/%s with %s seeds." % (job.name, launch_id, len(job.seeds)))
        return "Launched job %s/%s with %s seeds." % (job.name, launch_id, len(job.seeds))

    except Exception as e:
        logger.exception(e)
        restart_job.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def validate_job(job_id, launch_id):
    """
    This takes the just-completed job and validates that it is complete and ready to process.

    Specifically, it:

    - parses the crawl log, verifying that content was crawled and which WARC files were created.
    - checks for WARC files on HDFS in the correct location.
        - if they are not there yet, it retries later on.

    Note that a separate daemon process is busy copying up to HDFS as the data comes in.

    Currently passes straight on to SIP generation, as that is our current workflow. However, we
    should review this at some point and consider automated QA before attempting ingest.

    :param job_id:
    :param launch_id:
    :return:
    """
    try:
        logger.info("Got validate job for: %s/%s" % (job_id, launch_id))
        # Now initiate QA processing:
        #logger.info("Requesting index-for-QA for: %s/%s" % (job_id, launch_id))
        #index_for_qa.delay(job_id, launch_id)
        # Now initiate SIP build:
        logger.info("Requesting SIP-build for: %s/%s" % (job_id, launch_id))
        build_sip.delay(job_id, launch_id)
    except Exception as e:
        logger.exception(e)
        validate_job.retry(exc=e)

#
# @app.task(acks_late=True, max_retries=None, default_retry_delay=10)
# def index_for_qa(job_id,launch_id):
#     """
#     Does this belong in some WARC tasks stream?
#
#     :param job_id:
#     :param launch_id:
#     :return:
#     """
#     logger.info("Got index for QA for: %s/%s" % (job_id, launch_id))
#     logger.info("Requesting job QA for: %s/%s" % (job_id, launch_id))
#     qa_job.delay(job_id, launch_id)
#
#
# @app.task(acks_late=True, max_retries=None, default_retry_delay=10)
# def qa_job(job_id,launch_id):
#     logger.info("Got job QA for: %s/%s" % (job_id, launch_id))
#     # e.g. Check all seeds are present?
#     logger.info("Requesting SIP build for: %s/%s" % (job_id, launch_id))
#     build_sip.delay(job_id, launch_id)
#

@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def build_sip(job_id,launch_id):
    try:
        logger.info("Got SIP build for: %s/%s" % (job_id, launch_id))
        logger.info("Requesting SIP submission for: %s/%s" % (job_id, launch_id))
        submit_sip.delay(job_id, launch_id)
    except Exception as e:
        logger.exception(e)
        build_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def submit_sip(job_id,launch_id):
    try:
        logger.info("Got SIP submission for: %s/%s" % (job_id, launch_id))
        logger.info("Sending SIP verify for: %s/%s" % (job_id, launch_id))
        verify_sip.delay(job_id, launch_id)
    except Exception as e:
        logger.exception(e)
        submit_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def verify_sip(job_id,launch_id):
    try:
        logger.info("Got SIP verify for: %s/%s" % (job_id, launch_id))
        logger.info("Sending SIP index for: %s/%s" % (job_id, launch_id))
        index_sip.delay(job_id, launch_id)
    except Exception as e:
        logger.exception(e)
        verify_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def index_sip(job_id,launch_id):
    try:
        logger.info("Got SIP index for: %s/%s" % (job_id, launch_id))
        # TODO Pass on to Solr?
    except Exception as e:
        logger.exception(e)
        index_sip.retry(exc=e)

