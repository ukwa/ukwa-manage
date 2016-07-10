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

            # Pass on to the next step in the chain:
            logger.info("Requesting indexing for QA for: %s/%s" % (frequency, launch_id))
            index_for_qa.delay(frequency,launch_id)

        job = W3actJob(w, targets, frequency, heritrix=h)
        logger.debug("Starting job %s with %s seeds." % (job.name, len(job.seeds)))
        job.start()
    except Exception as e:
        logger.exception(e)
        restart_job.retry(exc=e)


@app.task()
def index_for_qa(job_id,launch_id):
    logger.info("Got index for QA request")
    logger.info("Requesting QA for: %s/%s" % (job_id, launch_id))
    qa_job.delay(job_id, launch_id)


@app.task()
def qa_job(job_id,launch_id):
    logger.info("Got QA request")
    logger.info("Requesting SIP build for: %s/%s" % (job_id, launch_id))
    build_sip.delay(job_id, launch_id)


@app.task()
def build_sip(job_id,launch_id):
    logger.info("Got sip build request")
    logger.info("Sending SIP submission...")
    submit_sip.delay(job_id, launch_id)


@app.task()
def submit_sip(job_id,launch_id):
    logger.info("Got sip submission request")
    logger.info("Sending SIP submission...")
    verify_sip.delay(job_id, launch_id)


@app.task()
def verify_sip(job_id,launch_id):
    logger.info("Got sip verification request")
    logger.info("Sending SIP submission...")
    index_sip.delay(job_id, launch_id)


@app.task()
def index_sip(job_id,launch_id):
    logger.info("Got sip index request")
    # TODO Pass on to Solr?


@app.task()
def add(x, y):
    tot = x + y
    mul.delay(tot,x)
    return tot


@app.task()
def mul(x,y):
    logger.info("MUL")
    return x*y


