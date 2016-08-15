"""
crawl/tasks.py:

This series of tasks form a chain, from job (re)starting through to indexing.

"""

from __future__ import absolute_import

from crawl.celery import cfg

import os
import shutil
from datetime import datetime
from celery.exceptions import Reject

import crawl.h3.hapyx as hapyx
from crawl.w3act.w3act import w3act
from crawl.w3act.job import W3actJob
from crawl.w3act.job import remove_action_files
from crawl.job.output import CrawlJobOutput
from crawl.sip.creator import SipCreator
from crawl.sip.submitter import SubmitSip
from crawl.cdx.tinycdxserver import send_uri_to_tinycdxserver
from crawl.dex.to_w3act import send_document_to_w3act

from crawl.celery import HERITRIX_ROOT
from crawl.celery import HERITRIX_HDFS_ROOT
from crawl.celery import HERITRIX_JOBS
import crawl.status

# import the Celery app context
from crawl.celery import app
# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def stop_start_job(frequency, start=datetime.utcnow(), restart=True):
    """Restarts the job for a particular frequency."""
    try:
        logger.info("Stopping/starting %s at %s" % (frequency, start))

        # Set up connection to W3ACT:
        w = w3act(cfg.get('act','url'),cfg.get('act','username'),cfg.get('act','password'))
        # Set up connection to H3:
        h = hapyx.HapyX("https://%s:%s" % (cfg.get('h3','host'), cfg.get('h3','port')), username=cfg.get('h3','username'), password=cfg.get('h3','password'))

        # Stop job if currently running:
        if frequency in h.list_jobs() and h.status(frequency) != "":
            """Stops a running job, notifies RabbitMQ and cleans up the directory."""
            launch_id = h.get_launch_id(frequency)
            job = W3actJob.from_directory(w, "%s/%s" % (HERITRIX_JOBS, frequency), heritrix=h)
            job.stop()
            remove_action_files(frequency)
            crawl.status.update_job_status.delay(job.name, "%s/%s" % (job.name, launch_id), "STOPPED")

            # Pass on to the next step in the chain:
            logger.info("Requesting assembly of output for: %s/%s" % (frequency, launch_id))
            assemble_job_output.delay(frequency,launch_id)
        else:
            job = None

        # Start job if requested:
        if restart:
            targets = w.get_ld_export(frequency)
            # logger.info("Found %s Targets in export." % len(export))
            #    targets = [t for t in export if (t["startDate"] is None or t["startDate"] < start) and (t["endDateISO"] is None or t["crawlEndDateISO"] > start)]
            logger.debug("Found %s Targets in date range." % len(targets))
            job = W3actJob(w, targets, frequency, heritrix=h)
            logger.info("Starting job %s..." % job.name)
            job.start()
            launch_id = h.get_launch_id(frequency)
            crawl.status.update_job_status.delay(job.name, "%s/%s" % (job.name, launch_id), "LAUNCHED" )
            logger.info("Launched job %s/%s with %s seeds." % (job.name, launch_id, len(job.seeds)))
            return "Launched job %s/%s with %s seeds." % (job.name, launch_id, len(job.seeds))
        else:
            if job:
                logger.info("Stopped job %s/%s without restarting..." % (job.name, launch_id))
                return "Stopped job %s/%s without restarting..." % (job.name, launch_id)
            else:
                logger.warning("No running '%s' job to stop!" % frequency)
                return "No running '%s' job to stop!" % frequency

    except Exception as e:
        logger.exception(e)
        raise Reject(e, requeue=True)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def assemble_job_output(job_id, launch_id):
    """
    This takes the just-completed job and validates that it is complete and ready to process.

    Specifically, it:

    - checks the crawl log is there, and that there is no crawl.log.lck file, and no other crawl.log files.
    - parses the crawl log, generating stats on what content was crawled and recording which WARC files were created.
    - checks for WARC files on HDFS in the correct location.
    - if they are any problems, like missing files, it retries later on.

    Note that a separate daemon process (movetohdfs.py) is busy copying up to HDFS as the data comes in.

    Currently passes straight on to SIP generation, as that is our current workflow. However, we
    should review this at some point and consider indexing for automated QA before attempting ingest.

    :param job_id:
    :param launch_id:
    :return:
    """
    try:
        logger.info("Got assemble_job_output for: %s/%s" % (job_id, launch_id))
        # Check all is well
        # Parse the logs
        # Check for the WARCs
        # Copy necessary logs (and any other files for the SIP) up to HDFS
        job_output = CrawlJobOutput.assemble(job_id, launch_id)

        # Update the job status:
        crawl.status.update_job_status.delay(job_id, "%s/%s" % (job_id, launch_id), "VALIDATED" )
        # TODO Request post-processing for indexing and to extract documents?
        # Now initiate SIP build:
        logger.info("Requesting SIP-build for: %s/%s" % (job_id, launch_id))
        build_sip.delay(job_id, launch_id, job_output)
    except Exception as e:
        logger.exception(e)
        assemble_job_output.retry(exc=e)

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
def build_sip(job_id, launch_id, job_output):
    try:
        logger.info("Got SIP build for: %s/%s" % (job_id, launch_id))
        logger.info("Job Output: %s", job_output)
        # Build and package the SIP:
        sip = SipCreator([job_output['job_id']], warcs=job_output['warcs'], viral=job_output['viral'], logs=job_output['logs'], dummy_run=True)
        # Move it up to HDFS:
        sip_name = launch_id
        sip_dir = os.path.abspath("%s/sips/%s/%s" % (HERITRIX_ROOT, job_id,sip_name))
        sip.create_sip(sip_dir)
        sip_on_hdfs = sip.copy_sip_to_hdfs(sip_dir, "%s/sips/%s/%s" % (HERITRIX_HDFS_ROOT, job_id, launch_id) )
        shutil.rmtree(sip_dir)

        # Update the job status:
        crawl.status.update_job_status.delay(job_id, "%s/%s" % (job_id, launch_id), "SIP_BUILT" )
        logger.info("Requesting SIP submission for: %s/%s" % (job_id, launch_id))
        submit_sip.delay(job_id, launch_id, sip_on_hdfs)
    except Exception as e:
        logger.exception(e)
        build_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=10)
def submit_sip(job_id,launch_id,sip_on_hdfs):
    try:
        logger.info("Got SIP submission for: %s/%s" % (job_id, launch_id))
        logger.info("Got SIP HDFS Path: %s" % sip_on_hdfs)
        # Download, check and submit the SIP:
        sub = SubmitSip(job_id, launch_id, sip_on_hdfs)

        # Update the job status:
        crawl.status.update_job_status.delay(job_id, "%s/%s" % (job_id, launch_id), "SIP_SUBMITTED" )
        logger.info("Sending SIP verify for: %s/%s" % (job_id, launch_id))
        verify_sip.delay(job_id, launch_id,sip_on_hdfs)
    except Exception as e:
        logger.exception(e)
        submit_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=100)
def verify_sip(job_id,launch_id,sip_on_hdfs):
    try:
        logger.info("Got SIP verify for: %s/%s" % (job_id, launch_id))
        logger.info("Got SIP HDFS Path: %s" % sip_on_hdfs)
        # Download the SIP package, unpack it.
        # Parse the METS to get the WARC metadata (ARKs, lengths, SHA512 hashes):
        # Check DLS for each ARK:

        if True:
            raise Exception("VERIFICATION Not Implemented Yet!")

        # Update the job status:
        crawl.status.update_job_status.delay(job_id, "%s/%s" % (job_id, launch_id), "SIP_VALIDATED" )
        logger.info("Sending SIP index for: %s/%s" % (job_id, launch_id))
        index_sip.delay(job_id, launch_id)
    except Exception as e:
        logger.exception(e)
        verify_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=100)
def index_sip(job_id,launch_id):
    try:
        logger.info("Got SIP index for: %s/%s" % (job_id, launch_id))

        if True:
            raise Exception("INDEXING Not Implemented Yet!")

        # TODO Pass on to Solr?
        # Update the job status:
        crawl.status.update_job_status.delay(job_id, "%s/%s" % (job_id, launch_id), "SIP_INDEXED" )
    except Exception as e:
        logger.exception(e)
        index_sip.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=100)
def uri_to_index(**kwargs):
    try:
        logger.debug("Got URI to index: %s" % kwargs)
        send_uri_to_tinycdxserver(cfg.get('tinycdxserver','endpoint'), kwargs)

    except Exception as e:
        logger.exception(e)
        uri_to_index.retry(exc=e)


@app.task(acks_late=True, max_retries=None, default_retry_delay=100)
def uri_of_doc(**kwargs):
    try:
        logger.info("Got doc to send to W3ACT for: %s" % kwargs)

        # Set up connection to W3ACT:
        w = w3act(cfg.get('act','url'),cfg.get('act','username'),cfg.get('act','password'))
        # And post this document up:
        send_document_to_w3act(kwargs,cfg.get('wayback','endpoint'),w)

    except Exception as e:
        logger.exception(e)
        uri_of_doc.retry(exc=e)

