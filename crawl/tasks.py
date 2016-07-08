from __future__ import absolute_import

from crawl.celery import cfg

import os
from glob import glob
from datetime import datetime
import dateutil.parser
import hapy
import json
from lib.agents.w3act import w3act
from crawl.w3act.job import W3actJob
from crawl.celery import HERITRIX_ROOT
from crawl.celery import HERITRIX_JOBS

# import the Celery app context
from crawl.celery import app
# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@app.task()
def add(x, y):
    tot = x + y
    mul.delay(tot,x)
    return tot

@app.task()
def mul(x,y):
    logger.info("MUL")
    return x*y

def remove_action_files(jobname):
    """Removes old 'action' files and symlinks."""
    actions_done = "%s/%s/latest/actions-done" % (HERITRIX_JOBS, jobname)
    done = "%s/%s/action/done" % (HERITRIX_JOBS, jobname)
    for root in [actions_done, done]:
        if os.path.exists(root):
            to_remove = glob("%s/*" % root)
            logger.info("Removing %s action files." % len(to_remove))
            for action in to_remove:
                os.remove(action)

def check_watched_targets(jobname, heritrix):
    """If there are any Watched Targets, send a message."""
    timestamp = heritrix.launchid(jobname)
    if not os.path.exists("%s/%s/%s/w3act-info.json" % (HERITRIX_JOBS, jobname, timestamp)):
        return
    with open("%s/%s/%s/w3act-info.json" % (HERITRIX_JOBS, jobname, timestamp), "rb") as i:
        info = i.read()
    for job in json.loads(info):
        if job["watched"]:
            logger.info("Found a Watched Target in %s/%s." % (jobname, timestamp))
#            send_message(
#                settings.QUEUE_HOST,
#                settings.WATCHED_QUEUE_NAME,
#                settings.WATCHED_QUEUE_KEY,
#                "%s/%s" % (jobname, timestamp)
#            )

@app.task()
def build_sip(job_id,launch_id):
    logger.info("Got sip build request")

@app.task()
def qa_job(job_id,launch_id):
    logger.info("Got job qa request")

def stop_running_job(frequency, heritrix):
    """Stops a running job, notifies RabbitMQ and cleans up the directory."""
    launchid = heritrix.launchid(frequency)
    message = "%s/%s" % (frequency, launchid)
    job = W3actJob.from_directory("%s/%s" % (HERITRIX_JOBS, frequency), heritrix=heritrix)
    job.stop()

    logger.info("Sending SIP message: %s" % message)
    build_sip.delay(frequency,launchid)

    logger.info("Sending QA message: %s" % message)
    qa_job.delay(frequency,launchid)

    remove_action_files(frequency)

@app.task()
def restart_job(frequency, start=datetime.utcnow()):
    """Restarts the job for a particular frequency."""
    logger.info("Restarting %s at %s" % (frequency, start))

    w = w3act(cfg.get('act','url'),cfg.get('act','username'),cfg.get('act','password'))

    export = w.get_ld_export(frequency)
    logger.debug("Found %s Targets in export." % len(export))
    targets = [t for t in export if (t["crawlStartDateISO"] is None or dateutil.parser.parse(t["crawlStartDateISO"]) < start) and (t["crawlEndDateISO"] is None or dateutil.parser.parse(t["crawlEndDateISO"]) > start)]
    logger.debug("Found %s Targets in date range." % len(targets))
    h = hapy.Hapy("https://%s:%s" % (cfg.get('h3','host'), cfg.get('h3','port')), username=cfg.get('h3','username'), password=cfg.get('h3','password'))
    #h = heritrix.API(host="https://%s:%s/engine" % (settings.HERITRIX_HOST, settings.HERITRIX_PORTS[frequency]), user="admin", passwd="bl_uk", verbose=False, verify=False)
    if frequency in h.listjobs() and h.status(frequency) != "":
        stop_running_job(frequency, h)
        #TODO: Automated QA
    job = W3actJob(targets, name=frequency, heritrix=h)
    logger.debug("Starting job %s with %s seeds." % (job.name, len(job.seeds)))
