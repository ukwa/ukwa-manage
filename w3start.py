#!/usr/bin/env python
"""
Restarts Heritrix jobs depending on the current time and the frequency of the job.
"""

import os
import sys
import json
import w3act
import logging
import argparse
import heritrix
import requests
import tempfile
import traceback
from glob import glob
import dateutil.parser
from w3act import settings
from slacker import Slacker
from datetime import datetime
from w3act.job import W3actJob
from w3act.w3actd import send_message
from w3act.util import generate_log_stats, stats_to_csv

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger("w3act.%s" % __name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

# Log to /var/log/...
var_log_handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
var_log_handler.setFormatter(formatter)
logger.addHandler(var_log_handler)

# Log to stdout...
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

parser = argparse.ArgumentParser(description="Restarts Heritrix jobs.")
parser.add_argument("-t", "--timestamp", dest="timestamp", type=str, required=False, help="Timestamp", default=datetime.now().isoformat())
parser.add_argument("-f", "--frequency", dest="frequency", type=str, required=False, help="Frequency", nargs="+", default=settings.FREQUENCIES)
parser.add_argument("-x", "--test", dest="test", action="store_true", required=False, help="Test")
args = parser.parse_args()

def remove_action_files(jobname):
    """Removes old 'action' files and symlinks."""
    actions_done = "%s/%s/latest/actions-done" % (settings.HERITRIX_JOBS, jobname)
    done = "%s/%s/action/done" % (settings.HERITRIX_JOBS, jobname)
    for root in [actions_done, done]:
        if os.path.exists(root):
            to_remove = glob("%s/*" % root)
            logger.info("Removing %s action files." % len(to_remove))
            for action in to_remove:
                os.remove(action)

def send_slack_messages(stats, name):
    messages = {}
    messages["json"] = json.dumps(stats, indent=4)
    if settings.SLACK_CSV:
        messages["csv"] = stats_to_csv(stats)
    slack = Slacker(settings.SLACK_TOKEN)
    for extension, data in messages.iteritems():
        output = "%s/%s.%s" % (tempfile.gettempdir(), name, extension)
        with open(output, "wb") as o:
            o.write(data)
        res = slack.files.upload(output, channels=settings.SLACK_CHANNEL, filename="%s-%s.log" % (name, datetime.now().strftime("%Y%m%d%H%M%S")), title=name)

def stop_running_job(frequency, heritrix):
    """Stops a running job, notifies RabbitMQ and cleans up the directory."""
    launchid = heritrix.launchid(frequency)
    message = "%s/%s" % (frequency, launchid)
    job = W3actJob.from_directory("%s/%s" % (settings.HERITRIX_JOBS, frequency), heritrix=heritrix)
    job.stop()
    logger.info("Sending SIP message: %s" % message)
    send_message(
        settings.QUEUE_HOST,
        settings.SIP_QUEUE_NAME,
        settings.SIP_QUEUE_KEY,
        message
    )
    logger.info("Sending QA message: %s" % message)
    send_message(
        settings.QUEUE_HOST,
        settings.QA_QUEUE_NAME,
        settings.QA_QUEUE_KEY,
        message
    )
    remove_action_files(frequency)
    if settings.SLACK:
        stats = generate_log_stats(glob("%s/%s/%s/crawl.log*" % (settings.HERITRIX_LOGS, frequency, launchid)))
        send_slack_messages(stats, frequency)

def restart_job(frequency, start=datetime.now()):
    """Restarts the job for a particular frequency."""
    logger.info("Restarting %s at %s" % (frequency, start))
    try:
        w = w3act.ACT()
        export = w.get_ld_export(frequency)
        logger.debug("Found %s Targets in export." % len(export))
        targets = [t for t in export if (t["crawlStartDateText"] is None or dateutil.parser.parse(t["crawlStartDateText"], dayfirst=True) < start) and (t["crawlEndDateText"] is None or dateutil.parser.parse(t["crawlEndDateText"], dayfirst=True) > start)]
        logger.debug("Found %s Targets in date range." % len(targets))
        h = heritrix.API(host="https://%s:%s/engine" % (settings.HERITRIX_HOST, settings.HERITRIX_PORTS[frequency]), user="admin", passwd="bl_uk", verbose=False, verify=False)
        if frequency in h.listjobs() and h.status(frequency) != "":
            stop_running_job(frequency, h)
            #TODO: Automated QA
        job = W3actJob(targets, name=frequency, heritrix=h)
        if not args.test:
            logger.debug("Starting job %s with %s seeds." % (job.name, len(job.seeds)))
            job.start()
    except:
        logger.error("%s: %s" % (frequency, str(sys.exc_info())))
        logger.error("%s: %s" % (frequency, traceback.format_exc()))
    
def restart_frequencies(frequencies, now):
    """Restarts jobs depending on the current time."""
    if now.hour == settings.JOB_RESTART_HOUR:
        if "daily" in frequencies:
            restart_job("daily", start=now)
        if now.isoweekday() == settings.JOB_RESTART_WEEKDAY:
            if "weekly" in frequencies:
                restart_job("weekly", start=now)
        if now.day == settings.JOB_RESTART_DAY:
            if "monthly" in frequencies:
                restart_job("monthly", start=now)
            if now.month%3 == 1:
                if "quarterly" in frequencies:
                    restart_job("quarterly", start=now)
            if now.month%6 == 1:
                if "sixmonthly" in frequencies:
                    restart_job("sixmonthly", start=now)
            if now.month == settings.JOB_RESTART_MONTH:
                if "annual" in frequencies:
                    restart_job("annual", start=now)

if __name__ == "__main__":
    restart_frequencies(args.frequency, dateutil.parser.parse(args.timestamp).replace(tzinfo=None))

