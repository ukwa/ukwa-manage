#!/usr/bin/env python
"""
Restarts Heritrix jobs depending on the current time and the frequency of the job.
"""

import sys
import json
import w3act
import logging
import argparse
import heritrix
import requests
import traceback
import dateutil.parser
from w3act import settings
from datetime import datetime
from w3act.job import W3actJob

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger("w3act.%s" % __name__)
handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


parser = argparse.ArgumentParser(description="Restarts Heritrix jobs.")
parser.add_argument("-t", "--timestamp", dest="timestamp", type=str, required=False, help="Timestamp", default=datetime.now().isoformat())
parser.add_argument("-f", "--frequency", dest="frequency", type=str, required=False, help="Frequency", nargs="+", default=settings.FREQUENCIES)
parser.add_argument("-n", "--no-check", dest="no_check", action="store_true", required=False, help="No QA")
parser.add_argument("-x", "--test", dest="test", action="store_true", required=False, help="Test")
args = parser.parse_args()


def restart_job(frequency, start=datetime.now()):
    """Restarts the job for a particular frequency."""
    logger.info("Restarting %s at %s" % (frequency, start))
    try:
        w = w3act.ACT()
        export = w.get_ld_export(frequency)
        logger.debug("Found %s Targets in export." % len(export))
        targets = [t for t in export if dateutil.parser.parse(t["crawlStartDateText"], dayfirst=True) < start and (t["crawlEndDateText"] is None or dateutil.parser.parse(t["crawlEndDateText"], dayfirst=True) > start)]
        logger.debug("Found %s Targets in date range." % len(targets))
        h = heritrix.API(host="https://%s:%s/engine" % (settings.HERITRIX_HOST, settings.HERITRIX_PORTS[frequency]), user="admin", passwd="bl_uk", verbose=False, verify=False)
        if h.status(frequency) != "":
            job = W3actJob.from_directory("%s/%s" % (settings.HERITRIX_JOBS, frequency), heritrix=h)
            job.stop()
            #TODO: Automated QA
        job = w3act.W3actJob(targets, name=frequency, heritrix=h)
        if not args.test:
            logger.debug("Starting job %s with %s seeds." % (job.name, len(job.seeds)))
            job.start()
    except:
        logger.error("%s: %s" % (frequency, str(sys.exc_info())))
        logger.error("%s: %s" % (frequency, traceback.format_exc()))
    

def restart_frequencies(frequencies, now):
    """Restarts jobs depending on the current time."""
    for frequency in frequencies:
        if now.hour == settings.JOB_RESTART_HOUR:
            restart_job("daily", start=now)
            if now.isoweekday() == settings.JOB_RESTART_WEEKDAY:
                restart_job("weekly", start=now)
                if now.day == settings.JOB_RESTART_DAY:
                    restart_job("monthly", start=now)
                    if now.month%3 == 0:
                        restart_job("quarterly", start=now)
                    if now.month%6 == 0:
                        restart_job("sixmonthly", start=now)
                    if now.month == settings.JOB_RESTART_MONTH:
                        restart_job("annual", start=now)

if __name__ == "__main__":
    restart_frequencies(args.frequency, dateutil.parser.parse(args.timestamp).replace(tzinfo=None))
