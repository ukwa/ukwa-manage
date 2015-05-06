#!/usr/bin/env python
"""
Adds new seeds to already-running jobs.
"""

import os
import sys
import json
import w3act
import shutil
import logging
import argparse
import heritrix
import requests
import traceback
from glob import glob
import dateutil.parser
from w3act import settings
from datetime import datetime

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
parser.add_argument("-x", "--test", dest="test", action="store_true", required=False, help="Test")
args = parser.parse_args()


def add_seeds(job, seeds):
    """Adds seeds to a job's Action directory."""
    if len(seeds) > 0:
        filename = "%s.seeds" % datetime.now().strftime("%Y%m%d%H%M%S")
        tmp = "/tmp/%s" % filename
        action = "%s/%s/action/%s" % (settings.HERITRIX_JOBS, job, filename)
        if not args.test:
            with open(tmp, "wb") as o:
                o.write(str(unicode("\n".join(seeds)).encode("utf-8")))
            shutil.move(tmp, action)
            logger.info("Added %s seeds to %s crawl." % (len(seeds), job))
        else:
            logger.info("Would add %s seeds to %s crawl." % (len(seeds), job))


def get_action_seeds(job):
    """Retrieves all seeds added via Action directory."""
    seeds = []
    for done in glob("%s/%s/action/done/*.seeds" % (settings.HERITRIX_JOBS, job)):
        with open(done, "rb") as i:
            seeds += [l.strip().decode("utf-8") for l in i]
    return seeds


def get_new_seeds(frequencies, start):
    """Determines new seeds for each frequency."""
    try:
        w = w3act.ACT()
        for frequency in frequencies:
            export = w.get_ld_export(frequency)
            # Only interested in Targets with definitive start-dates...
            targets = [t for t in export if (t["crawlStartDateText"] is not None and dateutil.parser.parse(t["crawlStartDateText"], dayfirst=True) < start) and (t["crawlEndDateText"] is None or dateutil.parser.parse(t["crawlEndDateText"], dayfirst=True) > start)]
            all_seeds = [u["url"] for t in targets for u in t["fieldUrls"]]
            h = heritrix.API(host="https://%s:%s/engine" % (settings.HERITRIX_HOST, settings.HERITRIX_PORTS[frequency]), user="admin", passwd="bl_uk", verbose=False, verify=False)
            current_seeds = h.seeds(frequency) + get_action_seeds(frequency)
            new_seeds = list(set(all_seeds) - set(current_seeds))
            add_seeds(frequency, new_seeds)
    except:
        logger.error("%s: %s" % (frequency, str(sys.exc_info())))
        logger.error("%s: %s" % (frequency, traceback.format_exc()))
        

if __name__ == "__main__":
    get_new_seeds(args.frequency, dateutil.parser.parse(args.timestamp).replace(tzinfo=None))

