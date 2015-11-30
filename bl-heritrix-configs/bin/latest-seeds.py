#!/usr/bin/env python
"""Script to retrieve recently-added URLs from ACT and add them to a crawl."""

import sys
import json
import shutil
import logging
import argparse
import requests
from settings import *
from retry_decorator import *
from datetime import datetime
from requests.exceptions import ConnectionError

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.WARNING)
logger = logging.getLogger("latest-seeds")

parser = argparse.ArgumentParser(description="Retrieve latest seeds from ACT and ")
parser.add_argument("-i", dest="input", type=str, required=False)
args = parser.parse_args()

ROOT = "/opt/heritrix/jobs/latest-20130809093642"
LATEST = "%s/latest.txt" % ROOT
SEEDS = "%s/seeds.txt" % ROOT
SEEDS_ACTION = "%s/action/%s.seeds" % (ROOT, datetime.now().strftime("%Y%m%d%H%M%S"))


@retry((ConnectionError, ValueError, IndexError), tries=5, timeout_secs=600)
def callAct(url):
    response = requests.post(URL_LOGIN, data={"email": EMAIL, "password": PASSWORD})
    cookie = response.history[0].headers["set-cookie"]
    headers = {
        "Cookie": cookie
    }
    r = requests.get(url, headers=headers)
    return r.content


def json_to_urls(js):
    """Parses JSON and returns a list of URLs therein."""
    latest = set()
    for node in json.loads(js):
        for url in [u["url"] for u in node["fieldUrls"]]:
            latest.add(url)
    return latest
    

def get_act_urls():
    """Generates a list of all URLs in ACT."""
    latest = set()
    js = None
    if args.input:
        logger.info("Reading %s." % args.input)
        with open(args.input, "r") as i:
            js = i.read()
        latest.update(json_to_urls(js))
    else:
        logger.info("Requesting /all")
        js = callAct("%s%s" % (URL_ROOT, "all"))
        latest.update(json_to_urls(js))
    latest = list(latest)
    return latest

def get_last_seeds():
    """Retrieves the last list of seeds from disk."""
    with open(LATEST, "r") as s:
        seeds = [line.rstrip() for line in s]
    return seeds

def get_new_urls(latest, disk):
    """Compares the current list if URLs from ACT to those on disk."""
    for url in disk:
        if url in latest:
            latest.remove(url)
    return latest

def write_new_urls(new):
    """Writes new URLs to disk; moves them to the Action Directory."""
    with open(SEEDS, "w") as s:
        for n in new:
            s.write("%s\n" % n.encode("utf-8"))
        #s.write("\n".join(new))
    shutil.copyfile(SEEDS, SEEDS_ACTION)

def write_last_seeds(last):
    """Writes the complete list of 'latest' seeds to disk."""
    with open(LATEST, "w") as l:
        for u in last:
            l.write("%s\n" % u.encode("utf-8"))
        #l.write("\n".join(last))

if __name__ == "__main__":
    try:
        latest = get_act_urls()
    except requests.exceptions.Timeout, t:
        logger.error(str(t))
        sys.exit(1)
    logger.info("%s URLs found in ACT." % len(latest))
    last_seeds = get_last_seeds()
    logger.info("%s URLs found from last run." % len(last_seeds))
    new = get_new_urls(latest, last_seeds)
    logger.info("%s new URLs found." % len(new))
    if len(new) > 0:
        write_new_urls(new)
    write_last_seeds(latest)

