#!/usr/bin/env python
"""
Script to extract documents (currently == PDF) from a Heritrix job's WARCs.
Attempts to trace a route from every document to a seed within 3 hops.
"""

import argparse
import collections
import json
import logging
import ssl
from StringIO import StringIO
from glob import glob
from urlparse import urlparse, urldefrag

import rfc6266
from pywb.warc import cdxindexer
from pywb.warc.archiveiterator import DefaultRecordIter

import os
import pika
import re
import requests
from crawl.settings import *
from hanzo.warcpayload import FileHTTPResponse
from hanzo.warctools import WarcRecord
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from w3act.watched import validation

parser = argparse.ArgumentParser(description="Extract documents from WARCs.")
parser.add_argument("-t", "--test", dest="test", action="store_true", required=False, help="Test")
parser.add_argument("-n", "--no-validate", dest="novalidate", action="store_true", required=False, help="Don't validate documents")
parser.add_argument("-l", "--from-logs", dest="fromlogs", action="store_true", required=False, help="Build from crawl.log.")
args = parser.parse_args()

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)
logger = logging.getLogger("ddhapt")
logger.setLevel(logging.DEBUG)

logging.getLogger("").setLevel(logging.WARNING)


class ForceTLSV1Adapter(HTTPAdapter):
    """"Transport adapter" that forces TLSv1."""
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1)


class Dictlist(dict):
    """Allows multiple values for a single key."""
    def __setitem__(self, key, value):
        try:
            self[key]
        except KeyError:
            super(Dictlist, self).__setitem__(key, [])
        self[key].append(value)


def unique_list(input):
   keys = {}
   for e in input:
       keys[e] = 1
   return keys.keys()


class Backtracker(object):
    def __init__(self, inverted_index=None, crawled_uris=None, seeds=None, max_hops=3, patterns=None):
        self.inverted_index = inverted_index if inverted_index is not None else Dictlist()
        self.crawled_uris = crawled_uris if crawled_uris is not None else []
        self.seeds = seeds if seeds is not None else []
        self.patterns = patterns if patterns is not None else []
        self.max_hops = max_hops
        self.documents = None

    def build_from_warcs(self, warcs):
        for warc in warcs:
            fh = WarcRecord.open_archive(warc, gzip="auto")
            try:
                for (offset, record, errors) in fh.read_records(limit=None):
                    if record:
                        if record.type == WarcRecord.METADATA:
                            for line in StringIO(record.content[1]):
                                if line.startswith("outlink: "):
                                    outlink = line.strip().split()[1]
                                    self.inverted_index[outlink] = record.url
                        if record.type == WarcRecord.RESPONSE:
                            f = FileHTTPResponse(record.content_file)
                            f.begin()
                            if f.status == 200 and record.url.startswith("http"):
                                self.crawled_uris.append((record.url, f.getheader("content-type"), record.date, record.content_length))
                    elif errors:
                        pass
                    else:
                        pass
            finally:
                fh.close()

    def build_from_logs(self, logs):
        for log in logs:
            logger.debug("Building from %s" % log)
            with open(log, "rb") as i:
                for line in i:
                    fields = line.split()
                    uri = fields[3]
                    status = fields[1]
                    size = fields[2]
                    referrer = fields[5]
                    if status.isdigit() and 1 < int(status) < 400 and uri.startswith("http") and referrer.startswith("http"):
                        mime = fields[6]
                        date = str(eval(fields[8]))[0:14]
                        notes = fields[11]
                        self.inverted_index[uri] = referrer
                        if not "warcRevisit" in notes:
                            self.crawled_uris.append((uri, mime, date, size))

    def find_documents(self):
        if self.documents is None:
            documents = [u for (u, m, d, s) in self.crawled_uris for e in DOC_EXT if (m is not None and m != "warc/revisit") and (urlparse(u).path.lower().endswith(".%s" % e) or m.lower().endswith(e))]
            self.documents = unique_list(documents)
            logger.info("Found %s unique docs." % len(self.documents))
        return self.documents

    def find_routes(self, path):
        """Backtracks through referrers from a document to a seed."""
        if len(path) <= (self.max_hops + 1):
            branch = list(path)
            if branch[-1] in self.inverted_index.keys():
                for referrer in [r for r in self.inverted_index[branch[-1]] if r not in branch]:
                    #If we've reached a seed...hurrah!
                    if referrer in self.seeds:
                        yield branch + [referrer]
                        raise StopIteration
                    else:
                        for b in self.find_routes(branch + [referrer]):
                            yield b


def get_act_cookie():
    response = requests.post(URL_LOGIN, data={"email": EMAIL, "password": PASSWORD})
    return response.history[0].headers["set-cookie"]


def post_to_act(url, data):
    cookie = get_act_cookie()
    headers = {
        "Cookie": cookie,
        "Content-Type": "application/json"
    }
    return requests.post(url, data=data, headers=headers)


def get_act(url):
    cookie = get_act_cookie()
    headers = {
        "Cookie": cookie
    }
    return requests.get(url, headers=headers)


def get_filename(entry):
    """Find the 'content-disposition' filename of a WARC entry."""
    for ext in DOC_EXT:
        if entry["mime"].lower().endswith(ext) or urlparse(entry["url"]).path.lower().endswith(".%s" % (ext)):
            for header, value in entry.record.status_headers.headers:
                if header.lower() == "content-disposition":
                    cd = rfc6266.parse_headers(value)
                    return cd.filename_unsafe


def get_filenames(warcs):
    """Builds a URL->filename lookup from WARC files."""
    filenames = {}
    options = {"include_all": False, "surt_ordered": False}
    for fullpath, filename in cdxindexer.iter_file_or_dir(warcs):
        with open(fullpath, "rb") as warc:
            iter = DefaultRecordIter(**options)
            for entry in iter(warc):
                if not entry.record.status_headers.statusline.startswith("200"):
                    continue
                if entry.record.rec_type == "revisit":
                    continue
                pdf = get_filename(entry)
                if pdf is not None:
                    key = entry.pop("url")
                    filenames[key] = pdf
    return filenames


def get_message(host, queue):
    """Pulls a single message from a queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = connection.channel()
    method_frame, header_frame, body = channel.basic_get(queue)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        connection.close()
        return body
    connection.close()


if __name__ == "__main__":
    submitted = False
    message = get_message(QUEUE_HOST, WATCHED_QUEUE_NAME)
    while message is not None:
        logger.info("Processing: %s" % message)
        backtracker = Backtracker()
        if args.fromlogs:
            logs = glob("%s/%s/crawl.log*" % (LOG_ROOT, message))
            backtracker.build_from_logs(logs)
        else:
            warcs = glob("%s/%s/*.gz" % (WARC_ROOT, message))
            backtracker.build_from_warcs(warcs)

        # Build our filename lookup...
        filenames = get_filenames(glob("%s/%s/*.gz" % (WARC_ROOT, message)))

        # Find routes for each document in each watched target...
        logger.info("Reading info: %s/%s/w3act-info.json" % (HERITRIX_JOBS, message))
        with open("%s/%s/w3act-info.json" % (HERITRIX_JOBS, message), "rb") as i:
            info = i.read()
        watched = [w for w in json.loads(info) if "watched" in w.keys() and w["watched"]]
        # Keep track of submitted docs...
        for w in watched:
            w["submission_count"] = 0

        for t in watched:
            seeds = list(t["seeds"])
            for s in seeds:
                # Need a non-fragment copy of URLs...
                url, frag = urldefrag(s)
                if len(frag) != 0:
                    t["seeds"] += [url]
                # Always need that trailing slash...
                pr = urlparse(s)
                if pr.path == "":
                    t["seeds"] += ["%s/" % s]

        backtracker.seeds = [urldefrag(s)[0] for t in watched for s in t["seeds"]]
        backtracker.patterns = [t["watchedTarget"]["documentUrlScheme"] for t in watched]

        docs = []
        validator = validation.Validator()
        for pdf in backtracker.find_documents():
            routes = [r for r in backtracker.find_routes([pdf]) if r is not None]
            if len(routes) == 0:
                # Increase scope until we find a route...
                max_hops = backtracker.max_hops
                while len(routes) == 0 and backtracker.max_hops < 20:
                    backtracker.max_hops += 1
                    logger.debug("Increasing hop-count to %s." % backtracker.max_hops)
                    routes = [r for r in backtracker.find_routes([pdf]) if r is not None]
                backtracker.max_hops = max_hops
                if len(routes) == 0:
                    logger.warning("Could not find route: %s" % pdf)
                    continue
            landing_pages = collections.Counter([r[1] for r in routes])
            landing_page, count = landing_pages.most_common(1)[0]

            # Find shorted routes...
            routes.sort(key=lambda x: len(x))
            shortest_routes = [r for r in routes if len(r) == len(routes[0])]
            # Make sure routes are unique...
            shortest_routes = [list(x) for x in set(tuple(x) for x in shortest_routes)]

            seeds_routed = []
            for route in shortest_routes:
                seed = route[-1]

                target = [t for t in watched if seed in t["seeds"]][0]
                pattern = target["watchedTarget"]["documentUrlScheme"]
                if pattern is not None and pattern not in pdf:
                    logger.debug("Document %s does not match pattern %s." % (pdf, pattern))
                    continue

                target["submission_count"] += 1

                # Only 1 route necessary per seed...
                if seed in seeds_routed:
                    continue
                else:
                    seeds_routed.append(seed)
                doc = {}
                doc["id_watched_target"] = target["watchedTarget"]["id"]
                doc["wayback_timestamp"] = [re.sub("[^0-9]", "", d) for (u, m, d, s) in backtracker.crawled_uris if u == pdf][0]
                doc["landing_page_url"] = landing_page
                doc["document_url"] = pdf
                try:
                    doc["filename"] = filenames[pdf]
                except KeyError:
                    doc["filename"] = os.path.basename(urlparse(pdf).path)
                doc["size"] = long([s for (u, m, d, s) in backtracker.crawled_uris if u == pdf][0])
                if args.novalidate or validator.validate(target, doc):
                    docs.append(doc)
                    submitted = True
                    logger.info(json.dumps(doc, indent=4))
                    if not args.test:
                        response = post_to_act(WATCHED_TARGET_URL, json.dumps([doc]))
        for non_found in [t for t in watched if t["submission_count"] == 0]:
            # Update DDHAPT for non-submitted docs.
            nodocuments = "%s/watchedtargets/%s/nodocuments" % (URL_BASE, t["watchedTarget"]["id"])
            response = requests.get(nodocuments)
        message = get_message(QUEUE_HOST, WATCHED_QUEUE_NAME)
    if submitted:
        r = get_act(DOCUMENT_CONVERSION_URL)


