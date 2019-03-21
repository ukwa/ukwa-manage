#!/usr/bin/env python
# encoding: utf-8
'''
agents.launch -- Feeds URIs into queues

@author:     Andrew Jackson

@copyright:  2016 The British Library.

@license:    Apache 2.0

@contact:    Andrew.Jackson@bl.uk
@deffield    updated: 2016-01-16
'''

import os
import sys
import time
import logging
import argparse
import requests
from lxml import html
from lib.enqueue import KafkaLauncher


# Set up a logging handler:
handler = logging.StreamHandler()
# handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
handler.setFormatter(formatter)

# Attach to root logger
logging.root.addHandler(handler)

# Set default logging output for all modules.
logging.root.setLevel(logging.WARNING)

# Set logging for this module and keep the reference handy:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def sender(launcher, args, uri):
    # Ensure a http:// or https:// at the front:
    if not (uri.startswith("http://") or uri.startswith("https://")):
        uri = "http://%s" % uri

    # Add the main URL
    launcher.launch("h3", uri, args.source, isSeed=args.seed, forceFetch=args.forceFetch,
                    recrawl_interval=args.recrawl_interval, sheets=args.sheets, reset_quotas=args.reset_quotas,
                    webrender_this=args.webrender_this, launch_ts=args.launch_ts)

    # Also, for some hosts, attempt to extract all pages from a oaged list:
    if args.pager:
        if uri.startswith("https://www.gov.uk/government/publications"):
            r = requests.get(uri)
            h = html.fromstring(r.content)
            # Extract the page range:
            span = h.xpath("//span[@class='page-numbers']/text()")[0]
            logger.info("Extracting pager URLs: %s" % span)
            last = int(span.split()[-1]) + 1
            for i in range(2, last):
                page_uri = "%s&page=%i" % (uri, i)
                launcher.launch(args.destination, page_uri, args.source, isSeed=False,
                                forceFetch=args.forceFetch)


def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='kafka_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument("-t", "--sheets", dest="sheets", type=str, default='',
                        help="Comma-separated list of sheets to apply for this URL. [default: %(default)s]")
    parser.add_argument("-s", "--source", dest="source", type=str, default='',
                        help="Source tag to attach to this URI, if any. [default: %(default)s]")
    parser.add_argument("-S", "--seed", dest="seed", action="store_true", default=False, required=False,
                        help="Treat supplied URI as a seed, thus widening crawl scope. [default: %(default)s]")
    parser.add_argument("-F", "--force-fetch", dest="forceFetch", action="store_true", default=False, required=False,
                        help="Force the URL to be fetched, even if already seen and queued/rejected. [default: %(default)s]")
    parser.add_argument("-P", "--pager", dest="pager", action="store_true", default=False, required=False,
                        help=argparse.SUPPRESS ) #"Attempt to extract URLs for all pages, and submit those too.")
    parser.add_argument("-r", "--recrawl-interval", dest="recrawl_interval", default=None, required=False, type=int,
                        help="Recrawl interval override for this URI (in seconds). [default: %(default)s]")
    parser.add_argument("-R", "--reset-quotas", dest="reset_quotas", action="store_true", default=False, required=False,
                        help="Reset the crawl quotas, setting crawled totals to zero. [default: %(default)s]")
    parser.add_argument("-W", "--webrender-this", dest="webrender_this", action="store_true", default=False, required=False,
                        help="Render this URI in a browser rather than using the usual downloader (always True for seeds). [default: %(default)s]")
    parser.add_argument("-L", "--launch-ts", dest="launch_ts", default=None, required=False, type=str,
                        help="Launch request timestamp as 14-character datetime e.g. '20190301120000' or use 'now' to use the current time. [default: %(default)s]")
    parser.add_argument('queue', metavar='queue', help="Name of queue to send URIs too, e.g. 'dc.discovered'.")
    parser.add_argument('uri_or_filename', metavar='uri_or_filename', help="URI to enqueue, or filename containing URIs to enqueue.")

    args = parser.parse_args()

    # Expand sheets into an array:
    if args.sheets:
        args.sheets = args.sheets.split(',')

    # Set up launcher:
    launcher = KafkaLauncher(kafka_server=args.kafka_server, topic=args.queue)

    # Read from a file, if the input is a file:
    if os.path.isfile(args.uri_or_filename):
        with open(args.uri_or_filename,'r') as f:
            for line in f:
                sent = False
                while not sent:
                    try:
                        uri = line.strip()
                        sender(launcher, args, uri)
                        sent = True
                    except Exception as e:
                        logger.error("Exception while submitting: %s" % line)
                        logger.exception(e)
                        logger.info("Sleeping for ten seconds...")
                        time.sleep(10)
    else:
        # Or send one URI
        sender(launcher, args, args.uri_or_filename)

    # Wait for send to complete:
    launcher.flush()


if __name__ == "__main__":
    sys.exit(main())
