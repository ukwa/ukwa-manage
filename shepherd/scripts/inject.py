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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shepherd.lib.launch import KafkaLauncher

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
    launcher.launch(args.destination, uri, args.source, isSeed=args.seed, clientId="FC-3-uris-to-crawl",
                    forceFetch=args.forceFetch, sendCheckMessage=False)

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
                launcher.launch(args.destination, page_uri, args.source, isSeed=False, clientId="FC-3-uris-to-crawl",
                                forceFetch=args.forceFetch)


def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-a', '--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@127.0.0.1:5672/%2f",
                        help="AMQP endpoint to use [default: %(default)s]")
    parser.add_argument('-e', '--exchange', dest='exchange',
                        type=str, default="heritrix",
                        help="Name of the exchange to use (defaults to heritrix).")
    parser.add_argument("-d", "--destination", dest="destination", type=str, default='h3',
                        help="Destination, implying message format to use: 'har' or 'h3'. [default: %(default)s]")
    parser.add_argument("-s", "--source", dest="source", type=str, default='',
                        help="Source tag to attach to this URI, if any. [default: %(default)s]")
    parser.add_argument("-S", "--seed", dest="seed", action="store_true", default=False, required=False,
                        help="Treat supplied URI as a seed, thus widening crawl scope. [default: %(default)s]")
    parser.add_argument("-F", "--force-fetch", dest="forceFetch", action="store_true", default=False, required=False,
                        help="Force the URL to be fetched, even if already seen and queued/rejected. [default: %(default)s]")
    parser.add_argument("-P", "--pager", dest="pager", action="store_true", default=False, required=False,
                        help="Attempt to extract URLs for all pages, and submit those too.")
    parser.add_argument('queue', metavar='queue', help="Name of queue to send seeds to.")
    parser.add_argument('uri_or_filename', metavar='uri_or_filename', help="URI to enqueue, or filename containing URIs to enqueue.")

    args = parser.parse_args()

    # Set up launcher:
    launcher = KafkaLauncher(args)

    # Read from a file:
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
