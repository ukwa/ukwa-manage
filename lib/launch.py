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
import json
import logging
import argparse
import requests
from datetime import datetime
import mmh3
import binascii
import struct
from urlparse import urlparse
from lxml import html
from kafka import KafkaProducer


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


class KafkaLauncher(object):
    '''
    classdocs
    '''

    def __init__(self, args):
        '''
        Constructor
        '''
        self.args = args
        self.producer = KafkaProducer(
            bootstrap_servers=self.args.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_message(self, key, message, queue=None):
        """
        Sends a message to the given queue.
        """
        #
        if not queue:
            queue = self.args.queue

        logger.info("Sending key %s, message: %s" % (key, json.dumps(message)))
        self.producer.send(queue, key=key, value=message)

    def launch(self, destination, uri, source, isSeed=False, forceFetch=False, sheets=[], hop="", recrawl_interval=None):
        curim = {}
        if destination == "h3":
            curim['headers'] = {}
            # curim['headers']['User-Agent'] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.120 Chrome/37.0.2062.120 Safari/537.36"
            curim['method'] = "GET"
            curim['parentUrl'] = uri
            curim['parentUrlMetadata'] = {}
            curim['parentUrlMetadata']['pathFromSeed'] = ""
            curim['parentUrlMetadata']['heritableData'] = {}
            curim['parentUrlMetadata']['heritableData']['source'] = source
            curim['parentUrlMetadata']['heritableData']['heritable'] = ['source', 'heritable']
            curim['isSeed'] = isSeed
            if not isSeed:
                curim['forceFetch'] = forceFetch
            curim['url'] = uri
            curim['hop'] = hop
            if len(sheets) > 0:
                curim['sheets'] = sheets
            if recrawl_interval:
                curim['recrawlInterval'] = recrawl_interval
            curim['timestamp'] = datetime.now().isoformat()
        elif destination == "har":
            curim['clientId'] = "unused"
            curim['metadata'] = {}
            curim['metadata']['heritableData'] = {}
            curim['metadata']['heritableData']['heritable'] = ['source', 'heritable']
            curim['metadata']['heritableData']['source'] = source
            curim['metadata']['pathFromSeed'] = ""
            curim['isSeed'] = isSeed
            if not isSeed:
                curim['forceFetch'] = forceFetch
            curim['url'] = uri
        else:
            logger.error("Can't handle destination type '%s'" % destination)

        # Determine the key, hashing the 'authority' (should match Java version):
        key = binascii.hexlify(struct.pack("<I", mmh3.hash(urlparse(uri).netloc, signed=False)))

        # Push a 'seed' message onto the rendering queue:
        self.send_message(key, curim)

    def flush(self):
        self.producer.flush()


def sender(launcher, args, uri):
    # Ensure a http:// or https:// at the front:
    if not (uri.startswith("http://") or uri.startswith("https://")):
        uri = "http://%s" % uri

    # Add the main URL
    launcher.launch(args.destination, uri, args.source, isSeed=args.seed, forceFetch=args.forceFetch,
                    recrawl_interval=args.recrawl_interval)

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
    parser.add_argument("-r", "--recrawl-interval", dest="recrawl_interval", default=None, required=False, type=int,
                        help="Recrawl interval override for this URI (in seconds). [default: %(default)s]")
    parser.add_argument('queue', metavar='queue', help="Name of queue to send URIs too, e.g. 'dc.discovered'.")
    parser.add_argument('uri_or_filename', metavar='uri_or_filename', help="URI to enqueue, or filename containing URIs to enqueue.")

    args = parser.parse_args()

    # Set up launcher:
    launcher = KafkaLauncher(args)

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
        sender(launcher, args, args.uri_or_filename,)

    # Wait for send to complete:
    launcher.flush()


if __name__ == "__main__":
    sys.exit(main())
