#!/usr/bin/env python

"""Daemon which watches a configured queue for messages and for each, calls a
webservice, storing the result in a WARC file."""

import sys
import gzip
import json
import pika
import uuid
import shutil
import logging
import requests
import settings
from daemonize import Daemon
from datetime import datetime
from urlparse import urlparse
from hanzo.warctools import WarcRecord
from warcwriterpool import WarcWriterPool, warc_datetime_str

logger = logging.getLogger("harchiverd")
handler = logging.FileHandler(settings.LOG_FILE)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def write_outlinks(har, dir, parent):
    """Writes outlinks in the HAR to a gzipped file."""
    if dir is None:
        return
    j = json.loads(har)
    filename = "%s/%s.schedule.gz" % (dir, str(datetime.now().strftime("%s")))
    with gzip.open(filename, "wb") as o:
        for entry in j["log"]["entries"]:
            protocol = urlparse(entry["request"]["url"]).scheme
            if not protocol in settings.PROTOCOLS:
                continue
            referer = None
            for header in entry["request"]["headers"]:
                if header["name"].lower() == "referer":
                    referer = header["value"]
            if referer is not None:
                o.write("F+ %s E %s\n" % (entry["request"]["url"], referer))
            else:
                o.write("F+ %s\n" % entry["request"]["url"])

def send_amqp_message(message, client_id):
    """Send outlinks to AMQP."""
    parameters = pika.URLParameters(settings.AMQP_URL)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=settings.AMQP_EXCHANGE,
                             type="direct", 
                             durable=True, 
                             auto_delete=False)
    channel.queue_declare(queue=settings.AMQP_OUTLINK_QUEUE, 
                          durable=False, 
                          exclusive=False, 
                          auto_delete=True)
    channel.queue_bind(queue=settings.AMQP_OUTLINK_QUEUE, 
           exchange=settings.AMQP_EXCHANGE,
           routing_key=client_id)
    channel.basic_publish(exchange=settings.AMQP_EXCHANGE,
        routing_key=client_id,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
        body=message)
    channel.close()
    connection.close()

def amqp_outlinks(har, client_id, parent):
    """Passes outlinks back to queue."""
    har = json.loads(har)
    parent = json.loads(parent)
    for entry in har["log"]["entries"]:
        protocol = urlparse(entry["request"]["url"]).scheme
        if not protocol in settings.PROTOCOLS:
            continue
        message = {
            "url": entry["request"]["url"],
            "method": entry["request"]["method"],
            "headers": entry["request"]["headers"],
            "parentUrl": parent["url"],
            "parentUrlMetadata": parent["metadata"]
        }
        try:
            send_amqp_message(json.dumps(message), client_id)
        except:
            logger.error("Problem sending message: %s; %s" % (message, sys.exc_info()))

def handle_json_message(message):
    """Parses AMQPPublishProcessor-style JSON messages."""
    logger.info("Handling JSON message: %s" % message)
    selectors = [":root"]
    j = json.loads(message)
    url = j["url"]
    if "selectors" in j.keys():
        selectors += j["selectors"]
    return (url, j["clientId"], selectors, amqp_outlinks)

def handle_pipe_message(message):
    """Parses pipe-separated message."""
    logger.info("Handling pipe-separated message: %s" % message)
    url = None
    dir = None
    selectors = [":root"]
    parts = message.split("|")
    if len(parts) == 1:
        url = parts[0]
    elif len(parts) == 2:
        url, dir = parts
    else:
        url = parts[0]
        dir = parts[1]
        selectors += parts[2:]
    return (url, dir, selectors, write_outlinks)

def callback(warcwriter, body):
    """Parses messages, writing results to disk.

    Arguments:
    warcwriter -- A python-warcwriterpool instance.
    body -- The incoming message body.

    """
    try:
        logger.debug("Message received: %s." % body)
        if body.startswith("{"):
            (url, handler_id, selectors, url_handler) = handle_json_message(body)
        else:
            (url, handler_id, selectors, url_handler) = handle_pipe_message(body)

        ws = "%s/%s" % (settings.WEBSERVICE, url)
        logger.debug("Calling %s" % ws)
        r = requests.post(ws, data=json.dumps(selectors))
        if r.status_code == 200:
            # Handle outlinks, passing original message...
            har = r.content
            url_handler(har, handler_id, body)
            headers = [
                (WarcRecord.TYPE, WarcRecord.METADATA),
                (WarcRecord.URL, url),
                (WarcRecord.CONTENT_TYPE, "application/json"),
                (WarcRecord.DATE, warc_datetime_str(datetime.now())),
                (WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1()),
            ]
            warcwriter.write_record(headers, "application/json", har)
        else:
            logger.warning("None-200 response for %s; %s" % (body, r.content))
    except Exception as e:
        logger.error("%s [%s]" % (str(e), body))

class HarchiverDaemon(Daemon):
    """Maintains a connection to the queue."""
    def run(self):
        warcwriter = WarcWriterPool(gzip=True, output_dir=settings.OUTPUT_DIRECTORY)
        while True:
            try:
                logger.debug("Starting connection: %s" % (settings.AMQP_URL))
                parameters = pika.URLParameters(settings.AMQP_URL)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                channel.exchange_declare(exchange=settings.AMQP_EXCHANGE,
                                         type="direct", 
                                         durable=True, 
                                         auto_delete=False)
                channel.queue_declare(queue=settings.AMQP_QUEUE, 
                                      durable=True, 
                                      exclusive=False, 
                                      auto_delete=False)
                channel.queue_bind(queue=settings.AMQP_QUEUE, 
                       exchange=settings.AMQP_EXCHANGE,
                       routing_key=settings.AMQP_KEY)
                for method_frame, properties, body in channel.consume(settings.AMQP_QUEUE):
                    callback(warcwriter, body)
                    channel.basic_ack(method_frame.delivery_tag)
            except Exception as e:
                logger.error(str(e))
                requeued_messages = channel.cancel()
                logger.debug("Requeued %i messages" % requeued_messages)

