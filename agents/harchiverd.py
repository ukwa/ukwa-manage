#!/usr/bin/env python

"""Process which watches a configured queue for messages and for each, calls a
webservice, storing the result in a WARC file."""

import sys
import json
import pika
import uuid
import logging
import requests
import time
import argparse
from datetime import datetime
from urlparse import urlparse
from hanzo.warctools import WarcRecord
from warcwriterpool import WarcWriterPool, warc_datetime_str

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s@%(lineno)d : %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.handlers = []
logging.root.addHandler( handler )

# Default log level:
logging.getLogger().setLevel(logging.WARNING)
logging.getLogger( 'pika' ).setLevel(logging.ERROR)

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )

#
def send_amqp_message(message, client_id):
    """Send outlinks to AMQP."""
    parameters = pika.URLParameters(settings.amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=settings.exchange,
                             type="direct", 
                             durable=True, 
                             auto_delete=False)
    channel.queue_declare(queue=client_id,
                          durable=True, 
                          exclusive=False, 
                          auto_delete=False)
    channel.queue_bind(queue=client_id,
           exchange=settings.exchange,
           routing_key=client_id)
    channel.basic_publish(exchange=settings.exchange,
        routing_key=client_id,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
        body=message)
    connection.close()

def send_to_amqp(client_id, url,method,headers, parentUrl, parentUrlMetadata, forceFetch=False, isSeed=False):
    sent = False
    message = {
        "url": url,
        "method": method,
        "headers": headers,
        "parentUrl": parentUrl,
        "parentUrlMetadata": parentUrlMetadata
    }
    if forceFetch:
        message["forceFetch"] = True
    if isSeed:
        message["isSeed"] = True
    logger.debug("Sending message: %s" % message)
    while not sent:
        try:
            send_amqp_message(json.dumps(message), client_id)
            sent = True
        except:
            logger.error("Problem sending message: %s; %s" % (message, sys.exc_info()))
            logger.error("Sleeping for 30 seconds...")
            time.sleep(30)


def amqp_outlinks(har, client_id, parent):
    """Passes outlinks back to queue."""
    har = json.loads(har)
    parent = json.loads(parent)
    embeds = 0
    for entry in har["log"]["entries"]:
        protocol = urlparse(entry["request"]["url"]).scheme
        if not protocol in settings.protocols:
            continue
        embeds = embeds + 1
        send_to_amqp(client_id, entry["request"]["url"],entry["request"]["method"], 
            {h["name"]: h["value"] for h in entry["request"]["headers"]}, 
            parent["url"], parent["metadata"], forceFetch=True)
    links = 0
    for entry in har["log"]["pages"]:
        for item in entry["map"]:
            # Some map regions are JavaScript rather than direct links, so only take the links:
            if 'href' in item:
                links = links + 1
                send_to_amqp(client_id, item['href'],"GET", {}, parent["url"], parent["metadata"])
    logger.info("Queued %i embeds and %i links for url '%s'." % (embeds, links, parent["url"]) )


def handle_json_message(message):
    """Parses AMQPPublishProcessor-style JSON messages."""
    logger.debug("Handling JSON message: %s" % message)
    selectors = [":root"]
    j = json.loads(message)
    url = j["url"]
    if "selectors" in j.keys():
        selectors += j["selectors"]
    return (url, j["clientId"], selectors, amqp_outlinks)

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
            logger.error("Cannot parse message: %s" %body )
            
        logger.debug("Rendering %s" % url )
        start_time = time.time()
        ws = "%s/%s" % (settings.webrender_url, url)
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
            end_time = time.time()
            logger.debug("Rendered and recorded output for %s in %d seconds." %(url, end_time-start_time))
        else:
            logger.warning("None-200 response for %s; %s" % (body, r.content))
        return True
    except Exception as e:
        logger.error("%s [%s]" % (str(e), body))

def run_harchiver():
    """Maintains a connection to the queue."""

    while True:
        channel = None
        try:
            logger.info("Setting up warc writer, in %s" % settings.output_directory)
            warcwriter = WarcWriterPool(gzip=True, output_dir=settings.output_directory)
            logger.info("Starting connection: %s" % (settings.amqp_url))
            parameters = pika.URLParameters(settings.amqp_url)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange=settings.exchange,
                                     type="direct", 
                                     durable=True, 
                                     auto_delete=False)
            channel.queue_declare(queue=settings.in_queue, 
                                  durable=True, 
                                  exclusive=False, 
                                  auto_delete=False)
            channel.queue_bind(queue=settings.in_queue, 
                   exchange=settings.exchange,
                   routing_key=settings.binding_key)
            logger.info("Started connection: %s" % (settings.amqp_url))
            for method_frame, properties, body in channel.consume(settings.in_queue):
                handled = callback(warcwriter, body)
                if handled:
                    channel.basic_ack(method_frame.delivery_tag)
                
        except Exception as e:
            logger.error("Error: %s" % e)
            if channel and channel.is_open and not channel.is_closing:
                try:
                    requeued_messages = channel.cancel()
                    logger.info("Requeued %i messages" % requeued_messages)
                except Exception as e:
                    logger.warning("Could not cancel/shutdown neatly.")
            if warcwriter:
                warcwriter.cleanup()
            logger.warning("Sleeping for 15 seconds before retrying...")
            time.sleep(15)
        except KeyboardInterrupt:
            # Tidy up:
            if warcwriter:
                warcwriter.cleanup()
            # quit
            sys.exit()            

if __name__ == "__main__":
    # Parse CLARGS
    parser = argparse.ArgumentParser('')
    parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
        help="AMQP endpoint to use (defaults to amqp://guest:guest@localhost:5672/%%2f)" )
    parser.add_argument('--webrender-url', dest='webrender_url', type=str, default="http://webrender:8000/webtools/domimage", 
        help="HAR webrender endpoint to use (defaults to http://webrender:8000/webtools/domimage" )
    parser.add_argument('--num', dest='qos_num', 
        type=int, default=10, help="Maximum number of messages to handle at once, (defaults to 10)")
    parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
    parser.add_argument('in_queue', metavar='in_queue', help="Name of queue to view messages from.")
    parser.add_argument('binding_key', metavar='binding_key', help="Name of binding_key for the input queue.")
    parser.add_argument('output_directory', metavar='out_dir', help="Directory to write HAR output WARCs into.")
    
    settings = parser.parse_args()
    
    settings.protocols = ['http', 'https']
    settings.log_level = 'INFO'
        
    # Report settings:
    logger.info("log_level = %s", settings.log_level)
    logger.info("output_directory = %s", settings.output_directory)
    logger.info("webrender_url = %s", settings.webrender_url)
    logger.info("protocols = %s", settings.protocols)
    logger.info("AMQP_URL = %s", settings.amqp_url)
    logger.info("AMQP_EXCHANGE = %s", settings.exchange)
    logger.info("AMQP_QUEUE = %s", settings.in_queue)
    logger.info("AMQP_BINDING_KEY = %s", settings.binding_key)

    # ...and run:
    run_harchiver()

