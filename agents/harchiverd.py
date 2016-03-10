#!/usr/bin/env python
# encoding: utf-8

"""Process which watches a configured queue for messages and for each, calls a
webservice, storing the result in a WARC file.

JSON input uses 'clientId' to specify onward link routing key, and 'selectors' array to specify DOM selectors to render 
(as well as ':root' which should always rendered by the rendering service.)

{
    "clientId": "ukwa-test-crawl",
    "metadata": {
        "heritableData": {
            "heritable": [
                "source",
                "heritable"
            ],
            "source": "http://www.bbc.co.uk/news"
        },
        "pathFromSeed": ""
    },
    "isSeed": true,
    "url": "http://www.bbc.co.uk/news"
}

"""

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
import urllib
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


def setup_outward_channel(client_id):
            # Set up connection:
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
            # Turn on delivery confirmations
            channel.confirm_delivery()
            return channel

#
def send_amqp_message(message, client_id, outchannel):
    """Send message to AMQP."""
    sent = False
    logger.debug("Sending (to %s) message %s" % (client_id, message));
    # Keep trying over and over:
    while not sent:
        try:
            sent = outchannel.basic_publish(exchange=settings.exchange,
                routing_key=client_id,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ),
                body=message)
        except:
            logger.error("Problem sending message: %s; %s" % (message, sys.exc_info()))
            logger.error("Sleeping for 30 seconds...")
            time.sleep(30)

def send_to_amqp(outchannel,client_id, url,method,headers, parentUrl, parentUrlMetadata, forceFetch=False, isSeed=False, hop='L'):
    message = {
        "url": url,
        "method": method,
        "headers": headers,
        "parentUrl": parentUrl,
        "parentUrlMetadata": parentUrlMetadata,
        "hop": hop
    }
    if forceFetch:
        message["forceFetch"] = True
    if isSeed:
        message["isSeed"] = True
    logger.debug("Sending message: %s" % message)
    send_amqp_message(json.dumps(message), client_id, outchannel)

def is_embed(entry):
    '''
    Checks whether a HAR request/response entry is likely to be an embed
    '''
    # If the browser sends a request that Accepts HTML, it's unlikely to be an embed:
    for header in entry['request']['headers']:
        if header['name'] == 'Accept':
            if 'text/html' in header['value']:
                return False
               
    # If the response appears to be HTML, it's unlikely to be an embed:
    for header in entry['response']['headers']:
        if header['name'] == 'Content-Type':
            if 'text/html' in header['value']:
                return False
            if 'application/xhtml' in header['value']:
                return False
            
    # Otherwise, assume it's an embed
    return True

def amqp_outlinks(outchannel, raw_har, client_id, raw_parent):
    """Passes outlinks back to queue."""
    har = json.loads(raw_har)
    parent = json.loads(raw_parent)
    # Send the parent on, set as seed if required:
    send_to_amqp(outchannel,client_id, parent["url"], "GET", {}, parent["url"], parent["metadata"], True, parent.get("isSeed",False), hop='I')
    # Process the embeds:
    resources = 0
    if settings.extract_embeds:
        for entry in har["log"]["entries"]:
            resources = resources + 1
            # Only send URLs with the allowed protocols:
            protocol = urlparse(entry["request"]["url"]).scheme
            if not protocol in settings.protocols:
                continue
            # Do not sent duplicates URLs out again:
            if parent["url"] == entry['request']['url']:
                continue
            # Set the hop types:
            hop = 'E'
            if not is_embed(entry):
                hop = 'L'
            # Send
            logger.info("Sending transcluded URL (%s) %s" % (hop, entry['request']['url']))
            send_to_amqp(outchannel,client_id, entry["request"]["url"],entry["request"]["method"], 
                {h["name"]: h["value"] for h in entry["request"]["headers"]}, 
                parent["url"], parent["metadata"], forceFetch=True, hop=hop)
        # PANIC if there are none at all, as this means the original URL did not work and should be looked at.
        if resources == 0:
            logger.warning("No resources transcluded for %s - not even itself!" % parent["url"])
        #raise Exception("Download of %s failed completely - is this a valid URL?" % parent["url"])
    # Process the discovered links (if desired):
    links = 0
    if settings.extract_links:
        for entry in har["log"]["pages"]:
            for item in entry["map"]:
                # Some map regions are JavaScript rather than direct links, so only take the links:
                if 'href' in item:
                    # Skip sending the parent again:
                    if parent["url"] == item['href']:
                        continue
                    links = links + 1
                    logger.info("Sending discovered (L) URL %s" % item['href'])
                    send_to_amqp(outchannel,client_id, item['href'],"GET", {}, parent["url"], parent["metadata"])
    logger.info("Queued %i resources and %i links for url '%s'." % (resources, links, parent["url"]) )


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
        try:
            (url, handler_id, selectors, url_handler) = handle_json_message(body)
        except Exception as e:
            logger.error("Ignoring invalid (unparseable) message! \"%s\"" % body, e )
            return False
        # Allow settings to override
        if settings.routing_key and not handler_id:
            handler_id = settings.routing_key

        # Start the render:            
        logger.info("Requesting render of %s" % url )
        start_time = time.time()
        ws = "%s/%s" % (settings.webrender_url, urllib.quote(url))
        logger.debug("Calling %s" % ws)
        r = requests.post(ws, data=json.dumps(selectors))
        if r.status_code == 200:
            # Get the HAR payload
            logger.debug("Got response. Reading.")
            har = r.content
            logger.debug("Got HAR.")
            # Write to the WARC
            wrid = uuid.uuid1()
            headers = [
                (WarcRecord.TYPE, WarcRecord.METADATA),
                (WarcRecord.URL, url),
                (WarcRecord.CONTENT_TYPE, "application/json"),
                (WarcRecord.DATE, warc_datetime_str(datetime.now())),
                (WarcRecord.ID, "<urn:uuid:%s>" % wrid),
            ]
            warcwriter.write_record(headers, "application/json", har)
            # TODO Also pull out the rendings as separate records?
            # see http://wpull.readthedocs.org/en/master/warc.html
            logger.debug("Written WARC.")
            # Send on embeds and outlinks, passing original message too...
            outchannel = setup_outward_channel(handler_id)
            url_handler(outchannel, har, handler_id, body)
            logger.debug("Sent messages.")
            # Record total elapsed time:
            end_time = time.time()
            logger.info("Rendered and recorded output for %s in %d seconds." %(url, end_time-start_time))
            # It appears everything worked, so return True and ack the original message
            return True
        else:
            logger.warning("None-200 response for %s; %s" % (body, r.content))
            return True
    except Exception as e:
        logger.exception("Exception %s %s when handling [%s]" % (type(e).__name__, e, body))
        return False

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
            channel.basic_qos(prefetch_count=settings.qos_num)
            logger.info("Started connection: %s" % (settings.amqp_url))
            for method_frame, properties, body in channel.consume(settings.in_queue):
                handled = callback(warcwriter, body)
                if handled is True:
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
        help="AMQP endpoint to use. [default: %(default)s]" )
    parser.add_argument('--webrender-url', dest='webrender_url', type=str, default="http://webrender:8000/webtools/domimage", 
        help="HAR webrender endpoint to use. [default: %(default)s]" )
    parser.add_argument("-E", "--extract_embeds", dest="extract_embeds", action="store_true", default=True, required=False, 
                    help="Extract URLs of transcluded resources [default: %(default)s]")
    parser.add_argument('-L', '--extract-links', dest='extract_links', action="store_true", default=False, required=False,
        help="Extract discovered hyperlinks [default: %(default)s]" )
    parser.add_argument('--num', dest='qos_num', 
        type=int, default=10, help="Maximum number of messages to handle at once. [default: %(default)s]")
    parser.add_argument('--log-level', dest='log_level', 
        type=str, default='INFO', help="Log level, e.g. ERROR, WARNING, INFO, DEBUG. [default: %(default)s]")
    parser.add_argument('--routing-key', dest='routing_key', 
                        help="Routing key to use for extracted links if there is no clientId set in the incoming message.")
    parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
    parser.add_argument('in_queue', metavar='in_queue', help="Name of queue to view messages from.")
    parser.add_argument('binding_key', metavar='binding_key', help="Name of binding_key for the input queue.")
    parser.add_argument('output_directory', metavar='out_dir', help="Directory to write HAR output WARCs into.")
    
    settings = parser.parse_args()
    
    settings.protocols = ['http', 'https']
        
    # Report settings:
    logger.info("log_level = %s", settings.log_level)
    logger.info("output_directory = %s", settings.output_directory)
    logger.info("webrender_url = %s", settings.webrender_url)
    logger.info("protocols = %s", settings.protocols)
    logger.info("AMQP_URL = %s", settings.amqp_url)
    logger.info("AMQP_EXCHANGE = %s", settings.exchange)
    logger.info("AMQP_QUEUE = %s", settings.in_queue)
    logger.info("AMQP_BINDING_KEY = %s", settings.binding_key)

    # Set up the logging    
    logger.setLevel( logging.getLevelName(settings.log_level))

    # ...and run:
    run_harchiver()

