#!/usr/bin/env python

"""Watched the crawl log queue and passes entries to the CDX server"""

import os
import re
import sys
import json
import pika
import time
import shutil
import logging
import requests

AMQP_URL = os.environ['AMQP_URL']
QUEUE_NAME = os.environ['QUEUE_NAME']
LOG_FILE = os.environ['LOG_FILE']
DUMMY = os.environ['DUMMY_RUN']

logger = logging.getLogger( "logtocdx" )
handler = logging.FileHandler( LOG_FILE )
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s: %(message)s" )
handler.setFormatter( formatter )
logger.addHandler( handler )
logger.setLevel( logging.INFO )

#Try to set logging output for all modules.
logging.root.setLevel( logging.WARNING )
logging.getLogger( "" ).addHandler( handler )


# - 20150914222034 http://www.financeminister.gov.au/                     text/html 200      ZMSA5TNJUKKRYAIM5PRUJLL24DV7QYOO - - 83848 117273 WEB-20150914222031256-00000-43190~heritrix.nla.gov.au~8443.warc.gz
# - 20151201225932 http://anjackson.net/projects/images/keeping-codes.png image/png 200 sha1:DDOWG5GHKEDGUCOCOXZCAPRUXPND7GOK - - -     593544 BL-20151201225813592-00001-37~157a2278f619~8443.warc.gz
# - 20151202001114 http://anjackson.net/robots.txt unknown 302 sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ http://anjackson.net/ - - 773 BL-20151202001107925-00001-41~157a2278f619~8443.warc.gz

def callback( ch, method, properties, body ):
	"""Passed a crawl log entry, it turns it into a CDX line and posts it to the index."""
	try:
		logger.debug( "Message received: %s." % body )
		cl = json.loads(body)
		url = cl["url"]
		# Skip non http(s) records (?)
		#if( not url[:4] == "http"):
		#	ch.basic_ack(delivery_tag = method.delivery_tag)
		redirect = "-"
		status_code = cl["status_code"]
		# Don't index negative status codes here:
		if( status_code <= 0 ):
			logger.info("Ignoring -ve status_code log entry: %s" % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		# Don't index revisit records as OW can't handle them (?)
		if "duplicate:digest" in cl["annotations"]:
			logger.info("Skipping de-duplicated resource: %s" % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		# Record via for redirects:
		if( status_code/100 == 3 ):
			redirect = cl["via"]
		cdx_11 = "- %s %s %s %s %s %s - - %s %s\n" % ( 
			cl["start_time_plus_duration"][:14],
			url,
			cl["mimetype"],
			status_code,
			cl["content_digest"],
			redirect,
			cl["warc_offset"],
			cl["warc_filename"]
			)
		logger.debug("CDX: %s" % cdx_11)
		r = requests.post("http://192.168.99.100:9090/fc", data=cdx_11)
		if( r.status_code == 200 ):
			logger.debug("Success!")
			ch.basic_ack(delivery_tag = method.delivery_tag)
		else:
			logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )
		logging.exception(e)

if __name__ == "__main__":
	try:
		if DUMMY:
			logger.warning( "Running in dummy mode." )
		logger.info( "Starting connection %s:%s." % ( AMQP_URL, QUEUE_NAME ) )
		parameters = pika.URLParameters(AMQP_URL)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.exchange_declare(exchange="heritrix", durable=True)
		channel.queue_declare( queue=QUEUE_NAME, durable=True )
		channel.queue_bind(queue=QUEUE_NAME, exchange="heritrix", routing_key="crawl-log-feed")
		channel.basic_qos(prefetch_count=1)
		channel.basic_consume( callback, queue=QUEUE_NAME, no_ack=False )
		channel.start_consuming()
	except Exception as e:
		logger.error( str( e ) )
		logging.exception(e)
		logger.info("Sleeping for 10 seconds before a restart is attempted...")
		time.sleep(10)

