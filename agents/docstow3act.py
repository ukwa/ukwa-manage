#!/usr/bin/env python

"""Watched the crawled documents log queue and passes entries to w3act"""

import os
import json
import pika
import time
import logging
import requests

# Should we skip duplicate records?
# It seems OWB cope with them.
skip_duplicates = False

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )


def callback( ch, method, properties, body ):
	"""Passed a document crawl log entry, POSTs it to W3ACT."""
	try:
		logger.debug( "Message received: %s." % body )
		cl = json.loads(body)
		url = cl["url"]
		# Skip non http(s) records (?)
		if( not url[:4] == "http"):
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		redirect = "-"
		status_code = cl["status_code"]
		# Don't index negative status codes here:
		if( status_code <= 0 ):
			logger.info("Ignoring <=0 status_code log entry: %s" % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		# Record via for redirects:
		if( status_code/100 == 3 ):
			redirect = cl["via"]
		# Don't index revisit records as OW can't handle them (?)
		mimetype = cl["mimetype"]
		if "duplicate:digest" in cl["annotations"]:
			if skip_duplicates:
				logger.info("Skipping de-duplicated resource: %s" % body)
				ch.basic_ack(delivery_tag = method.delivery_tag)
				return
			else:
				mimetype = "warc/revisit"
				status_code = "-"
		# Build CDX line:
		cdx_11 = "- %s %s %s %s %s %s - - %s %s\n" % ( 
			cl["start_time_plus_duration"][:14],
			url,
			mimetype,
			status_code,
			cl["content_digest"],
			redirect,
			cl["warc_offset"],
			cl["warc_filename"]
			)
		logger.debug("CDX: %s" % cdx_11)
		r = requests.post(CDX_SERVER_URL, data=cdx_11)
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
		logger.info( "Starting connection %s:%s." % ( AMQP_URL, QUEUE_NAME ) )
		parameters = pika.URLParameters(AMQP_URL)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.exchange_declare(exchange="heritrix", durable=True)
		channel.queue_declare( queue=QUEUE_NAME, durable=True )
		channel.queue_bind(queue=QUEUE_NAME, exchange="heritrix", routing_key="crawl-log-feed")
		channel.basic_qos(prefetch_count=10)
		channel.basic_consume( callback, queue=QUEUE_NAME, no_ack=False )
		channel.start_consuming()
	except Exception as e:
		logger.error( str( e ) )
		logging.exception(e)
		logger.info("Sleeping for 10 seconds before a restart is attempted...")
		time.sleep(10)

