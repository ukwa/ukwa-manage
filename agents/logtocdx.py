#!/usr/bin/env python

"""Watched the crawl log queue and passes entries to the CDX server"""

import os
import json
import pika
import time
import logging
import requests

AMQP_URL = os.environ['AMQP_URL']
QUEUE_NAME = os.environ['QUEUE_NAME']
DUMMY = os.environ['DUMMY_RUN']
CDX_SERVER_URL = os.environ['CDX_SERVER_URL']

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
logger = logging.getLogger( "logtocdx" )
logger.setLevel( logging.INFO )


# - 20150914222034 http://www.financeminister.gov.au/                     text/html 200      ZMSA5TNJUKKRYAIM5PRUJLL24DV7QYOO - - 83848 117273 WEB-20150914222031256-00000-43190~heritrix.nla.gov.au~8443.warc.gz
# - 20151201225932 http://anjackson.net/projects/images/keeping-codes.png image/png 200 sha1:DDOWG5GHKEDGUCOCOXZCAPRUXPND7GOK - - -     593544 BL-20151201225813592-00001-37~157a2278f619~8443.warc.gz
# - 20151202001114 http://anjackson.net/robots.txt unknown 302 sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ http://anjackson.net/ - - 773 BL-20151202001107925-00001-41~157a2278f619~8443.warc.gz

# Example de-duplicated CDX line from pywb cdx-indexer:
#
# net,anjackson)/assets/js/ie8-responsive-file-warning.js 20151202230549 http://anjackson.net/assets/js/ie8-responsive-file-warning.js text/html 404 HJ66ECSQVYNX22SEAFF7QAF4AZYKN2BD - - 2755 2945445 BL-20151202230405810-00000-38~101e6c786d7f~8443.warc.gz
# net,anjackson)/assets/js/ie8-responsive-file-warning.js 20151202230632 http://anjackson.net/assets/js/ie8-responsive-file-warning.js warc/revisit - HJ66ECSQVYNX22SEAFF7QAF4AZYKN2BD - - 548 3604638 BL-20151202230405810-00000-38~101e6c786d7f~8443.warc.gz


#[2015-12-02 22:35:45,851] ERROR: Failed with 400 Bad Request
#java.lang.NumberFormatException: For input string: "None"
#At line: - 20151202223545 dns:447119634 text/dns 1001 None - - - None None

def callback( ch, method, properties, body ):
	"""Passed a crawl log entry, it turns it into a CDX line and posts it to the index."""
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
		if DUMMY:
			logger.warning( "Running in dummy mode." )
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

