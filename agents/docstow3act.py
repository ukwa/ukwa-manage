#!/usr/bin/env python

"""Watched the crawled documents log queue and passes entries to w3act

Input:

{
    "annotations": "ip:173.236.225.186,duplicate:digest",
    "content_digest": "sha1:44KA4PQA5TYRAXDIVJIAFD72RN55OQHJ",
    "content_length": 324,
    "extra_info": {},
    "hop_path": "IE",
    "host": "acid.matkelly.com",
    "jobName": "frequent",
    "mimetype": "text/html",
    "seed": "WTID:12321444",
    "size": 511,
    "start_time_plus_duration": "20160127211938966+230",
    "status_code": 404,
    "thread": 189,
    "timestamp": "2016-01-27T21:19:39.200Z",
    "url": "http://acid.matkelly.com/img.png",
    "via": "http://acid.matkelly.com/",
    "warc_filename": "BL-20160127211918391-00001-35~ce37d8d00c1f~8443.warc.gz",
    "warc_offset": 36748
}

Note that 'seed' is actually the source tag, and is set up to contain the original (Watched) Target ID.

Output:

[
{
"id_watched_target":<long>,
"wayback_timestamp":<String>,
"landing_page_url":<String>,
"document_url":<String>,
"filename":<String>,
"size":<long>
},
<further documents>
]

See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint

i.e. 

seed -> id_watched_target
start_time_plus_duration -> wayback_timestamp
via -> landing_page_url
url -> document_url (and filename)
content_length -> size

Note that, if necessary, this process to refer to the 
cdx-server and wayback to get more information about 
the crawled data and improve the landing page and filename data.


"""

import os
import json
import pika
import time
import logging
import requests
import argparse

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
		r = requests.post(args.w3act_url, data=cdx_11)
		if( r.status_code == 200 ):
			logger.debug("Success!")
			ch.basic_ack(delivery_tag = method.delivery_tag)
		else:
			logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )
		logging.exception(e)

if __name__ == "__main__":
	parser = argparse.ArgumentParser('Get documents from the queue and post to W3ACT.')
	parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
		help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('--w3act-url', dest='w3act_url', type=str, default="http://localhost:9000/act", 
		help="W3ACT endpoint to use [default: %(default)s]" )
	parser.add_argument('--num', dest='qos_num', 
		type=int, default=100, help="Maximum number of messages to handle at once. [default: %(default)s]")
	parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
	parser.add_argument('queue', metavar='queue', help="Name of queue to view messages from.")
	
	args = parser.parse_args()
	
	try:
		logger.info( "Starting connection %s:%s." % ( args.amqp_url, args.queue ) )
		parameters = pika.URLParameters(args.amqp_url)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.exchange_declare(exchange=args.exchange, durable=True)
		channel.queue_declare( queue=args.queue, durable=True )
		channel.queue_bind(queue=args.queue, exchange=args.exchange, routing_key="documents-to-catalogue")
		channel.basic_qos(prefetch_count=args.qos_num)
		channel.basic_consume( callback, queue=args.queue, no_ack=False )
		channel.start_consuming()
	except Exception as e:
		logger.error( str( e ) )
		logging.exception(e)
		logger.info("Sleeping for 10 seconds before a restart is attempted...")
		time.sleep(10)

