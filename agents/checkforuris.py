#!/usr/bin/env python

"""Watch the uris-to-check queue and wait for them to turn up in Wayback
"""

import os
import sys
import json
import pika
import time
import logging
import argparse
from urlparse import urlparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib.agents.w3act import w3act
from lib.agents.document_mdex import DocumentMDEx
from lib.agents.wayback import document_available

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Replace root logger
logging.root.handlers = []
logging.root.addHandler( handler )
logging.root.setLevel( logging.DEBUG )

# Set default logging output for all modules.
logging.getLogger('requests').setLevel( logging.WARNING )
logging.getLogger('pika').setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger(__name__)

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
		status_code = cl["status_code"]
		# Don't index negative or non 2xx status codes here:
		if( status_code/100 != 2 ):
			logger.info("Ignoring <=0 status_code log entry: %s" % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		# grab source tag
		source = cl.get('source', None)
		# Build document info line:
		doc = {}
		doc['wayback_timestamp'] = cl['start_time_plus_duration'][:14]
		doc['landing_page_url'] = cl['via']
		doc['document_url'] = cl['url']
		doc['filename'] = os.path.basename( urlparse(cl['url']).path )
		doc['size'] = int(cl['content_length'])
		# Check if content appears to be in Wayback:
		if document_available(args.wb_url, doc['document_url'], doc['wayback_timestamp']):
			# Lookup Target and extract any additional metadata:
			doc = DocumentMDEx(act, doc, source).mdex()
			# Documents may be rejected at this point:
			if doc == None:
				logger.critical("The document based on this message has been REJECTED! :: "+body)
				ch.basic_ack(delivery_tag = method.delivery_tag)
				return
			# Inform W3ACT it's available:
			logger.debug("Sending doc: %s" % doc)
			r = act.post_document(doc)
			if( r.status_code == 200 ):
				logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
				ch.basic_ack(delivery_tag = method.delivery_tag)
				return
			else:
				logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
		else:
			logger.info("Not yet available in wayback: %s" % doc['document_url'])
	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )
		logging.exception(e)
		
	# All that failed? Then reject and requeue the message to try later:
	ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
	# Now sleep briefly to avoid overloading the servers:
	logger.warning("Sleeping for a few seconds before retrying...")
	time.sleep(10)
	return

if __name__ == "__main__":
	parser = argparse.ArgumentParser('Get documents from the queue and post to W3ACT.')
	parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
		help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('-w', '--w3act-url', dest='w3act_url', 
					type=str, default="http://localhost:9000/act/", 
					help="W3ACT endpoint to use [default: %(default)s]" )
	parser.add_argument('-u', '--w3act-user', dest='w3act_user', 
					type=str, default="wa-sysadm@bl.uk", 
					help="W3ACT user email to login with [default: %(default)s]" )
	parser.add_argument('-p', '--w3act-pw', dest='w3act_pw', 
					type=str, default="sysAdmin", 
					help="W3ACT user password [default: %(default)s]" )
	parser.add_argument('-W', '--wb-url', dest='wb_url', 
					type=str, default="http://localhost:8080/wayback", 
					help="Wayback endpoint to check URL availability [default: %(default)s]" )
	parser.add_argument('--num', dest='qos_num', 
		type=int, default=10, help="Maximum number of messages to handle at once. [default: %(default)s]")
	parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
	parser.add_argument('queue', metavar='queue', help="Name of queue to view messages from.")
	
	args = parser.parse_args()
	
	try:
		# Set up connection to ACT:
		act = w3act(args.w3act_url,args.w3act_user,args.w3act_pw)
		# Connect to AMQP:
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

