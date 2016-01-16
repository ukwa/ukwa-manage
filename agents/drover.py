#!/usr/bin/env python

"""Drives the crawls. Downloads the crawl feeds and initiates crawls as appropriate by dropping messages on the right queue."""

import os
import logging
import pika
import argparse
from shared.w3act import w3act

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


if __name__ == "__main__":
	parser = argparse.ArgumentParser('Peek at a message queue, downloading messages without ack-ing so that they remain on the queue.')
	default_amqp_url = "amqp://guest:guest@localhost:5672/%2f"
	parser.add_argument('--amqp-url', dest='amqp_url', 
					type=str, default=default_amqp_url, 
					help="AMQP endpoint to use (defaults to amqp://guest:guest@localhost:5672/%%2f)" )
	parser.add_argument('--num', dest='peek_num', 
					type=int, default=10, help="Maximum number of messages to peek at, (defaults to 10)")
	parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
	parser.add_argument('queue', metavar='queue', help="Name of queue to view messages from.")
	args = parser.parse_args()
	
	# Get all the frequently-crawled items
	act = w3act("http://localhost:9000/act/","wa-sysadm@bl.uk","sysAdmin")
	logger.info(act.get_ld_export('frequent'));
	
	# Determine if any are due to start in the current hour
	
	# Push a 'seed' message onto the rendering queue:
	parameters = pika.URLParameters(args.amqp_url)
	connection = pika.BlockingConnection( parameters )
	channel = connection.channel()
	channel.queue_bind(queue=args.queue, exchange=args.exchange)
	
	# Note that the other aspects, like depth etc, and setup periodically via fc-shaper.
	
	# Separate process bundles up per checkpoint (gather.py)
	
	# Separate process sends Documents to a queue (in H3) and sends the queue to W3ACT (mule.py)
	# muster.py, yoke.py, shear.py, rouseabout, riggwelter (upside down sheep), 
	# lanolin (grease), cull.py, heft (land), flock, fold, dip, bellwether (flock lead)

