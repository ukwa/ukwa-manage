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
import logging
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib.agents.launch import launcher


# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.ERROR )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )


if __name__ == "__main__":
	parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
	parser.add_argument('-a', '--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@127.0.0.1:5672/%2f",
					help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('-e', '--exchange', dest='exchange', 
					type=str, default="heritrix",
					help="Name of the exchange to use (defaults to heritrix).")	
	parser.add_argument("-d", "--destination", dest="destination", type=str, default='har',
					help="Destination, implying message format to use: 'har' or 'h3'. [default: %(default)s]")
	parser.add_argument("-s", "--source", dest="source", type=str, default='',
					help="Source tag to attach to this URI, if any. [default: %(default)s]")
	parser.add_argument("-S", "--seed", dest="seed", action="store_true", default=False, required=False, help="Treat supplied URI as a seed, thus widening crawl scope. [default: %(default)s]")
	parser.add_argument('queue', metavar='queue', help="Name of queue to send seeds to.")
	parser.add_argument('uri', metavar='uri', help="URI to enqueue.")
	
	args = parser.parse_args()
	
	# Set up launcher:
	launcher = launcher(args)
	launcher.launch(args.destination, args.uri, args.source, isSeed=args.seed, clientId="FC-3-uris-to-crawl")
	
