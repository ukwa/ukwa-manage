#!/usr/bin/env python
# encoding: utf-8
'''
agents.drover -- Drives the Frequent Crawl

agents.drover checks for Targets that should be re-crawled and initiates the crawl by sending messages to the crawl queues.
Downloads the crawl feeds and initiates crawls as appropriate by dropping messages on the right queue.

For the HAR daemon, we use:

{
    "clientId": "FC-3-uris-to-crawl",
    "metadata": {
        "heritableData": {
            "heritable": [
                "source",
                "heritable"
            ],
            "source": "http://acid.matkelly.com/"
        },
        "pathFromSeed": ""
    },
    "isSeed": true,
    "url": "http://acid.matkelly.com/"
}

@author:     Andrew Jackson

@copyright:  2016 The British Library.

@license:    Apache 2.0

@contact:    Andrew.Jackson@bl.uk
@deffield    updated: 2016-01-16
'''

import logging
import argparse
import json
import pika
import dateutil.parser
from datetime import datetime
from shared.w3act import w3act
from _watchdog_fsevents import schedule

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

def send_message( message ):
	"""Sends a message to the given queue."""
	parameters = pika.URLParameters(args.amqp_url)
	connection = pika.BlockingConnection( parameters )
	channel = connection.channel()
	channel.exchange_declare(exchange=args.exchange, durable=True)
	channel.queue_declare( queue=args.queue, durable=True )
	channel.queue_bind(queue=args.queue, exchange=args.exchange)#, routing_key="uris-to-crawl")
	channel.tx_select()
	channel.basic_publish( exchange=args.exchange,
		routing_key=args.queue,
		properties=pika.BasicProperties(
			delivery_mode=2,
		),
		body=message )
	channel.tx_commit()
	connection.close()

if __name__ == "__main__":
	parser = argparse.ArgumentParser('(Re)Launch frequently crawled sites.')
	parser.add_argument('-w', '--w3act-url', dest='w3act_url', 
					type=str, default="http://localhost:9000/act/", 
					help="W3ACT endpoint to use [default: %(default)s]" )
	parser.add_argument('-u', '--w3act-user', dest='w3act_user', 
					type=str, default="wa-sysadm@bl.uk", 
					help="W3ACT user email to login with [default: %(default)s]" )
	parser.add_argument('-p', '--w3act-pw', dest='w3act_pw', 
					type=str, default="sysAdmin", 
					help="W3ACT user password [default: %(default)s]" )
	parser.add_argument('-a', '--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@127.0.0.1:5672/%2f",
					help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('-e', '--exchange', dest='exchange', 
					type=str, default="heritrix",
					help="Name of the exchange to use (defaults to heritrix).")	
	parser.add_argument("-t", "--timestamp", dest="timestamp", type=str, required=False, 
					help="Timestamp to use rather than the current time, e.g. \"2016-01-13 09:00:00\" ", 
					default=datetime.utcnow().isoformat())
	parser.add_argument("-f", "--frequency", dest="frequency", type=str, required=False, 
					help="Frequency to look at. Use 'frequent' for all valid frequencies. [default: %(default)s]", nargs="+", default='frequent')
	parser.add_argument("-d", "--destination", dest="destination", type=str, required=True, default='har',
					help="Destination, implying message format to use: 'har' or 'h3'. [default: %(default)s]")
	parser.add_argument('queue', metavar='queue', help="Name of queue to send seeds to.")
	
	args = parser.parse_args()
	
	# Get all the frequently-crawled items
	act = w3act(args.w3act_url,args.w3act_user,args.w3act_pw)
	targets = act.get_ld_export('frequent')
	destination = "har" # or use "h3" for message suitable for h3
	
	# Determine if any are due to start in the current hour
	now = dateutil.parser.parse(args.timestamp)
	for t in targets:
		logger.info("Looking at "+t['title']);
		for schedule in t['schedules']:
			startDate = datetime.fromtimestamp(schedule['startDate']/1000)
			if( now < startDate ):
				continue
			if schedule['endDate']:
				endDate = datetime.fromtimestamp(schedule['endDate']/1000)
				if now > endDate:
					continue
			# Is it the current hour?
			if now.hour is startDate.hour:
				logger.info("The hour is current, sending seed for %s to the crawl queue." % t['title'])
				for seed in t['seeds']:			
					curim = {}
					if destination is "h3":
						curim['headers'] = {}
						#curim['headers']['User-Agent'] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.120 Chrome/37.0.2062.120 Safari/537.36"
						curim['method']= "GET"
						curim['parentUrl'] = seed
						curim['parentUrlMetadata'] = {}
						curim['parentUrlMetadata']['pathFromSeed'] = ""
						curim['parentUrlMetadata']['source'] = seed
						curim['parentUrlMetadata']['heritable'] = ['source','heritable']
						curim['isSeed'] = "true"
						curim['url'] = seed
					elif destination is "har":
						curim['clientId']= "FC-3-uris-to-crawl"
						curim['metadata'] = {}
						curim['metadata']['heritableData'] = {}
						curim['metadata']['heritableData']['heritable'] = ['source','heritable']
						curim['metadata']['heritableData']['source'] = seed
						curim['metadata']['pathFromSeed'] = ""
						curim['isSeed'] = "true"
						curim['url'] = seed
					else:
						logger.severe("Can't handle destination type '%s'" % destination )
					message = json.dumps(curim)
					logger.info("Got message: "+message)
	
					# Push a 'seed' message onto the rendering queue:
					send_message(message)
				
			else:
				logger.info("The hour is not current.")
			


	
	# Note that the other aspects, like depth etc, and setup periodically via "h3cc fc-sync".
	
	# Separate process bundles up per checkpoint (gather.py)
	
	# Separate process sends Documents to a queue (in H3) and sends the queue to W3ACT (mule.py)
	# muster.py, yoke.py, shear.py, rouseabout, riggwelter (upside down sheep), 
	# lanolin (grease), cull.py, heft (land), flock, fold, dip, bellwether (flock lead)
	
	

