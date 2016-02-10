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

import os
import sys
import logging
import argparse
import dateutil.parser
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib.agents.w3act import w3act
from lib.agents.launch import launcher


# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.INFO )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )

def launch_by_hour(now,startDate,t,destination,source):
			# Is it the current hour?
			if now.hour is startDate.hour:
				logger.info("The hour is current, sending seed for %s to the crawl queue." % t['title'])
				counter = 0
				for seed in t['seeds']:
					# For now, only treat the first URL as a scope-defining seed that we force a re-crawl for:
					if counter == 0:
						isSeed = True
					else:
						isSeed = False
					# And send launch message:
					launcher.launch(destination, seed, source, isSeed, "FC-3-uris-to-crawl")
					counter = counter + 1
				
			else:
				logger.info("The hour (%s) is not current." % startDate.hour)


def write_surt_file(targets,filename):
	with open(filename, 'w') as f:
		for t in targets:
			for seed in t['seeds']:
				#f.write("%s\n" % url_to_surt(seed))
				f.write("%s\n" % seed)

def write_watched_surt_file(targets,filename):
	with open(filename, 'w') as f:
		for t in targets:
			if t['watched']:
				for seed in t['seeds']:
					#f.write("%s\n" % url_to_surt(seed))
					f.write("%s\n" % seed)

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
	parser.add_argument("-t", "--timestamp", dest="timestamp", type=str, 
					help="Timestamp to use rather than the current time, e.g. \"2016-01-13 09:00:00\" ", 
					default=datetime.utcnow().isoformat())
	parser.add_argument("-f", "--frequency", dest="frequency", type=str, 
					help="Frequency to look at. Use 'frequent' for all valid frequencies. [default: %(default)s]", default='frequent')
	parser.add_argument("-d", "--destination", dest="destination", type=str, default='har',
					help="Destination, implying message format to use: 'har' or 'h3'. [default: %(default)s]")
	parser.add_argument("-tid", "--target-id", dest="target_id", type=int,
					help="Target ID to allow to launch (for testing purposes). [default: %(default)s]")
	parser.add_argument("-S", "--surt-file", dest="surt_file", type=str, 
					help="SURT file to write to, for scoping Heritrix crawls [default: %(default)s]", default=None)	
	parser.add_argument("-W", "--watched-surt-file", dest="watched_surt_file", type=str, 
					help="SURT file to write Watched Targets, for scoping document extraction [default: %(default)s]", default=None)	
	parser.add_argument('queue', metavar='queue', help="Name of queue to send seeds to.")
	
	args = parser.parse_args()
	
	# Get all the frequently-crawled items
	act = w3act(args.w3act_url,args.w3act_user,args.w3act_pw)
	targets = act.get_ld_export(args.frequency)
	logger.info("Got %s targets" % len(targets))
	destination = args.destination # or use "h3" for message suitable for h3
	
	# Update scope file, if enabled:
	if args.surt_file:
		write_surt_file(targets, args.surt_file)
	
	# Update watched target scope file, if enabled:
	if args.watched_surt_file:
		write_watched_surt_file(targets, args.watched_surt_file)
		
	# Set up launcher:
	launcher = launcher(args)
	
	# Determine if any are due to start in the current hour
	now = dateutil.parser.parse(args.timestamp)
	for t in targets:
		if args.target_id and not int(t['id']) == args.target_id:
			continue
		logger.info("Looking at %s (tid:%d)" % (t['title'], t['id']))
		# Add a source tag if this is a watched target:
		source = ''
		if t['watched']:
			source = "WTID:%d" % t['id']
		# Check the scheduling:
		for schedule in t['schedules']:
			startDate = datetime.fromtimestamp(schedule['startDate']/1000)
			if( now < startDate ):
				continue
			if schedule['endDate']:
				endDate = datetime.fromtimestamp(schedule['endDate']/1000)
				if now > endDate:
					continue
			# Check if the frequency and date match up:
			if schedule['frequency'] == "DAILY":
				launch_by_hour(now,startDate,t,destination,source)
			elif schedule['frequency'] == "WEEKLY":
				if now.isoweekday() == startDate.isoweekday():
					launch_by_hour(now,startDate,t,destination,source)
			elif schedule['frequency'] == "MONTHLY":
				if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day:
					launch_by_hour(now,startDate,t,destination,source)
			elif schedule['frequency'] == "QUARTERLY":
				if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month%3 == startDate.month%3:
					launch_by_hour(now,startDate,t,destination,source)
			elif schedule['frequency'] == "SIXMONTHLY":
				if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month%6 == startDate.month%6:
					launch_by_hour(now,startDate,t,destination,source)
			elif schedule['frequency'] == "ANNUAL":
				if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month == startDate.month:
					launch_by_hour(now,startDate,t,destination,source)
			else:
				logger.error("Don't understand crawl frequency "+schedule['frequency'])
