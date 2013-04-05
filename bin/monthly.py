#!/usr/local/bin/python2.7

import os
import re
import sys
import time
import rfc3987
import urllib2
import logging
import argparse
import dateutil.parser
from crontab import CronTab
from datetime import datetime
from xml.dom.minidom import parseString

MONTHLY_URL = "http://www.webarchive.org.uk/act/websites/export/monthly"
MONTHLY_DAY = "8"
DAILY_HOUR = "12"
SEED_FILE = "/heritrix/monthly-seeds.txt"
LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "monthly" )
seeds = []

try:
	xml = urllib2.urlopen( MONTHLY_URL ).read()
except urllib2.URLError, e:
	logger.error( "Cannot read ACT! " + e )
	sys.exit( 1 )

def add_seeds( urls ):
	for url in urls:
		try:
			parsed = rfc3987.parse( url, rule="URI" )
			seeds.append( url )
			logger.info( "Adding: " + url )
		except ValueError, v:
			logger.error( "INVALID URL: " + url )

dom = parseString( xml )
now = datetime.now()
#crawlDateRange will be:
#	blank			Monthly at 12:00
#	"start_date"		Monthly at start time/day specified if start_date < now
#	"start_date end_date"	Monthly at start time/day specified if start_date < now and end_date > now
for node in dom.getElementsByTagName( "node" ):
	start_date = end_date = ""
	if len( node.getElementsByTagName( "crawlStartDate" )[ 0 ].childNodes ) > 0:
		start_date = node.getElementsByTagName( "crawlStartDate" )[ 0 ].firstChild.nodeValue
	if len( node.getElementsByTagName( "crawlEndDate" )[ 0 ].childNodes ) > 0:
		start_date = node.getElementsByTagName( "crawlEndDate" )[ 0 ].firstChild.nodeValue

	if start_date == "" and str( now.hour ) == DAILY_HOUR and str( now.day ) == MONTHLY_DAY:
		add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
	elif end_date == "":
		start_date = dateutil.parser.parse( start_date )
		if start_date.hour == now.hour and start_date.day == now.day and start_date < now:
			add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
	else:
		start_date = dateutil.parser.parse( start_date )
		end_date = dateutil.parser.parse( end_date )
		if start_date.hour == now.hour and start_date.day == now.day and start_date < now and end_date > now:
			add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )


#Exit abnormally if there are no relevant seeds.
if len( seeds ) == 0:
	sys.exit( 1 )
else:
	try:
		output = open( SEED_FILE, "wb" )
		logger.info( "Writing seeds to " + SEED_FILE )
		for seed in seeds:
			output.write( seed + "\n" )
		output.close()
		logger.info( "Found " + str( len( seeds ) ) + " seeds." )
	except IOError, i:
		logger.warning( "Problem writing seeds to " + SEED_FILE )
		sys.exit( 1 )
