#!/usr/local/bin/python2.7

import os
import sys
import pytz
import time
import urllib2
import logging
import argparse
import dateutil.parser
from crontab import CronTab
from datetime import datetime
from xml.dom.minidom import parseString

DAILY_URL = "http://www.webarchive.org.uk/act/websites/export/daily"
DAILY_HOUR = "12"
#SEED_FILE = "/heritrix/seeds-" + time.strftime( "%Y%m%d%H%M%S" ) + ".txt"
SEED_FILE = "/heritrix/daily-seeds.txt"
LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "daily" )
seeds = []

try:
	logger.info( "Exporting from ACT." )
	xml = urllib2.urlopen( DAILY_URL ).read()
except urllib2.URLError, e:
	logger.error( "Cannot read ACT! " + e )
	sys.exit( 1 )

def add_seeds( urls ):
	for url in urls:
		seeds.append( url )
		logger.info( "Adding: " + url )

dom = parseString( xml )
now = datetime.now( pytz.utc )
#crawlDateRange will be:
#	blank			Daily at 12:00
#	"start_date"		Daily at start time specified if start_date < now
#	"start_date end_date"	Daily at start time specified if start_date < now and end_date > now
for node in dom.getElementsByTagName( "node" ):
	date_range = node.getElementsByTagName( "crawlDateRange" )[ 0 ].childNodes[ 0 ].data
	if date_range == "" and str( now.hour ) == DAILY_HOUR:
		add_seeds( node.getElementsByTagName( "urls" )[ 0 ].childNodes[ 0 ].data.split( " " ) )
	elif len( date_range.split( " " ) ) == 2:
		start_date = dateutil.parser.parse( date_range )
		if start_date.hour == now.hour and start_date < now:
			add_seeds( node.getElementsByTagName( "urls" )[ 0 ].childNodes[ 0 ].data.split( " " ) )
	else:
		times = date_range.split( " " )
		start_date = dateutil.parser.parse( times[ 0 ] + " " + times[ 1 ] )
		end_date = dateutil.parser.parse( times[ 2 ] + " " + times[ 3 ] )
		if start_date.hour == now.hour and start_date < now and end_date > now:
			add_seeds( node.getElementsByTagName( "urls" )[ 0 ].childNodes[ 0 ].data.split( " " ) )


#Exit abnormally if there are no relevant seeds.
if len( seeds ) == 0:
	logger.warning( "No relevant seeds; deleting " + SEED_FILE )
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
