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
from datetime import date
from xml.dom.minidom import parseString

# Set globals
#QUARTERLY_URL = "http://www.webarchive.org.uk/act/websites/export/quarterly"
QUARTERLY_URL = "http://opera.bl.uk/quarterlytest"
QUARTERLY_DAY = 1
QUARTERLY_MONTH = 1
QUARTERLY_HOUR = 12
SEED_FILE = "/heritrix/quarterly-seeds.txt"
LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

# Initialise
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "quarterly" )
a_seeds = []
o_now = datetime.now()
i_nowmonth = o_now.month

# Get quarterly export list
try:
	s_xml = urllib2.urlopen( QUARTERLY_URL ).read()
except urllib2.URLError, e:
	logger.error( "Cannot read ACT! " + str( e ) )
	sys.exit( 1 )
o_dom = parseString( s_xml )

# add_seeds function ---------------------------------------------------------
def add_seeds( s_urls ):
	for s_url in s_urls:
		try:
			parsed = rfc3987.parse( s_url, rule="URI" )
			a_seeds.append( s_url )
			logger.info( "Adding: " + s_url )
		except ValueError, v:
			logger.error( "INVALID URL: " + s_url )
# ----------------------------------------------------------------------------

# for each site in export list, extract urls within the crawl date range
for o_node in o_dom.getElementsByTagName( "node" ):
	s_start_date = s_end_date = ""
	i_crawlmonth = QUARTERLY_MONTH

	# Check node has correct quarterly crawlFrequency
	if o_node.getElementsByTagName("crawlFrequency")[0].firstChild.nodeValue != 'quarterly':
		logger.error("Non quarterly node found in quarterly export list. Node " + o_node.getElementsByTagName("actLink")[0].firstChild.nodeValue)
		continue

	# Get start and end crawl date strings
	if len( o_node.getElementsByTagName( "crawlStartDate" )[ 0 ].childNodes ) > 0:
		s_start_date = o_node.getElementsByTagName( "crawlStartDate" )[ 0 ].firstChild.nodeValue
	if len( o_node.getElementsByTagName( "crawlEndDate" )[ 0 ].childNodes ) > 0:
		s_end_date = o_node.getElementsByTagName( "crawlEndDate" )[ 0 ].firstChild.nodeValue

	o_start_date = dateutil.parser.parse(s_start_date)
	o_end_date = dateutil.parser.parse(s_end_date)

	# Skip if outside crawl date range
	if s_start_date != "" and o_start_date > o_now:
		continue
	elif s_end_date != "" and o_end_date < o_now:
		continue
	# Skip if not crawl day
	elif s_start_date != "" and o_start_date.day != o_now.day:
		continue
	elif s_start_date == "" and o_now.day != QUARTERLY_DAY:
		continue
	else:
		# Skip if not crawl month
		if s_start_date != "":
			i_crawlmonth = o_start_date.month

		i_modcrawlmonth = i_crawlmonth%3
		i_modnowmonth = i_nowmonth%3

		if i_modcrawlmonth != i_modnowmonth:
			continue

		else:
			if s_start_date == "":
				if o_now.hour == QUARTERLY_HOUR:
					add_seeds( o_node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
				else:
					continue

			elif o_start_date.hour == o_now.hour:
				add_seeds( o_node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )


#Exit abnormally if there are no relevant seeds.
if len( a_seeds ) == 0:
	logger.warning( "No relevant seeds" )
	sys.exit( 1 )
else:
	try:
		output = open( SEED_FILE, "wb" )
		logger.info( "Writing seeds to " + SEED_FILE )
		for seed in a_seeds:
			output.write( seed + "\n" )
		output.close()
		logger.info( "Recorded " + str( len( a_seeds ) ) + " seeds." )
	except IOError, i:
		logger.warning( "Problem writing seeds to " + SEED_FILE )
		sys.exit( 1 )
