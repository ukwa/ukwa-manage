#!/usr/local/bin/python2.7

import os
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

frequencies = [ "daily", "weekly", "monthly", "quarterly", "sixmonthly" ]

DEFAULT_HOUR = 12
DEFAULT_DAY = 8
DEFAULT_WEEKDAY = 0
DEFAULT_MONTH = 1

for frequency in frequencies:
	URL_ROOT = "http://www.webarchive.org.uk/act/websites/export/"
	SEED_FILE = "/heritrix/" + frequency + "-seeds.txt"
	LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

	logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
	logger = logging.getLogger( "frequency" )
	seeds = []

	try:
		xml = urllib2.urlopen( URL_ROOT + frequency ).read()
	except urllib2.URLError, e:
		logger.error( "Cannot read ACT! " + str( e ) )
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
	#	blank			Determined by frequency.
	#	"start_date"		Determined by frequency if start_date < now.
	#	"start_date end_date"	Determined by frequency if start_date < now and end_date > now
	for node in dom.getElementsByTagName( "node" ):
		seeds = []
		start_date = end_date = ""
		if len( node.getElementsByTagName( "crawlStartDate" )[ 0 ].childNodes ) > 0:
			start_date = node.getElementsByTagName( "crawlStartDate" )[ 0 ].firstChild.nodeValue
		if len( node.getElementsByTagName( "crawlEndDate" )[ 0 ].childNodes ) > 0:
			end_date = node.getElementsByTagName( "crawlEndDate" )[ 0 ].firstChild.nodeValue

		if end_date == "" or dateutil.parser.parse( end_date ) > now:
			if start_date == "":
				start_date = now
				hotd = DEFAULT_HOUR
				dotw = DEFAULT_WEEKDAY
				dotm = DEFAULT_DAY
				moty = DEFAULT_MONTH
			else:
				start_date = dateutil.parser.parse( start_date )
				hotd = start_date.hour
				dotw = start_date.weekday()
				dotm = start_date.day
				moty = start_date.month

			if start_date <= now:
				if hotd == now.hour:
					if frequency == "daily": 
						add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
					if frequency == "weekly" and dotw = now.weekday():
						add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
					if dotm = now.day:
						if frequency == "monthly":
							add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
						if frequency == "quarterly" and moty%3 == now.month%3:
							add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
						if frequency == "sixmonthly" and moty%6 == now.month%6:
							add_seeds( node.getElementsByTagName( "urls" )[ 0 ].firstChild.nodeValue.split( " " ) )
		if len( seeds ) > 0:
			logger.info( "Test for frequency: " + frequency )
			for seed in seeds:
				logger.info( "\t" + seed )
#			try:
#				output = open( SEED_FILE, "wb" )
#				logger.info( "Writing seeds to " + SEED_FILE )
#				for seed in seeds:
#					output.write( seed + "\n" )
#				output.close()
#				logger.info( "Found " + str( len( seeds ) ) + " seeds." )
#			except IOError, i:
#				logger.warning( "Problem writing seeds to " + SEED_FILE )
