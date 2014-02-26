#!/usr/bin/env python
"""Script to retrieve recently-added URLs from ACT and add them to a crawl."""

import sys
import shutil
import logging
import argparse
import requests
from lxml import etree
from datetime import datetime

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
logger = logging.getLogger( "latest-seeds" )

parser = argparse.ArgumentParser( description="Retrieve latest seeds from ACT and " )
parser.add_argument( "-i", dest="input", type=str, required=False )
args = parser.parse_args()

frequencies = [ "daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual", "domaincrawl" ]

URL_PREFIX = "http://www.webarchive.org.uk/act/websites/export"
TIMEOUT = 1200
ROOT = "/opt/heritrix/jobs/latest-20130809093642"
LATEST = "%s/latest.txt" % ROOT
SEEDS = "%s/seeds.txt" % ROOT
SEEDS_ACTION = "%s/action/%s.seeds" % ( ROOT, datetime.now().strftime( "%Y%m%d%H%M%S" ) )

def xml_to_urls( xml ):
	"""Parses XML and returns a list of URLs therein."""
	latest = set()
	dom = etree.fromstring( str( xml ) )
	for node in dom.findall( "node" ):
		urls = node.find( "urls" ).text
		for url in urls.split():
			latest.add( url )
	return latest
	

def get_act_urls():
	"""Generates a list of all URLs in ACT."""
	latest = set()
	xml = None
	if args.input:
		logger.info( "Reading %s." % args.input )
		with open( args.input, "rb" ) as x:
			xml = x.read()
		latest.update( xml_to_urls( xml ) )
	else:
		for frequency in frequencies:
			logger.info( "Requesting /%s" % frequency )
			xml = requests.get( "%s/%s" % ( URL_PREFIX, frequency ), timeout=TIMEOUT ).content
			latest.update( xml_to_urls( xml ) )
	return latest

def get_last_seeds():
	"""Retrieves the last list of seeds from disk."""
	with open( LATEST ) as s:
		seeds = [ line.rstrip() for line in s ]
	return seeds

def get_new_urls( latest, disk ):
	"""Compares the current list if URLs from ACT to those on disk."""
	act = list( latest )
	for url in disk:
		if url in act:
			act.remove( url )
	return act

def write_new_urls( new ):
	"""Writes new URLs to disk; moves them to the Action Directory."""
	with open( SEEDS, "wb" ) as s:
		s.writelines( "\n".join( new ) )
	shutil.copyfile( SEEDS, SEEDS_ACTION )

def write_last_seeds( last ):
	"""Writes the complete list of 'latest' seeds to disk."""
	with open( LATEST, "wb" ) as l:
		l.writelines( "\n".join( last ) )

if __name__ == "__main__":
	try:
		latest = get_act_urls()
	except requests.exceptions.Timeout, t:
		logger.error( str( t ) )
		sys.exit( 1 )
	logger.info( "%s URLs found in ACT." % len( latest ) )
	last_seeds = get_last_seeds()
	logger.info( "%s URLs found from last run." % len( last_seeds ) )
	new = get_new_urls( latest, last_seeds )
	logger.info( "%s new URLs found." % len( new ) )
	if len( new ) > 0:
		write_new_urls( new )
	write_last_seeds( latest )

