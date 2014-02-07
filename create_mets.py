#!/usr/bin/env python

"""Given a series of 'jobnames' of the form 'job/launchid', will create a METS
file and form a Bagit folder for said METS."""

import os
import sys
import glob
import bagit
import socket
import logging
import urllib2
import argparse
import commands
import simplejson
from settings import *
from lxml import etree
from datetime import datetime
from dateutil.parser import parse
from xml.dom.minidom import parseString
from domain.mets import createDomainMets
from crawler.mets import Warc, createCrawlerMets, ZipContainer
from crawler.settings import HOOP, HOOP_HOST, HOOP_STATUS_SUFFIX

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger.addHandler( logging.StreamHandler( sys.stdout ) )
logger = logging.getLogger( "ldp06.mets" )

parser = argparse.ArgumentParser( description="Create METS files." )
parser.add_argument( "jobs", metavar="J", type=str, nargs="+", help="Heritrix job name" )
args = parser.parse_args()

def getIdentifiers( num ):
	try:
		request = urllib2.Request( ARK_URL + str( num ), "" )
		data = urllib2.urlopen( request ).read()
	except Exception as e:
		logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Could not obtain ARKs: " + str( e ) )
		sys.exit( 1 )
	xml = parseString( data )
	for ark in xml.getElementsByTagName( "ark" ):
		identifiers.append( ark.firstChild.wholeText )
	if( len( identifiers ) != num ):
		logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Incorrect number of ARKs returned." )
		logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Dumping to arks.dump." )
		dump = open( "arks.dump", "wb" )
		dump.writelines( identifiers )
		dump.close()
		sys.exit( 1 )
	return identifiers

def hdfsExists( path ):
	url = HOOP + path + HOOP_STATUS_SUFFIX
	response = ""
	try:
		response = urllib2.urlopen( url )
		data = response.read()
	except urllib2.HTTPError:
		data = "{}"
	except urllib2.URLError:
		data = "{}"
	json = simplejson.loads( data )
	return json.has_key( "FileStatus" )

logger.info( "Resolving WebHDFS host..." )
try:
	socket.gethostbyname( HOOP_HOST )
except socket.error:
	logger.error( "Cannot resolve host: " + HOOP_HOST )
	sys.exit( 1 )
logger.info( "Verifying WebHDFS service..." )
if not hdfsExists( "/" ):
	logger.error( "Cannot connect to WebHDFS service: " + HOOP_HOST )
	sys.exit( 1 )
logger.info( "Verifying Zip binaries..." )
if not os.access( ZIP.split()[ 0 ], os.X_OK ) or not os.access( UNZIP, os.X_OK ):
	logger.error( "Cannot find Zip binaries." )
	sys.exit( 1 )

jobname = datetime.now().strftime( "%Y%m%d%H%M%S" )
if len( args.jobs ) == 1:
	jobname = args.jobs[ 0 ] + "/" + jobname
os.makedirs( jobname )
warcs = []
viral = []
logs = []
startdate = datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" )

def findStartDate( logs ):
	timestamps = []
	for log in logs:
		with open( log, "rb" ) as l:
			fields = l.readline().split()
			if len( fields ) > 0:
				timestamps.append( parse( fields[ 0 ] ) )
	timestamps.sort()
	return timestamps[ 0 ].strftime( "%Y-%m-%dT%H:%M:%SZ" )

for job in args.jobs:
	logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Processing job %s..." % job )
	for warc in glob.glob( WARC_ROOT + "/" + job + "/*.warc.gz" ):
		logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Found %s..." % os.path.basename( warc ) )
		warcs.append( Warc( warc, job ) )

	for warc in glob.glob( VIRAL_ROOT + "/" + job + "/*.warc.gz" ):
		logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Found %s..." % os.path.basename( warc ) )
		viral.append( Warc( warc, job ) )

	try:
		zip = LOG_ROOT + "/" + job + "/" + job + ".zip"
		if os.path.exists( zip ):
			logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Deleting %s..." % zip )
			try:
				os.remove( zip )
			except Exception as e:
				logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Cannot delete %s..." % zip )
				sys.exit( 1 )

		crawl_logs = glob.glob( LOG_ROOT + "/" + job + "/crawl.log*" )
		startdate = findStartDate( crawl_logs )
		log_container = ZipContainer( zip, job )
		for crawl_log in crawl_logs:
			logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Found %s..." % os.path.basename( crawl_log ) )
			commands.getstatusoutput( ZIP + " " + zip + " " + crawl_log )

		for log in glob.glob( LOG_ROOT + "/" + job + "/*-errors.log*" ):
			logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Found %s..." % os.path.basename( log ) )
			commands.getstatusoutput( ZIP + " " + zip + " " + log )
		if os.path.exists( JOBS_ROOT + job + "/crawler-beans.cxml" ):
			logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Found config..." )
			commands.getstatusoutput( ZIP + " " + zip + " " + JOBS_ROOT + job + "/latest/crawler-beans.cxml" )
		else:
			logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] ERROR: Cannot find config." )
			sys.exit( 1 )
	except Exception as e:
		logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Problem building ZIP file: " + str( e ) )
		sys.exit( 1 )
	finally:
		log_container.updateMetadata()
	if hdfsExists( zip ):
		logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Deleting hdfs://%s..." % zip )
		commands.getstatusoutput( "hadoop fs -rm " + zip )
		if hdfsExists( zip ):
			logger.error( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Cannot delete hdfs://%s..." % zip )
			sys.exit( 1 )
	logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Copying " + os.path.basename( zip ) + " to HDFS." )
	commands.getstatusoutput( "hadoop fs -put " + zip + " " + zip )
	logs.append( log_container )

logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Getting ARK identifiers..." )
identifiers = getIdentifiers( len( warcs ) + len( viral ) + len( logs ) )

logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Building domain-level METS..." )
mets = createDomainMets( startdate )

logger.info( "[" + datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) + "] Building METS..." )
mets = createCrawlerMets( mets, warcs, viral, logs, identifiers )

output = open( "%s/%s.xml" % ( OUTPUT_ROOT, jobname ), "wb" )
try:
	output.write( etree.tostring( mets, pretty_print=True, xml_declaration=True, encoding="UTF-8" ) )
finally:
	output.close()
bagit.make_bag( "%s/%s" % ( OUTPUT_ROOT, jobname ), { "Contact-Name": BAGIT_CONTACT_NAME, "Contact-Email": BAGIT_CONTACT_EMAIL, "Timestamp": datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ), "Description": BAGIT_DESCRIPTION + ";".join( args.jobs ) } )
