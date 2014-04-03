#!/usr/bin/env python

"""
Various SIPs are stored in gzipped tarfiles in HDFS. This script will pull
a single message from a queue, verify that the corresponding SIP exists in
HDFS, retrieve it, parse the METS file for ARK identifiers and verify that
each ARK is available in DLS. If so, the corresponding SIP is flagged for 
indexing.
"""

import os
import re
import pika
import logging
import tarfile
import webhdfs
import dateutil.parser
from lxml import etree
from StringIO import StringIO
from datetime import datetime, timedelta

SIPS="/heritrix/sips"
METS={ "mets": "http://www.loc.gov/METS/" }
PREMIS={ "premis": "info:lc/xmlns/premis-v2" }
QUEUE_HOST="194.66.232.93"
SIP_QUEUE="sip-submitted"
ERROR_QUEUE="sip-error"
INDEX_QUEUE="index"
DLS_LOOKUP="/heritrix/sips/dls-export/Public Web Archive Access Export.txt"
SELECTIVE_PREFIX="f6ivc2"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT )
logger = logging.getLogger( "verify" )
logger.setLevel( logging.INFO )
logging.getLogger( "pika" ).setLevel( logging.ERROR )

w = webhdfs.API( prefix="http://194.66.232.90:14000/webhdfs/v1" )

def get_available_arks():
	"""Reads relevant ARKs from a text file."""
	with open( DLS_LOOKUP, "rb" ) as i:
		return [ l.split()[ 0 ] for l in i if not l.startswith( SELECTIVE_PREFIX ) ]
	

def get_message():
	"""Pulls a single message from a queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( QUEUE_HOST ) )
	channel = connection.channel()
	method_frame, header_frame, body = channel.basic_get( SIP_QUEUE )
	if method_frame:
		logger.debug( "%s, %s, %s" % ( method_frame, header_frame, body ) )
		channel.basic_ack( method_frame.delivery_tag )
		connection.close()
		return body
	connection.close()

def isvalid( message ):
	"""Verifies that a message is valid. i.e. it's similar to: 'daily-0400/20140207041736'"""
        r = re.compile( "^[^/]+/[0-9]+" )
        return r.match( message )

def outside_embargo( message ):
	"""Checks whether a message is outside the week embargo."""
	timestamp = message.split( "/" )[ 1 ]
	date = dateutil.parser.parse( timestamp )
	return date < ( datetime.now() - timedelta( days=7 ) )

def requeue( message, queue ):
	"""Puts a message back on the appropriate queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( QUEUE_HOST ) )
	channel = connection.channel()
	channel.queue_declare( queue=queue, durable=True )
	channel.tx_select()
	channel.basic_publish( exchange="",
		routing_key=queue,
		properties=pika.BasicProperties(
			delivery_mode=2,
		),
		body=message
	)
	channel.tx_commit()
	connection.close()

def get_identifiers( sip ):
	"""Parses the SIP in HDFS and retrieves ARKs."""
	arks = []
	tar = "%s/%s.tar.gz" % ( SIPS, sip )
	if w.exists( tar ):
		logger.debug( "Found %s" % tar )
		t = w.open( tar )
		tar = tarfile.open( mode="r:gz", fileobj=StringIO( t ) )
		for i in tar.getmembers():
			if i.name.endswith( ".xml" ):
				xml = tar.extractfile( i ).read()
				tree = etree.fromstring( xml )
				for warc in tree.xpath( "//premis:object[premis:objectCharacteristics/premis:format/premis:formatDesignation/premis:formatName='application/warc']", namespaces=PREMIS ):
					for id in warc.xpath( "premis:objectIdentifier/premis:objectIdentifierValue", namespaces=PREMIS ):
						arks.append( id.text.replace( "ark:/81055/", "" ) )
	else:
		logger.warning( "Could not find SIP: hdfs://%s" % tar )
	return arks

if __name__ == "__main__":
	if not os.path.exists( DLS_LOOKUP ) and os.access( DLS_LOOKUP, os.R_OK ):
		logger.error( "Cannot read DLS lookup: %s" % DLS_LOOKUP )
		sys.exit( 1 )

	dls_arks = get_available_arks()
	if len( dls_arks ) == 0:
		logger.error( "No DLS ARKs found!" )
		sys.exit( 1 )

	seen = []
	message = get_message()
	while message is not None and message not in seen:
		message = message.strip()
		logger.debug( "Received message: %s" % message )
		if isvalid( message ):
			if outside_embargo( message ):
				arks = get_identifiers( message )
				if len( arks ) > 0:
					all_arks_available = True
					for ark in arks:
						all_arks_available = ( all_arks_available and ( ark in dls_arks ) )
					if all_arks_available:
						requeue( message, INDEX_QUEUE )
						logger.debug( "OK to index: %s" % message )
					else:
						seen.append( message )
						requeue( message, SIP_QUEUE )
						logger.info( "ARKs not available: %s" % message )
				else:
					logger.warning( "No ARKs found for %s" % message )
			else:
				seen.append( message )
				requeue( message, SIP_QUEUE )
				logger.info( "Inside embargo period: %s" % message )
		else:
			requeue( "%s|%s" % ( message, "Invalid message." ), ERROR_QUEUE )
		message = get_message()
	if message is not None:
		requeue( message, SIP_QUEUE )

