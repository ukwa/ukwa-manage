#!/usr/bin/env python

"""
Various SIPs are stored in gzipped tarfiles in HDFS. This script will pull
a single message from a queue, verify that the corresponding SIP exists in
HDFS, retrieve it, parse the METS file for ARK identifiers and verify that
each ARK is available in DLS. If so, the corresponding SIP is flagged for 
indexing.
"""

import re
import pika
import logging
import tarfile
import webhdfs
import requests
import dateutil.parser
from StringIO import StringIO
from datetime import datetime, timedelta

SIPS="/heritrix/sips"
METS={ "mets": "http://www.loc.gov/METS/" }
PREMIS={ "premis": "info:lc/xmlns/premis-v2" }
QUEUE_HOST="194.66.232.93"
SIP_QUEUE="sip-submitted"
INDEX_QUEUE="index"
DLS="http://DLS-BSP-AC01"
ARK_REGEX="<premis:objectIdentifierValue>(ark[^<]+)</premis:objectIdentifierValue>"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "verify" )

w = webhdfs.API( prefix="http://194.66.232.90:14000/webhdfs/v1" )

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
	channel.basic_publish( exchange="",
		routing_key=queue,
		properties=pika.BasicProperties(
			delivery_mode=2,
		),
		body=message
	)
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
				xml = t.extractfile( i ).read()
				arks += re.findall( ARK_REGEX, xml )
	else:
		logger.warning( "Could not find SIP: hdfs://%s" % tar )
	return arks

def check_availability( ark ):
	"""Verifies that an ARK is available in DLS."""
	r = requests.head( "%s/%s" % ( DLS, ark ) )
	return r.ok

if __name__ == "__main__":
	message = get_message( SIP_QUEUE )
	while message is not None:
		if isvalid( message ) and outside_embargo( message ):
			arks = get_identifiers( message )
			if len( arks ) > 0:
				all_arks_available = True
				for ark in arks:
					all_arks_available = ( all_arks_available and check_availability( ark ) )
				if all_arks_available:
					requeue( message, INDEX_QUEUE )
				else:
					requeue( message, SIP_QUEUE )
			else:
				logger.warning( "No ARKs found for %s" % message )
		message = get_message( SIP_QUEUE )
