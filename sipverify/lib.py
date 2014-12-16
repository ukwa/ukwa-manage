#!/usr/bin/env python

"""
Generic methods used for verifying/indexing SIPs.
"""

import re
import pika
import logging
import tarfile
import webhdfs
from lxml import etree
from StringIO import StringIO

SIP_ROOT="/heritrix/sips"
NS={ "mets": "http://www.loc.gov/METS/", "premis": "info:lc/xmlns/premis-v2" }
XLINK="{http://www.w3.org/1999/xlink}"
WEBHDFS="http://194.66.232.90:14000/webhdfs/v1"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT )
logger = logging.getLogger( "sipverify.lib" )
logger.setLevel( logging.INFO )
logging.getLogger( "pika" ).setLevel( logging.ERROR )

def get_message( host, queue ):
	"""Pulls a single message from a queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( host ) )
	channel = connection.channel()
	method_frame, header_frame, body = channel.basic_get( queue )
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

def put_message( host, queue, message ):
	"""Puts a message back on the appropriate queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( host ) )
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
	w = webhdfs.API( prefix=WEBHDFS )
	arks = []
	tar = "%s/%s.tar.gz" % ( SIP_ROOT, sip )
	if w.exists( tar ):
		logger.debug( "Found %s" % tar )
		t = w.open( tar )
		tar = tarfile.open( mode="r:gz", fileobj=StringIO( t ) )
		for i in tar.getmembers():
			if i.name.endswith( ".xml" ):
				xml = tar.extractfile( i ).read()
				tree = etree.fromstring( xml )
				for warc in tree.xpath( "//premis:object[premis:objectCharacteristics/premis:format/premis:formatDesignation/premis:formatName='application/warc']", namespaces=NS ):
					for id in warc.xpath( "premis:objectIdentifier/premis:objectIdentifierValue", namespaces=NS ):
						arks.append( id.text.replace( "ark:/81055/", "" ) )
	else:
		logger.warning( "Could not find SIP: hdfs://%s" % tar )
	return arks

def get_warc_identifiers( sip ):
	"""Parses the SIP in HDFS and retrieves WARC/ARK tuples."""
	w = webhdfs.API( prefix=WEBHDFS )
	identifiers = []
	tar = "%s/%s.tar.gz" % ( SIP_ROOT, sip )
	if w.exists( tar ):
		logger.debug( "Found %s" % tar )
		t = w.open( tar )
		tar = tarfile.open( mode="r:gz", fileobj=StringIO( t ) )
		for i in tar.getmembers():
			if i.name.endswith( ".xml" ):
				xml = tar.extractfile( i ).read()
				tree = etree.fromstring( xml )
				for warc in tree.xpath( "//mets:file[@MIMETYPE='application/warc']", namespaces=NS ):
					try:
						admid = warc.attrib[ "ADMID" ]
						amdsec = tree.xpath( "//mets:amdSec[@ID='%s']" % admid, namespaces=NS )[ 0 ]
						oiv = amdsec.xpath( "mets:digiprovMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectIdentifier/premis:objectIdentifierValue", namespaces=NS )[ 0 ]
						path = re.findall( "^.+(/heritrix.+\.warc\.gz)\?.+$", warc.xpath( "mets:FLocat", namespaces=NS )[ 0 ].attrib[ "%shref" % XLINK ] )[ 0 ]
						identifiers.append( ( path, oiv.text ) )
					except IndexError as i:
						logger.error( "Problem parsing METS for SIP: %s" % sip )
	else:
		logger.warning( "Could not find SIP: hdfs://%s" % tar )
	return identifiers

