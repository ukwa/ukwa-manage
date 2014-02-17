#!/usr/bin/env python

"""Daemon which watches a configured queue for messages and for each, calls a
webservice, storing the result in a WARC file."""

import os
import sys
import pika
import shutil
import logging
import settings
from daemonize import Daemon
from warcwriterpool import WarcWriterPool

logger = logging.getLogger( "harchiverd" )
handler = logging.FileHandler( settings.LOG_FILE )
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s: %(message)s" )
handler.setFormatter( formatter )
logger.addHandler( handler )
logger.setLevel( logging.DEBUG )

#Try to set logging output for all modules.
logging.root.setLevel( logging.DEBUG )
logging.getLogger( "" ).addHandler( handler )

warcwriter = WarcWriterPool( gzip=True, output_dir=settings.OUTPUT_DIRECTORY )

def callback( ch, method, properties, body ):
	"""Passed a URL, passes that URL to a webservice, storing the
	return content in a WARC file."""
	try:
		logger.debug( "Message received: %s." % body )
		if "|" in body:
			#Old-style messages
			url, dir = body.split( "|" )
		else:
			url = body
		har = requests.get( "%s/%s" % ( settings.WEBSERVICE, url ) )
		headers = [
			( WarcRecord.TYPE, WarcRecord.RESOURCE ),
			( WarcRecord.URL, url ),
			( WarcRecord.CONTENT_TYPE, "application/json" ),
			( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
			( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
		]
		warcwriter.write_record( headers, "application/json", har )
	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )

class HarchiverDaemon( Daemon ):
	"""Maintains a connection to the queue."""
	def run( self ):
		while True:
			try:
				if settings.DUMMY:
					logger.debug( "Running in dummy mode." )
				logger.debug( "Starting connection %s:%s." % ( settings.HAR_QUEUE_HOST, settings.HAR_QUEUE_NAME ) )
				connection = pika.BlockingConnection( pika.ConnectionParameters( settings.HAR_QUEUE_HOST ) )
				channel = connection.channel()
				channel.queue_declare( queue=settings.HAR_QUEUE_NAME, durable=True )
				channel.basic_consume( callback, queue=settings.HAR_QUEUE_NAME, no_ack=True )
				channel.start_consuming()
			except Exception as e:
				logger.error( str( e ) )
 
if __name__ == "__main__":
	"""Sets up the daemon."""
	if len( sys.argv ) == 2:
		daemon = HarchiverDaemon( settings.PID_FILE )
	elif len( sys.argv ) == 3:
		daemon = HarchiverDaemon( sys.argv[ 2 ] )
	logger.debug( "Arguments: %s" % sys.argv )
	if len( sys.argv ) == 2:
		if "start" == sys.argv[ 1 ]:
			logger.info( "Starting sipd." )
			daemon.start()
		elif "stop" == sys.argv[ 1 ]:
			logger.info( "Stopping sipd." )
			daemon.stop()
		elif "restart" == sys.argv[ 1 ]:
			logger.info( "Restarting sipd." )
			daemon.restart()
		else:
			print "Unknown command"
			print "usage: %s start|stop|restart" % sys.argv[ 0 ]
			sys.exit( 2 )
		logger.debug( "Exiting." )
		sys.exit( 0 )
	else:
		print "usage: %s start|stop|restart" % sys.argv[ 0 ]
		sys.exit( 2 )
