#!/usr/bin/env python

"""Daemon which watches a configured queue for messages and for each, creates
a SIP."""

import sys
import pika
import logging
import settings
from daemonize import Daemon

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( filename=settings.LOG_FILE, format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "sipd" )

def send_index_message( message ):
	"""Sends a message to the 'index' queue."""
	message = "%s/%s" % ( jobname, timestamp )
	connection = pika.BlockingConnection( pika.ConnectionParameters( settings.INDEX_QUEUE_HOST ) )
	channel = connection.channel()
	channel.queue_declare( queue=settings.INDEX_QUEUE_NAME, durable=True )
	channel.basic_publish( exchange="", routing_key=settings.INDEX_QUEUE_KEY, body=message )
	connection.close()

def callback( ch, method, properties, body ):
	"""Passed a 'jobname/timestamp', creates a SIP. Having created the
	SIP, adds a message to the indexing queue."""
	try:
		logger.info( "Message received: %s." % body )
		pass
#		send_index_message( body )
	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )

class SipDaemon( Daemon ):
	"""Maintains a connection to the queue."""
	def run( self ):
		while True:
			try:
				logger.debug( "Starting connection %s:%s." % ( settings.SIP_QUEUE_HOST, settings.SIP_QUEUE_NAME ) )
				connection = pika.BlockingConnection( pika.ConnectionParameters( settings.SIP_QUEUE_HOST ) )
				channel = connection.channel()
				channel.queue_declare( queue=settings.SIP_QUEUE_NAME, durable=True )
				channel.basic_consume( callback, queue=settings.SIP_QUEUE_NAME, no_ack=True )
				channel.start_consuming()
			except Exception as e:
				logger.error( str( e ) )
 
if __name__ == "__main__":
	"""Sets up the daemon."""
	daemon = SipDaemon( settings.PID_FILE )
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

