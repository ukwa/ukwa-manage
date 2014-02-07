#!/usr/local/bin/python2.7

"""Daemon which watches a configured queue for messages and for each, creates
a SIP."""

import sys
import time
import pika
import gzip
import logging
import urllib2
import settings
import daemonize
from daemonize import Daemon
from datetime import datetime

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "sipd" )

def send_index_message( message ):
	"""Sends a message to the 'index' queue."""
	message = "%s/%s" % ( jobname, timestamp )
	connection = pika.BlockingConnection( pika.ConnectionParameters( INDEX_QUEUE_HOST ) )
	channel = connection.channel()
	channel.queue_declare( queue=INDEX_QUEUE_NAME, durable=True )
	channel.basic_publish( exchange="", routing_key=INDEX_QUEUE_KEY, body=message )
	connection.close()

def callback( ch, method, properties, body ):
	"""Passed a 'jobname/timestamp', creates a SIP. Having created the
	SIP, adds a message to the indexing queue."""
	try:
		pass
		send_index_message( body )
	except Exception as e:
		sys.stderr.write( "ERROR: " + str( e ) + "\n" )

class SipDaemon( Daemon ):
	"""Maintains a connection to the queue."""
	def run( self ):
		while True:
			connection = pika.BlockingConnection( pika.ConnectionParameters( settings.SIP_QUEUE_HOST ) )
			channel = connection.channel()
			channel.queue_declare( queue=settings.SIP_QUEUE_NAME, durable=True )
			channel.basic_consume( callback, queue=settings.SIP_QUEUE_NAME, no_ack=True )
			channel.start_consuming()
			except Exception as e:
				sys.stderr.write( "ERROR: " + str( e ) + "\n" )
 
if __name__ == "__main__":
	"""Sets up the daemon."""
	daemon = ReceiveDaemon( "/tmp/daemon-example.pid" )
	if len( sys.argv ) == 2:
		if "start" == sys.argv[ 1 ]:
			daemon.start()
		elif "stop" == sys.argv[ 1 ]:
			daemon.stop()
		elif "restart" == sys.argv[ 1 ]:
			daemon.restart()
		else:
			print "Unknown command"
			sys.exit( 2 )
		sys.exit( 0 )
	else:
		print "usage: %s start|stop|restart" % sys.argv[ 0 ]
		sys.exit( 2 )
