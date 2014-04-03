#!/usr/bin/env python

"""Daemon which watches a configured queue for messages and for each, creates
a SIP."""

import os
import re
import sip
import sys
import pika
import bagit
import shutil
import logging
import webhdfs
import settings
from daemonize import Daemon

logger = logging.getLogger( "sipd" )
handler = logging.FileHandler( settings.LOG_FILE )
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s: %(message)s" )
handler.setFormatter( formatter )
logger.addHandler( handler )
logger.setLevel( logging.DEBUG )

#Try to set logging output for all modules.
logging.root.setLevel( logging.DEBUG )
logging.getLogger( "" ).addHandler( handler )

def send_error_message( message ):
	"""Sends a message to the 'sip-error' queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( settings.ERROR_QUEUE_HOST ) )
	channel = connection.channel()
	channel.queue_declare( queue=settings.ERROR_QUEUE_NAME, durable=True )
	channel.basic_publish( exchange="",
		routing_key=settings.ERROR_QUEUE_KEY,
		properties=pika.BasicProperties(
			delivery_mode=2,
		),
		body=message )
	connection.close()

def send_index_message( message ):
	"""Sends a message to the 'index' queue."""
	connection = pika.BlockingConnection( pika.ConnectionParameters( settings.SUBMITTED_QUEUE_HOST ) )
	channel = connection.channel()
	channel.queue_declare( queue=settings.SUBMITTED_QUEUE_NAME, durable=True )
	channel.basic_publish( exchange="",
		routing_key=settings.SUBMITTED_QUEUE_KEY,
		properties=pika.BasicProperties(
			delivery_mode=2,
		),
		body=message )
	connection.close()

def verify_message( message ):
	"""Verifies that a message is valid. i.e. it's similar to: 'daily-0400/20140207041736'"""
	r = re.compile( "^[a-z]+(-[0-9])?-([a-z]{3})?[0-9]+/[0-9]+" )
	return r.match( message )

def copy_to_dls( sip ):
	"""Copies a source directory to its destination; skips over errors as
	copying to Windows shares throws numerous exceptions."""
	src = "%s/%s" % ( settings.SIP_ROOT, sip )
	dst = "%s/%s" % ( settings.DLS_DROP, os.path.basename( sip ) )
	try:
		shutil.copytree( src, dst )
	except shutil.Error as s:
		pass
	return dst

def create_sip( job ):
	"""Creates a SIP and returns the path to the folder containing the METS."""
	sip_dir = "%s/%s" % ( settings.SIP_ROOT, job )
	w = webhdfs.API( prefix=settings.WEBHDFS_PREFIX, user=settings.WEBHDFS_USER )
	if os.path.exists( sip_dir ):
		raise Exception( "Directory already exists: %s." % sip_dir )
	if w.exists( "%s.tar.gz" % sip_dir ):
		raise Exception( "SIP already exists in HDFS: %s.tar.gz" % sip_dir )

	s = sip.SipCreator( jobs=[ job ], jobname=job, dummy=settings.DUMMY )
	if s.verifySetup():
		s.processJobs()
		s.createMets()
		filename = os.path.basename( job )
		os.makedirs( sip_dir )
		with open( "%s/%s.xml" % ( sip_dir, filename ), "wb" ) as o:
			s.writeMets( o )
		s.bagit( sip_dir )
	else:
		raise Exception( "Could not verify SIP for %s" % job )
	return sip_dir

def copy_to_hdfs( sip_dir ):
	"""Creates a tarball of a SIP and copies to HDFS."""
	gztar = shutil.make_archive( base_name=sip_dir, format="gztar", root_dir=os.path.dirname( sip_dir ), base_dir=os.path.basename( sip_dir ) )
	w = webhdfs.API( prefix=settings.WEBHDFS_PREFIX, user=settings.WEBHDFS_USER )
	r = w.create( gztar, file=gztar )
	if not r.status_code == 201:
		raise Exception( "Error copying to HDFS: %s" % dir )
	return gztar

def callback( ch, method, properties, body ):
	"""Passed a 'jobname/timestamp', creates a SIP. Having created the
	SIP, adds a message to the indexing queue."""
	try:
		logger.info( "Message received: %s." % body )
		if verify_message( body ):
			sip_dir = create_sip( body )
			logger.debug( "Created SIP: %s" % sip_dir )
			#Create our Bagit.
			bag = bagit.Bag( sip_dir )
			if bag.validate():
				logger.debug( "Moving %s to %s." % ( body, settings.DLS_DROP ) )
				dls = copy_to_dls( body )
				bag = bagit.Bag( dls )
				if bag.validate():
					logger.debug( "Moving %s to %s." % ( dls, settings.DLS_WATCH ) )
					shutil.move( dls, "%s/%s" % ( settings.DLS_WATCH, os.path.basename( body ) ) )
					gztar = copy_to_hdfs( sip_dir )
					logger.debug( "SIP tarball at hdfs://%s" % gztar )
					logger.debug( "Sending message to '%s': %s" % ( settings.SUBMITTED_QUEUE_NAME, body ) )
					send_index_message( body )
				else:
					raise Exception( "Invalid Bagit after copy: %s" % dls )
			else:
				raise Exception( "Invalid Bagit: %s" % sip_dir )
		else:
			raise Exception( "Could not verify message: %s" % body )
	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )
		send_error_message( "%s|%s" % ( body, str( e ) ) )

class SipDaemon( Daemon ):
	"""Maintains a connection to the queue."""
	def run( self ):
		while True:
			try:
				if settings.DUMMY:
					logger.debug( "Running in dummy mode." )
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

