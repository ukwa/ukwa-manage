#!/usr/bin/env python

"""

Watches a configured queue for messages and for each, creates a SIP, and submits it to DLS.

Input messages are like this:

{
    "checkpointDirAbsolutePath": "/jobs/frequent/checkpoints/cp00001-20160126192005",
    "checkpointDirPath": "cp00001-20160126192005",
    "name": "cp00001-20160126192005",
    "shortName": "cp00001"
}

"""

import os
import re
import sys
import json
import pika
import bagit
import shutil
import logging
import webhdfs
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib import sip
from lib.agents import amqp

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )

#
class sipstodls(amqp.QueueConsumer):

	def send_index_message( self, message ):
		"""Sends a message to the 'index' queue."""
		parameters = pika.URLParameters(self.ampq_url)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.queue_declare( queue=settings.SUBMITTED_QUEUE_NAME, durable=True )
		channel.confirm_delivery()
		sent = False
		while not sent:
			sent = channel.basic_publish( exchange="",
			    routing_key=settings.out_queue,
			    properties=pika.BasicProperties(
					content_type='application/json',
				    delivery_mode=2,
			    ),
			    body=message )
		connection.close()
	
	def verify_message( self, path_id ):
		"""Verifies that a message is valid. i.e. it's similar to: 'frequent/cp00001-20140207041736'"""
		r = re.compile( "^[a-z]+/cp[0-9]+-[0-9]+$" )
		return r.match( path_id )
	
	def copy_to_dls( self, sip ):
		"""Copies a source directory to its destination; skips over errors as
		copying to Windows shares throws numerous exceptions."""
		src = "%s/%s" % ( settings.SIP_ROOT, sip )
		dst = "%s/%s" % ( settings.DLS_DROP, os.path.basename( sip ) )
		try:
			shutil.copytree( src, dst )
		except shutil.Error as s:
			logger.exception("ERROR when copying to dls!")
			pass
		return dst
	
	def create_sip( self, job ):
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
	
	def copy_to_hdfs( self, sip_dir ):
		"""Creates a tarball of a SIP and copies to HDFS."""
		gztar = shutil.make_archive( base_name=sip_dir, format="gztar", root_dir=os.path.dirname( sip_dir ), base_dir=os.path.basename( sip_dir ) )
		w = webhdfs.API( prefix=settings.WEBHDFS_PREFIX, user=settings.WEBHDFS_USER )
		r = w.create( gztar, file=gztar )
		if not r.status_code == 201:
			raise Exception( "Error copying to HDFS: %s" % dir )
		return gztar
	
	def callback(self, ch, method, properties, body ):
		"""Passed a 'jobname/cp00000-timestamp', creates a SIP. Having created the
		SIP, adds a message to the indexing queue."""
		try:
			logger.info( "Message received: %s." % body )
			cpm = json.loads(body)
			path_id = "frequent/%s" % cpm['name']
			if self.verify_message( path_id ):
				sip_dir = self.create_sip( path_id )
				logger.debug( "Created SIP: %s" % sip_dir )
				# Create our Bagit.
				bag = bagit.Bag( sip_dir )
				if bag.validate():
					logger.debug( "Moving %s to %s." % ( path_id, settings.DLS_DROP ) )
					dls = self.copy_to_dls( path_id )
					bag = bagit.Bag( dls )
					if bag.validate():
						logger.debug( "Moving %s to %s." % ( dls, settings.DLS_WATCH ) )
						shutil.move( dls, "%s/%s" % ( settings.DLS_WATCH, os.path.basename( path_id ) ) )
						gztar = self.copy_to_hdfs( sip_dir )
						logger.debug( "SIP tarball at hdfs://%s" % gztar )
						logger.debug( "Sending message to '%s': %s" % ( settings.SUBMITTED_QUEUE_NAME, path_id ) )
						self.send_index_message( path_id )
						# It's all gone well. ACK
						ch.basic_ack(delivery_tag = method.delivery_tag)
					else:
						raise Exception( "Invalid Bagit after copy: %s" % dls )
				else:
					raise Exception( "Invalid Bagit: %s" % sip_dir )
			else:
				raise Exception( "Could not verify message: %s" % body )
		except Exception as e:
			logger.error( "%s [%s]" % ( str( e ), body ) )

#
if __name__ == "__main__":
	#
	parser = argparse.ArgumentParser('Take checkpoints from the packaging queue')
	parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
		help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('--num', dest='qos_num', 
		type=int, default=100, help="Maximum number of messages to handle at once. [default: %(default)s]")
	parser.add_argument('--exchange', dest='exchange', default='post-crawl', help="Name of the exchange to use. [default: %(default)s]")
	parser.add_argument('--in_queue', dest='in_queue', default='PC-1-checkpoints-to-package', help="Name of queue to view messages from. [default: %(default)s]")
	parser.add_argument('--out_queue', dest='out_queue', default='PC-2-sips-to-verify', help="Name of queue to push messages to. [default: %(default)s]")
	
	settings = parser.parse_args()
	#
	t = sipstodls(settings.amqp_url, settings.in_queue, "post-crawl", "checkpoints-to-package")
	t.begin()


