#!/usr/bin/env python

"""

Watches a configured queue for messages and for each, creates a SIP.

Input messages are like this:

{
    "checkpointDirAbsolutePath": "/jobs/frequent/checkpoints/cp00001-20160229142814",
    "checkpointDirPath": "cp00001-20160229142814",
    "name": "cp00001-20160229142814",
    "shortName": "cp00001"
}

"""

import os
import re
import sys
import json
import pika
import glob
import gzip
import bagit
import shutil
import logging
import argparse
import subprocess
from dateutil.parser import parse
from pywebhdfs.webhdfs import PyWebHdfsClient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib import sip
from lib.agents import amqp

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s@%(lineno)d : %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.handlers = []
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.DEBUG )
logging.getLogger( 'pika' ).setLevel(logging.ERROR)
logging.getLogger( 'requests' ).setLevel(logging.WARNING)

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.DEBUG )

class CheckpointSipPreparer():
	
	def __init__( self, jobname, checkpoint, args):
		"""Takes the checkpoint info and sets up data needed to build the SIP."""
		self.hdfs = PyWebHdfsClient( host=args.webhdfs_host, port=args.webhdfs_port, user_name=args.webhdfs_user, timeout=100 )
		self.WARC_ROOT="/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/warcs"
		self.IMAGE_ROOT="/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/viral"
		self.VIRAL_ROOT="/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/images"
		self.LOG_ROOT="/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/logs"
		self.JOBS_ROOT="/Users/andy/Documents/workspace/wren/compose-dev-crawler/jobs"
		# Need zip 3.0 and unzip 6.0:
		self.zip="/usr/bin/zip -jg"
		self.unzip="/usr/bin/unzip"
		self.verifyBinaries()
		self.verifyWebhdfs()
		#
		self.upload_to_hdfs = True
		#
		self.jobname = jobname
		self.checkpoint = checkpoint
		self.sip_dir = "%s/%s/%s-%s" % ( args.sip_root, jobname, jobname, checkpoint )
		if os.path.exists( self.sip_dir ):
			if args.overwrite:
				logger.info("Removing SIP directory %s" % self.sip_dir)
				shutil.rmtree(self.sip_dir)
			else:
				raise Exception( "Directory already exists: %s." % self.sip_dir )
		self.crawl_log = self.get_crawl_log()
		self.start_date = self.file_start_date([self.crawl_log])
		# This should bundle up the files into a ZIP and use that instead. i.e. as getLogs below but done right.
		self.get_logs_as_zip()
		# Find the WARCFilename to AboluteFilepath mapping:
		self.parse_crawl_log(self.crawl_log)
		# Calculate checksums for WARCs
		# And copy up to HDFS
		self.copy_warcs_to_hdfs()
		
	def get_crawl_log( self ):
		# First, parse the crawl log(s) and determine the WARC file names:
		logger.info("Looking for crawl logs...")
		logfilepath = "%s/%s/crawl.log.%s" % ( self.LOG_ROOT, self.jobname, self.checkpoint )
		if os.path.exists(logfilepath):
			logger.info( "Found %s..." % os.path.basename( logfilepath ) )
			return logfilepath
		
	def file_start_date( self, logs ):
		"""Finds the earliest timestamp in a series of log files."""
		timestamps = []
		for log in logs:
			if log.endswith(".gz"):
				with gzip.open(log, "rb") as l:
					fields = l.readline().split()
					if len(fields) > 0:
						timestamps.append(parse(fields[0]))
			else:
				with open( log, "rb" ) as l:
					fields = l.readline().split()
					if len( fields ) > 0:
						timestamps.append( parse( fields[ 0 ] ) )
		timestamps.sort()
		return timestamps[ 0 ].strftime( "%Y-%m-%dT%H:%M:%SZ" )
	
	def warc_file_path(self,warcfile):
		return "%s/%s/%s" % (self.WARC_ROOT, self.jobname, warcfile)
			
	def viral_file_path(self,warcfile):
		return "%s/%s/%s" % (self.VIRAL_ROOT, self.jobname, warcfile)
			
	def parse_crawl_log(self, crawl_log):
		# Get the WARC filenames
		warcfiles = set()
		with open(crawl_log,'r') as f:
			for line in f:
				parts = re.split(" +",line,maxsplit=13)
				warcfiles.add(json.loads(parts[12])['warcFilename'])
		# Search for the actual files and return absolute paths:
		self.warc_lookup = {}
		self.warcs = []
		self.viral = []
		for warcfile in warcfiles:
			if os.path.exists(self.warc_file_path(warcfile)):
				logger.info("Found WARC %s" % self.warc_file_path(warcfile))
				self.warc_lookup[warcfile] = self.warc_file_path(warcfile)
				self.warcs.append(self.warc_file_path(warcfile))
			elif os.path.exists(self.viral_file_path(warcfile)):
				logger.info("Found Viral WARC %s" % self.viral_file_path(warcfile))
				self.viral.append(self.viral_file_path(warcfile))
			else:
				raise Exception("Cannot file warc file %s" % warcfile)
	
	def copy_warcs_to_hdfs(self):
		for warc in self.warcs:
			self.copy_warc_to_hdfs(warc)
			
			
	def copy_warc_to_hdfs(self, warc_path):
		logger.info("Attempting to copy '%s' to HDFS...", warc_path)
		if self.hdfs.exists_file_dir(warc_path) and not args.overwrite:
			logger.warn("Path %s already exists!" % warc_path)
			self.hdfs.delete_file_dir(warc_path)
		with open(warc_path) as file_data:
			self.hdfs.create_file( warc_path, file_data, overwrite=args.overwrite )
		logger.info("Copy completed.")

	def get_logs_as_zip( self ):
		"""Zips up all log/config. files and copies said archive to HDFS; finds the
		earliest timestamp in the logs."""
		self.logs = []
		try:
			zip_path = "%s/%s/%s-%s.zip" % ( self.LOG_ROOT, self.jobname, os.path.basename( self.jobname ), self.checkpoint )
			if os.path.exists( zip_path ):
				logger.info( "Deleting %s..." % zip_path )
				try:
					os.remove( zip_path )
				except Exception as e:
					logger.error( "Cannot delete %s..." % zip_path )
					raise Exception( "Cannot delete %s..." % zip_path )
			logger.info( "Zipping logs to %s" % zip_path )
			for crawl_log in glob.glob( "%s/%s/crawl.log.%s" % ( self.LOG_ROOT, self.jobname, self.checkpoint ) ):
				logger.info( "Found %s..." % os.path.basename( crawl_log ) )
				logger.info( self.zip.split() + [ zip_path, crawl_log ] )
				subprocess.check_output( self.zip.split() + [ zip_path, crawl_log ] )

			for log in glob.glob( "%s/%s/*-errors.log.%s" % ( self.LOG_ROOT, self.jobname, self.checkpoint ) ):
				logger.info( "Found %s..." % os.path.basename( log ) )
				subprocess.check_output( self.zip.split() + [ zip_path, log ] )

			for txt in glob.glob( "%s/%s/*.txt" % ( self.JOBS_ROOT, self.jobname ) ):
				logger.info( "Found %s..." % os.path.basename( txt ) )
				subprocess.check_output( self.zip.split() + [ zip_path, txt ] )

			if os.path.exists( "%s/%s/crawler-beans.cxml" % ( self.JOBS_ROOT, self.jobname ) ):
				logger.info( "Found config..." )
				subprocess.check_output( self.zip.split() + [ zip_path, "%s/%s/crawler-beans.cxml" % ( self.JOBS_ROOT, self.jobname ) ] )
			else:
				logger.error( "Cannot find config." )
				raise Exception( "Cannot find config." )
		except Exception as e:
			logger.error( "Problem building ZIP file: %s" % str( e ) )
			logger.exception(e)
			raise Exception( "Problem building ZIP file: %s" % str( e ) )
		if self.upload_to_hdfs:
			if self.hdfs.exists_file_dir( zip_path ):
				logger.info( "Deleting hdfs://%s..." % zip_path )
				self.hdfs.delete_file_dir( zip_path )
			logger.info( "Copying %s to HDFS." % os.path.basename( zip_path ) )
			with open(zip_path) as file_data:
				self.hdfs.create_file( zip_path, file_data, overwrite=args.overwrite )
		self.logs.append( zip_path )
		

	def verifyBinaries( self ):
		"""Verifies that the Zip binaries are present and executable. Need zip >= 3.0 unzip >= 6.0 other zip binaries that supports ZIP64."""
		logger.info( "Verifying Zip binaries..." )
		if not os.access( self.zip.split()[ 0 ], os.X_OK ) or not os.access( self.unzip, os.X_OK ):
			logger.error( "Cannot find Zip binaries." )
			raise Exception( "Cannot find Zip binaries." )
		return True

	def verifyWebhdfs( self ):
		"""Verifies that the WebHDFS services is available."""
		logger.info( "Verifying WebHDFS service..." )
		return self.hdfs.exists_file_dir( "/" )


#
class sipstodls(amqp.QueueConsumer):

	def send_submit_message( self, message ):
		"""Sends a message to the 'index' queue."""
		parameters = pika.URLParameters(self.ampq_url)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.queue_declare( queue=args.out_queue, durable=True )
		channel.tx_select()
		channel.basic_publish( exchange="",
			routing_key=args.out_queue,
			properties=pika.BasicProperties(
				delivery_mode=2,
			),
			body=message )
		channel.tx_commit()
		connection.close()
	
	def verify_message( self, path_id ):
		"""Verifies that a message is valid. i.e. it's similar to: 'frequent/cp00001-20140207041736'"""
		r = re.compile( "^[a-z]+/cp[0-9]+-[0-9]+$" )
		return r.match( path_id )
	
	def create_sip( self, jobname, cpp ):
		"""Creates a SIP and returns the path to the folder containing the METS."""
		sip_dir = cpp.sip_dir
		if self.hdfs.exists_file_dir( "%s.tar.gz" % sip_dir ) and not args.overwrite:
			raise Exception( "SIP already exists in HDFS: %s.tar.gz" % sip_dir )
	
		s = sip.SipCreator( [jobname], jobname, warcs=cpp.warcs, viral=cpp.viral, logs=cpp.logs, start_date=cpp.start_date, args=args )
		if s.verifySetup():
			s.processJobs()
			s.createMets()
			if not os.path.exists(sip_dir):
				os.makedirs( sip_dir )
			with open( "%s/%s-%s.xml" % ( sip_dir, jobname, cpp.checkpoint ), "wb" ) as o:
				s.writeMets( o )
			s.bagit( sip_dir )
		else:
			raise Exception( "Could not verify SIP for %s" % sip_dir )
		return sip_dir
	
	def copy_sip_to_hdfs( self, sip_dir ):
		"""Creates a tarball of a SIP and copies to HDFS."""
		gztar = shutil.make_archive( base_name=sip_dir, format="gztar", root_dir=os.path.dirname( sip_dir ), base_dir=os.path.basename( sip_dir ) )
		logger.info("Copying %s to HDFS..." % gztar)
		with open(gztar) as file_data:
			self.hdfs.create_file( gztar, file_data, overwrite=args.overwrite )
		logger.info("Done.")
		return gztar
	
	def setup_hdfs(self, args):
		self.hdfs = PyWebHdfsClient( host=args.webhdfs_host, port=args.webhdfs_port, user_name=args.webhdfs_user, timeout=100 )


	
	def callback(self, ch, method, properties, body ):
		"""Passed a checkpoint message, copies data to HDFS and creates a SIP. Having created the
		SIP, adds a message to the submission queue."""
		try:
			logger.info( "Message received: %s." % body )
			cpm = json.loads(body)
			path_id = "frequent/%s" % cpm['name']
			if self.verify_message( path_id ):
				cpp = CheckpointSipPreparer("frequent", cpm['name'], args)
				self.setup_hdfs(args)		
				sip_dir = self.create_sip( "frequent", cpp )
				logger.debug( "Created SIP: %s" % sip_dir )
				# Create our Bagit.
				bag = bagit.Bag( sip_dir )
				if bag.validate():
					logger.debug( "Copying %s to HDFS..." % path_id )
					gztar = self.copy_sip_to_hdfs( sip_dir )
					logger.debug( "SIP tarball at hdfs://%s" % gztar )
					# Clean up temp files now were done (not required?)
					#shutil.rmtree(sip_dir)
					#os.remove(gztar)
					# And post on
					logger.debug( "Sending message to '%s': %s" % ( args.out_queue, path_id ) )
					self.send_submit_message( path_id )
					# It's all gone well. ACK
					ch.basic_ack(delivery_tag = method.delivery_tag)
				else:
					raise Exception( "Invalid Bagit: %s" % sip_dir )
			else:
				raise Exception( "Could not verify message: %s" % body )
		except Exception as e:
			logger.error( "%s [%s]" % ( str( e ), body ) )
			logging.exception(e)

#
if __name__ == "__main__":
	#
	parser = argparse.ArgumentParser('Take checkpoints from the packaging queue')
	parser.add_argument('--dummy-run', dest='dummy_run', action="store_true", default=False)
	parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
		help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('--num', dest='qos_num', 
		type=int, default=100, help="Maximum number of messages to handle at once. [default: %(default)s]")
	parser.add_argument('--exchange', dest='exchange', default='post-crawl', help="Name of the exchange to use. [default: %(default)s]")
	parser.add_argument('--in-queue', dest='in_queue', default='PC-1-checkpoints-to-package', help="Name of queue to view messages from. [default: %(default)s]")
	parser.add_argument('--out-queue', dest='out_queue', default='PC-2-sips-to-submit', help="Name of queue to push messages to. [default: %(default)s]")
	parser.add_argument('--webhdfs-host', dest='webhdfs_host', default='hadoop', help="HTTP host for WebHDFS URIs. (default: %(default)s)")
	parser.add_argument('--webhdfs-port', dest='webhdfs_port', default='50070', help="HTTP port for WebHDFS URIs. (default: %(default)s)")
	parser.add_argument('--webhdfs-user', dest='webhdfs_user', default='root', help="Username for WebHDFS URIs. (default: %(default)s)")
	parser.add_argument('--sip-root', dest='sip_root', default='/heritrix/sips', help='Root folder for SIP building. (default: %(default)s)')
	
	args = parser.parse_args()
	
	args.overwrite = True
	
	#
	t = sipstodls(args.amqp_url, args.in_queue, "post-crawl", "checkpoints-to-package")
	t.begin()


