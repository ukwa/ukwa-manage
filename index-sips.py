#!/usr/bin/env python

"""
Pulls all messages from the 'index' queue, creating a list of WARCs for all
jobs therein. Submits a CDX-indexing job to Hadoop, outputting to
/wayback/cdx-index/.
"""

import os
import sys
import glob
import pika
import logging
import webhdfs
import tempfile
import subprocess
from sipverify import *
from datetime import datetime

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT )
logger = logging.getLogger( "index-sips" )
logger.setLevel( logging.INFO )
logging.getLogger( "pika" ).setLevel( logging.ERROR )

WARC_ROOT="/heritrix/output/warcs"
QUEUE_HOST="194.66.232.93"
ERROR_QUEUE="sip-error"
INDEX_QUEUE="index"
HADOOP_JAR="/heritrix/bin/warc-hadoop-indexer-1.1.2-SNAPSHOT-job.jar"
HADOOP_STREAM="/usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar"
HADOOP_SORT="/heritrix/bin/hadoop-utils-0.0.1-jar-with-dependencies.jar"
HADOOP_SORT_CLASS="com.alexholmes.hadooputils.sort.Sort"
CDX_CLASS="uk.bl.wa.hadoop.mapreduce.cdx.ArchiveCDXGenerator"
SOLR_CLASS="uk.bl.wa.hadoop.indexer.WARCIndexerRunner"
CDX_OUTPUT="/wayback/cdx-index"
CDX_COMBINED="/wayback/cdx-index/combined/"
CDX_SORTED="/wayback/cdx-index/sorted/"
SOLR_OUTPUT="/user/heritrix/solr"
SOLR_CONF="/heritrix/bin/ldwa.conf"

def create_cdx( warc_arks ):
	"""Given a list of ( WARC, ARK ) tuples, creates a CDX in HDFS."""
	input = tempfile.NamedTemporaryFile( delete=False )
	warc_ark_lookup = tempfile.NamedTemporaryFile( delete=False )
	for warc, ark in warc_arks:
		input.write( "%s\n" % warc )
		warc_ark_lookup.write( "%s\t%s\n" % ( os.path.basename( warc ), ark ) )
	input.close()
	warc_ark_lookup.close()

	w = webhdfs.API( prefix=WEBHDFS, user="heritrix" )
	logger.info( "Copying input file to HDFS: %s" % input.name )
	w.create( input.name, file=input.name )
	if not w.exists( input.name ):
		logger.error( "Problem copying input file: %s" % input.name )
		sys.exit( 1 )
	logger.info( "Copying lookup file to HDFS: %s" % warc_ark_lookup.name )
	w.create( warc_ark_lookup.name, warc_ark_lookup.name )
	if not w.exists( warc_ark_lookup.name ):
		logger.error( "Problem copying lookup file: %s" % warc_ark_lookup.name )
		sys.exit( 1 )

	output = "%s/index-queue-%s/%s/" % ( CDX_OUTPUT, datetime.now().strftime( "%Y%m%d%H%M%S" ), datetime.now().strftime( "%Y%m%d%H%M%S" ) )
	command = "hadoop jar %s %s -i %s -o %s -s /tmp/split.txt -r 260 -h -a %s -w" % ( HADOOP_JAR, CDX_CLASS, input.name, output, warc_ark_lookup.name )
	logger.info ( "Running command: %s" % command )
	try:
		hadoop = subprocess.check_output( command.split() )
	except subprocess.CalledProcessError as s:
		logger.error( "CalledProcessError: %s" % str( s ) )
		sys.exit( 1 )

	hdfssize = 0
	for part in w.list( output )[ "FileStatuses" ][ "FileStatus" ]:
		hdfssize += part[ "length" ]
	if hdfssize == 0:
		logger.warning( "Problem creating CDX!" )
		sys.exit( 1 )

	logger.info( "Removing input files from HDFS." )
	w.delete( input.name )
	w.delete( warc_ark_lookup.name )
	if w.exists( input.name ) or w.exists( warc_ark_lookup.name ):
		logger.warning( "Problem deleting input files: %s" % [ input.name, warc_ark_lookup.name ] )

def update_solr( warc_arks ):
	"""Given a list of WARCs, submits the content to Solr."""
	warcs = [ w for ( w, a ) in warc_arks ]
	input = tempfile.NamedTemporaryFile( delete=False )
	for warc in warcs:
		input.write( "%s\n" % warc )
	input.close()

	w = webhdfs.API( prefix=WEBHDFS, user="heritrix" )
	logger.info( "Copying input file to HDFS: %s" % input.name )
	w.create( input.name, file=input.name )
	if not w.exists( input.name ):
		logger.error( "Problem copying input file: %s" % input.name )
		sys.exit( 1 )
	output = "%s/%s/" % ( SOLR_OUTPUT, datetime.now().strftime( "%Y%m%d%H%M%S" ) )
	command = "hadoop jar %s %s -Dmapred.compress.map.output=true -i %s -o %s -c %s -a -w -x" % ( HADOOP_JAR, SOLR_CLASS, input.name, output, SOLR_CONF )
	logger.info( "Running command: %s" % command )
	try:
		solr = subprocess.check_output( command.split() )
	except subprocess.CalledProcessError as s:
		logger.error( "CalledProcessError: %s" % str( s ) )
		sys.exit( 1 )

	logger.info( "Removing input files from HDFS." )
	w.delete( input.name )
	os.unlink( input.name )
	if w.exists( input.name ) or os.path.exists( input.name ):
		 logger.warning( "Problem deleting input file: %s" % input.name )

def combine_cdx():
	"""Merges multiple CDX outputs to a single directory."""
	logger.info( "Merging CDX files..." )
	w = webhdfs.API( prefix=WEBHDFS, user="heritrix" )
	if w.exists( CDX_COMBINED ):
		logger.info( "Removing old, merged CDX." )
		w.delete( CDX_COMBINED, recursive=True )
	if w.exists( CDX_SORTED ):
		logger.info( "Removing old, sorted CDX." )
		w.delete( CDX_SORTED, recursive=True )
	command = "hadoop jar %s \
		-Dmapred.reduce.tasks=80 \
		-Dmapred.textoutputformat.separator=# \
		-Dmapred.job.name=combine \
        -Dmapred.compress.map.output=true \
		-mapper cat \
		-reducer cat \
		-input %s \
		-output %s" % ( HADOOP_STREAM, "/wayback/cdx-index/*/*/part-*", CDX_COMBINED )
	try:
		solr = subprocess.check_output( command.split() )
	except subprocess.CalledProcessError as s:
		logger.error( "CalledProcessError: %s" % str( s ) )
		sys.exit( 1 )

def sort_cdx( delete_input=False ):
	"""Sorts a single directory in HDFS."""
	logger.info( "Sorting CDX files..." )
	w = webhdfs.API( prefix=WEBHDFS, user="heritrix" )
	if w.exists( CDX_SORTED ):
		logger.info( "Removing old, sorted CDX." )
		w.delete( CDX_SORTED, recursive=True )
	command = "hadoop jar %s %s -Dmapred.compress.map.output=true --total-order 0.1 100000 100 %s %s" % ( HADOOP_SORT, HADOOP_SORT_CLASS, "%s/*" % CDX_COMBINED, CDX_SORTED )
	try:
		solr = subprocess.check_output( command.split() )
	except subprocess.CalledProcessError as s:
		logger.error( "CalledProcessError: %s" % str( s ) )
		sys.exit( 1 )
	if delete_input:
		logger.info( "Deleting sort-input." )
		w.delete( CDX_COMBINED, recursive=True )

if __name__ == "__main__":
	logger.info( "Reading messages from queue: %s" % INDEX_QUEUE )
##	message = get_message()
##	while message is not None:
	with open( "/home/rcoram/git/python-sip-verification/queue-index.txt", "rb" ) as i:
		messages = [ j.strip() for j in i.readlines() ]
	warc_arks = []
	for message in messages:
		warc_arks += get_warc_identifiers( message )
##		message = get_message()
	logger.info( "Found %s WARCs." % len( warc_arks ) )
	logger.info( "Creating CDX..." )
	create_cdx( warc_arks )
	combine_cdx()
	sort_cdx( delete_input=True )

#	logger.info( "Submitting to Solr..." )
#	update_solr( warc_arks )

