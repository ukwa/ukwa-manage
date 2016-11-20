#!/usr/bin/env python

"""Indexes Instances and copies the index to HDFS.

Intended to run on Opera, as this server has access to the data, this checks
ACT for Instances marked for migration and runs 'ld2ukwa' to generate a CDX.

This CDX is copied to HDFS for use by Mosaic.

"""

import os
import re
import act
import sys
import json
import logging
import webhdfs
import subprocess
from time import sleep
from urlparse import urlparse
from threading import Thread

logger = logging.getLogger( "ld-to-ukwa-opera" )
s = logging.StreamHandler( sys.stdout )
s.setLevel( logging.DEBUG )
s.setFormatter( logging.Formatter( "[%(asctime)s] %(levelname)s: %(message)s" ) )
logger.addHandler( s )

w = webhdfs.API( prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1" )
a = act.ACT()

def threaded( node, logger ):
	body = node[ "body" ][ "value" ]
	id = node[ "field_target" ][ "id" ]
	data = a.request_node( str( id ) )
	wct_id = str( data[ "field_wct_id" ] )
	timestamp = node[ "field_timestamp" ]
	logger.info( "Migrating %s" % timestamp )
	domains = []
	for url in data[ "field_url" ]:
		domains.append( urlparse( url[ "url" ] ).netloc )
	jobname = re.findall( "Job ID: ([^<]+)", body )[ 0 ]
	logger.debug( "{\n\t\"id\": %s,\n\t\"wct_id\": %s,\n\t\"timestamp\": %s\n\t\"domains\": %s\n\t\"jobname\": %s\n}" % ( id, wct_id, timestamp, domains, jobname ) )
	cdx = "/dev/shm/%s-%s.cdx" % ( wct_id, timestamp )
	logger.debug( " ".join( [ "ld2ukwa", "-d", "|".join( domains ), "-j", jobname, "-t", timestamp, "-o", cdx ] ) )
	output = subprocess.check_output( [ "ld2ukwa", "-d", "|".join( domains ), "-j", jobname, "-t", timestamp, "-o", cdx ] )
	if os.path.exists( cdx ) and os.stat( cdx ).st_size > 0:
		if wct_id is not None:
			hdfs_file = "/data/wayback/cdx-index/%s/%s.cdx" % ( wct_id, timestamp )
		else:
			logger.warning( "Couldn't find WCT ID for %s" % timestamp )
			hdfs_file = "/data/wayback/cdx-index/%s/%s.cdx" % ( domains[ 0 ], timestamp )
		w.create( hdfs_file, file=cdx )
		if w.exists( hdfs_file ):
			os.remove( cdx )
			update = {}
			update[ "field_published" ] = 1
			a.send_data( node[ "nid" ], json.dumps( update ) )
		else:
			logger.error( "Error creating %s" % hdfs_file )
	else:
		logger.warning( "0-length CDX created for %s" % timestamp )

threads = []
a = act.ACT()

j = a.request_instances_to_migrate()
if j is not None:
	for node in j[ "list" ]:
		thread = Thread( target=threaded, args=( node, logger ) )
		threads.append( thread )
		thread.start()
		sleep( 10 )
		while len( threads ) >= 4:
			sleep( 60 )
			for t in threads:
				if not t.isAlive():
					threads.remove( t )

