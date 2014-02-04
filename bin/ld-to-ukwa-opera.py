#!/usr/bin/env python

"""Indexes Instances and copies the index to HDFS.

Intended to run on Opera, as this server has access to the data, this checks
ACT for Instances marked for migration and runs 'ld2ukwa' to generate a CDX.

This CDX is copied to HDFS for use by Mosaic.

"""

import os
import re
import act
import logging
import webhdfs
import subprocess
from urlparse import urlparse

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
logger = logging.getLogger( "ld-to-ukwa" )
logging.root.setLevel( logging.WARNING )

w = webhdfs.API( prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1" )
a = act.ACT()

j = a.request_instances_to_migrate()
for node in j[ "list" ]:
	body = node[ "body" ][ "value" ]
	wct_id = re.findall( "^.+WCT ID: ([0-9]+)\\b.*", body )[ 0 ]
	timestamp = node[ "field_timestamp" ]
	logger.info( "Migrating %s" % timestamp )
	id = node[ "field_target" ][ "id" ]
	data = a.request_node( str( id ) )
	domains = []
	for url in data[ "field_url" ]:
		domains.append( re.sub( "^www\.", "", urlparse( url[ "url" ] ).netloc ) )
	jobname = re.findall( "Job ID: ([^<]+)", body )[ 0 ]
	cdx = "/dev/shm/%s.cdx" % timestamp
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

