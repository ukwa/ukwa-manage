#!/usr/bin/env python

import os
import re
import act
import json
import ukwa
import logging
import webhdfs
import subprocess

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
logger = logging.getLogger( "ld-to-ukwa" )
logging.root.setLevel( logging.WARNING )

def url_in_warc( hdfs_cdx, url ):
	h = subprocess.Popen( [ "hadoop", "fs", "-cat", hdfs_cdx ], stdout=subprocess.PIPE )
	#Broken Pipe?
	s = subprocess.Popen( [ "sort", "-T", "/dev/shm" ], stdin=h.stdout, stdout=subprocess.PIPE, stderr=open( os.devnull, "wb" ) )
	g = subprocess.Popen( [ "grep", "-m1", url ], stdin=s.stdout, stdout=subprocess.PIPE )
	output = subprocess.check_output( [ "awk", "{ print $10 }" ], stdin=g.stdout )
	if len( output ) > 0:
		output = re.sub( "^.+/(BL.+\.warc.gz).*$", "\\1", output.strip() )
	return output

a = act.ACT()
w = webhdfs.API( prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1" )

j = a.request_migrated_instances()
for node in j[ "list" ]:
	body = node[ "body" ][ "value" ]
	id = node[ "field_target" ][ "id" ]
	timestamp = node[ "field_timestamp" ]
	wct_id = re.findall( "^.+WCT ID: ([0-9]+)\\b.*", body )[ 0 ]
	if wct_id is not None:
		if w.exists( "/data/wayback/cdx-index/%s/%s.cdx" % ( wct_id, timestamp ) ):
			with ukwa.UKWA() as u:
				result = u.get_instance_by_resource_timestamp( wct_id, timestamp )
				if len( result ) == 0:
					logger.info( "Found CDX for %s/%s; creating Instance." % ( wct_id, timestamp ) )
					last_instance = u.get_last_instance( wct_id )
					u.create_instance( wct_id )
					new = u.get_last_instance( wct_id )
					if new == last:
						logger.error( "Problem creating Instance for %s" % wct_id )
					else:
						logger.info( "Setting date for %s to %s." % ( new, timestamp ) )
						u.update_date_by_instance( timestamp, new )
						primary_url = u.get_primary_url( new )
						warc = url_in_warc( "/data/wayback/cdx-index/%s/%s.cdx" % ( wct_id, timestamp ), primary_url )
						if len( warc ) > 0:
							logger.info( "Setting storage for %s to %s." % ( new, warc ) )
							u.update_storage_by_instance( warc, new )
						else:
							logger.error( "Could not determine WARC for %, %s." % ( new, primary_url ) )
		else:
			logger.warning( "No CDX found for published Instance %s/%s." % ( wct_id, timestamp ) )
	else:
		logger.error( "Cannot find WCT ID for timestamp %s" % timestamp )

