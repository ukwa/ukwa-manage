#!/usr/bin/env python

"""
Checks whether the most recent CDX in HDFS is bigger than that on the local
disk. If so, replaces the local version.
"""

import os
import sys
import logging
import webhdfs
from hdfssync import settings

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.INFO )
logger = logging.getLogger( "hdfscdxsync" )
logging.root.setLevel( logging.INFO )

if __name__ == "__main__":
	w = webhdfs.API( prefix="http://%s:14000/webhdfs/v1" % settings.hdfshost, user=settings.hdfscdxuser )
	if not w.exists( settings.hdfscdxroot ):
		logger.error( "No HDFS CDX found: %s" % hdfscdx )
		sys.exit( 1 )
	if not os.path.exists( settings.localcdx ):
		logger.error( "No local CDX found: %s" % localcdx )
		sys.exit( 1 )

	hdfssize = 0
	for part in w.list( settings.hdfscdxroot )[ "FileStatuses" ][ "FileStatus" ]:
		hdfssize += part[ "length" ]

	localcdxsize = os.stat( settings.localcdx ).st_size
	if hdfssize > localcdxsize:
		logger.info( "Replacing local CDX (%s) with HDFS CDX (%s)." % ( settings.localcdx, settings.hdfscdxroot ) )
		with open( settings.localcdx, "wb" ) as o:
			w.getmerge( hdfscdx, o )

