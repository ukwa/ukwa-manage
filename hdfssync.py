#!/usr/bin/env python

"""
Intended to keep local log files synced with HDFS.
"""

import os
import glob
import logging
import webhdfs
from hdfslogs import settings
from socket import gethostname

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.INFO )
logger = logging.getLogger( "hdfssync" )
logging.root.setLevel( logging.INFO )

if __name__ == "__main__":
	w = webhdfs.API( prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1", user=settings.hdfsuser )
	for path in settings.directories.split( "," ):
		if not path.startswith( "/" ):
			logger.warning( "Relative path found; ignoring: %s" % path )
			continue
		if not os.path.exists( path ):
			logger.warning( "Trying to sync. non-existant path: %s" % path )
			continue
		if not path.endswith( "/" ):
			path = "%s/" % path
		hdfs_prefix = "%s/%s" % ( settings.hdfsroot, gethostname() )
		for log in glob.glob( "%s*" % path ):
			local_size = os.stat( log ).st_size
			if local_size == 0:
				continue
			hdfs_log = "%s%s" % ( hdfs_prefix, log )
			if w.exists( hdfs_log ):
				hdfs_size = int( w.list( hdfs_log )[ "FileStatuses" ][ "FileStatus" ][ 0 ][ "length" ] )
				if hdfs_size != local_size:
					logger.info( "Removing %s" % hdfs_log )
					w.delete( hdfs_log )
					if( w.exists( hdfs_log ) ):
						logger.error( "Problem deleting %s" % hdfs_log )
						continue
				else:
					continue
			logger.info( "Creating %s" % hdfs_log )
			r = w.create( hdfs_log, file=log )
			if not r.ok or not w.exists( hdfs_log ):
				logger.error( "Problem creating %s" % hdfs_log )
