#!/usr/bin/env python

"""Daemon which watches a configured queue for messages and for each, calls a
webservice, storing the result in a WARC file."""

import sys
import logging
from harchiverd import HarchiverDaemon, settings

logger = logging.getLogger( "harchiverd" )
handler = logging.FileHandler( settings.LOG_FILE )
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s: %(message)s" )
handler.setFormatter( formatter )
logger.addHandler( handler )
logger.setLevel( logging.DEBUG )

#Try to set logging output for all modules.
logging.root.setLevel( logging.DEBUG )
logging.getLogger( "" ).addHandler( handler )

if __name__ == "__main__":
	"""Sets up the daemon."""
	if len( sys.argv ) == 2:
		daemon = HarchiverDaemon( settings.PID_FILE )
	elif len( sys.argv ) == 3:
		#Possibly pass a PID file to enable multiple instances.
		daemon = HarchiverDaemon( sys.argv[ 2 ] )
	logger.debug( "Arguments: %s" % sys.argv )
	if len( sys.argv ) >= 2:
		if "start" == sys.argv[ 1 ]:
			logger.info( "Starting harchiverd." )
			daemon.start()
		elif "stop" == sys.argv[ 1 ]:
			logger.info( "Stopping harchiverd." )
			daemon.stop()
		elif "restart" == sys.argv[ 1 ]:
			logger.info( "Restarting harchiverd." )
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
