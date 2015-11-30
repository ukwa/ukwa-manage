#!/usr/local/bin/python2.7

import logging
import heritrix
from twitter import *

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

services = { ( "opera.bl.uk", "8443" ), ( "opera.bl.uk", "8444" ), ( "opera.bl.uk", "8445" ), ( "opera.bl.uk", "8446" ) }

logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "heritrix-monitor" )

def send_message( message ):
	t = Twitter( auth=OAuth( "16067043-PDJQCHs8JvjFbE0R98KBS8mOmgjboiDGecX8DzIwY", "PRb6uzccTe08mjOP565mCR3pZDxRxDyCSqCWfOhjg", "Q2EiadOZrHp7cCk9ITCkGA", "2YtbjCuKwf8rnrTWV8Y0tomi30tEevcrTixi8jn8QMM" ) )
	t.direct_messages.new( user="PsypherPunk", text=message )
	logger.error( message )

failures = []
for host, port in services:
	api = heritrix.API( host="https://%s:%s/engine" % ( host, port ), user="admin", passwd="bl_uk", verbose=False, verify=False )
	try:
		api.rescan()
	except Exception:
		failures.append( "%s@%s" % ( host, port ) )
if len( failures ) > 0:
	send_message( "Could not connect to Heritrix: %s." % ", ".join( failures ) )

