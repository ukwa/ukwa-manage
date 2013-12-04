#!/usr/local/bin/python2.7

import logging
import heritrix
from twitter import *

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"

ports = { "8443", "8444" }

logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "heritrix-monitor" )

def send_message( port ):
	message = "Could not connect to Heritrix on port " + port
	t = Twitter( auth=OAuth( "16067043-PDJQCHs8JvjFbE0R98KBS8mOmgjboiDGecX8DzIwY", "PRb6uzccTe08mjOP565mCR3pZDxRxDyCSqCWfOhjg", "QQLmIMllFWxZKi0rdmPuSg", "Q6yGMQWBAlxgai3B45G9C8Na9i5k3bQ8hZ9j7JorIs" ) )
	t.direct_messages.new( user="PsypherPunk", text=message )
	logger.error( message )

for port in ports:
	api = heritrix.API( host="https://opera.bl.uk:" + port + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False )
	try:
		api.rescan()
	except Exception:
		send_message( port )
	
