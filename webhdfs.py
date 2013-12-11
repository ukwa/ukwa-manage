#!/usr/bin/env python

import json
import logging
import requests

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "webhdfs" )

class API():
	def __init__( self, prefix="http://localhost:14000/webhdfs/v1", verbose=False, user="hadoop" ):
		self.verbose = verbose
		self.prefix = prefix
		self.user = user

	def _get( self, path="/", op="GETFILESTATUS", stream=False ):
		url = "%s%s?user.name=%s&op=%s" % ( self.prefix, path, self.user, op )
		r = requests.get( url, stream=stream )
		return r

	def list( self, path="/" ):
		return self._get( path=path, op="LISTSTATUS" )

	def open( self, path="/" ):
		return self._get( path=path, op="OPEN" )

	def openstream( self, path="/" ):
		return self._get( path=path, op="OPEN", stream=True )

	def isdir( self, path="/" ):
		r = self._get( path=path )
		j = json.loads( r.text )
		return j[ "FileStatus" ][ "type" ] == "DIRECTORY"

	def download( self, output, path="/" ):
		r = openstream( path=path )
		with open( output, "wb" ) as o:
			for chunk in r.iter_content( chunk_size=4096 ):
				if chunk:
					o.write( chunk )
					o.flush()

