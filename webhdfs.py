#!/usr/bin/env python

import sys
import json
import logging
import requests

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
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

	def _post( self, path, input=None, op="CREATE" ):
		url = "%s%s?user.name=%s&op=%s&data=true" % ( self.prefix, path, self.user, op )
		headers = { "content-type": "application/octet-stream" }
		with open( input, "rb" ) as i:
			r = requests.put( url, headers=headers, data=i )
		return r

	def list( self, path ):
		return self._get( path=path, op="LISTSTATUS" )

	def open( self, path ):
		return self._get( path=path, op="OPEN" )

	def openstream( self, path ):
		return self._get( path=path, op="OPEN", stream=True )

	def exists( self, path ):
		r = self._get( path=path )
		j = json.loads( r.text )
		return j[ "FileStatus" ] is not None

	def isdir( self, path ):
		r = self._get( path=path )
		j = json.loads( r.text )
		return ( self.exists( path ) and j[ "FileStatus" ][ "type" ] == "DIRECTORY" )

	def download( self, path, output=sys.stdout ):
		r = self.openstream( path )
		with open( output, "wb" ) as o:
			for chunk in r.iter_content( chunk_size=4096 ):
				if chunk:
					o.write( chunk )
					o.flush()

	def create( self, path, file=None ):
		if file is None:
			logger.warning( "Need file parameter." )
		else:
			r = self._post( path, input=file )
			return r

