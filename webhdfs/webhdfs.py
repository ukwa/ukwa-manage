#!/usr/bin/env python

import os
import sys
import json
import zlib
import logging
import requests
from fnmatch import fnmatch
from StringIO import StringIO

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
logger = logging.getLogger( "webhdfs" )

def readlines(generator):
    previous = ""
    for chunk in dechunk(generator):
        lines = StringIO(previous + chunk).readlines()
        for line in lines[:-1]:
            yield line
        previous = lines[-1]
    yield previous

def dechunk(generator):
    d = zlib.decompressobj(zlib.MAX_WBITS|32)
    while True:
        block = generator.read(4096)
        if not block:
            break
        dec = d.decompress(block)
        if len(dec) == 0:
            break
        yield dec


class API():
    def __init__( self, prefix="http://localhost:14000/webhdfs/v1", verbose=False, user="hadoop" ):
        self.verbose = verbose
        self.prefix = prefix
        self.user = user

    def _get( self, path="/", op="GETFILESTATUS", stream=False ):
        url = "%s%s?user.name=%s&op=%s" % ( self.prefix, path, self.user, op )
        r = requests.get( url, stream=stream )
        return r

    def _post( self, path, file=None, data=None, op="CREATE" ):
        url = "%s%s?user.name=%s&op=%s&data=true" % ( self.prefix, path, self.user, op )
        headers = { "content-type": "application/octet-stream" }
        if file is not None:
            with open( file, "rb" ) as f:
                r = requests.put( url, headers=headers, data=f )
        else:
            r = requests.put( url, headers=headers, data=data )
        return r

    def _delete( self, path, recursive=False ):
        url = "%s%s?user.name=%s&op=DELETE&recursive=%s" % ( self.prefix, path, self.user, str( recursive ).lower() )
        r = requests.delete( url )
        return r

    def list( self, path ):
        r = self._get( path=path, op="LISTSTATUS" )
        return json.loads( r.text )

    def find(self, path, name="*"):
        if self.isdir(path):
            for entry in self.list(path)["FileStatuses"]["FileStatus"]:
                for sub in self.find(os.path.join(path, entry["pathSuffix"]), name=name):
                    yield sub
        else:
            if fnmatch(path, name):
                yield path

    def file(self, path):
        r = self._get(path=path, op="GETFILESTATUS")
        return json.loads(r.text)

    def open( self, path ):
        r = self._get( path=path, op="OPEN" )
        return r.content

    def openstream( self, path ):
        return self._get( path=path, op="OPEN", stream=True )

    def exists( self, path ):
        r = self._get( path=path )
        j = json.loads( r.text )
        return j.has_key( "FileStatus" )

    def isdir( self, path ):
        r = self._get( path=path )
        j = json.loads( r.text )
        return ( self.exists( path ) and j[ "FileStatus" ][ "type" ] == "DIRECTORY" )

    def download( self, path, output=sys.stdout ):
        """Copies a single file from HDFS to a local file."""
        r = self.openstream( path )
        for chunk in r.iter_content( chunk_size=4096 ):
            if chunk:
                o.write( chunk )
                o.flush()

    def getmerge( self, path, output=sys.stdout ):
        """Merges one or more HDFS files into a single, local file."""
        if self.isdir(path):
            directory = path
        else:
            directory = os.path.dirname(path)
        j = self.list( path )
        for file in j[ "FileStatuses" ][ "FileStatus" ]:
            if file["type"] == "FILE":
                r = self.openstream(os.path.join(directory, f["pathSuffix"]))
                for chunk in r.iter_content( chunk_size=4096 ):
                    if chunk:
                        output.write( chunk )
                        output.flush()

    def create( self, path, file=None, data=None ):
        if ( file is None and data is None ) or ( file is not None and data is not None ):
            logger.warning( "Need either input file or data." )
        else:
            if file is not None:
                r = self._post( path, file=file )
            else:
                r = self._post( path, data=data )
            return r

    def delete( self, path, recursive=False ):
        if not self.exists( path ):
            logger.error( "Does not exist: %s" % path )
        else:
            r = self._delete( path, recursive=recursive )
            return json.loads( r.text )

    def checksum( self, path ):
        r = self._get( path=path, op="GETFILECHECKSUM" )
        return json.loads( r.text )

    def readlines(self, path):
        if self.isdir(path):
            directory = path
        else:
            directory = os.path.dirname(path)
        j = self.list(path)
        for f in j["FileStatuses"]["FileStatus"]:
            if f["type"] == "FILE":
                r = self.openstream(os.path.join(directory, f["pathSuffix"]))
                for line in readlines(r.raw):
                    yield line.strip()
                r.close()

