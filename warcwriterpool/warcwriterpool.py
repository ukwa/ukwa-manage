#!/usr/bin/env python
"""Writes records to a configurable number of WARC files."""

import os
import logging
import itertools
from datetime import datetime
from hanzo.warctools import WarcRecord
from hanzo.warctools.warc import warc_datetime_str

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "warcwriterpool" )

class WarcWriterPool:
	def __init__( self, pool_size=1, gzip=True, prefix="BL", output_dir=".", max_size=1073741824 ):
		self.gzip = gzip
		self.prefix = prefix
		self.output_dir = output_dir
		self.max_size = max_size
		self.warcs = {}

		if gzip:
			self.suffix = ".gz"
		else:
			self.suffix = ""
		logger.debug( "Pooling %i WARCs." % pool_size )
		self.add_warcs( pool_size )

	def add_warcs( self, number ):
		for n in range( number ):
			name = "%s/%s-%s-%s.warc%s" % ( self.output_dir, self.prefix, datetime.now().strftime("%Y%m%d%H%M%S"), len( self.warcs.keys() ), self.suffix )
			fh = open( name, "wb" )
			self.warcs[ name ]  = fh
			logger.debug( "Added %s" % name )
		self.pool = itertools.cycle( self.warcs.iteritems() )

	def check_warc_size( self, path ):
		stat = os.stat( path )
		if stat.st_size >= self.max_size:
			logger.debug( "Size limit exceeded for %s" % path )
			del self.warcs[ path ]
			self.add_warcs( 1 )

	def write_record( self, headers, mime, data ):
		record = WarcRecord( headers=headers, content=( mime, data ) ) 
		name, fh = self.pool.next()
		record.write_to( fh, gzip=self.gzip )
		fh.flush()
		self.check_warc_size( name )

	def cleanup( self ):
		for w in self.warcs:
			if not w.closed:
				w.close()

