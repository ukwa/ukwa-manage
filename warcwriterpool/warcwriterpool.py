#!/usr/bin/env python
"""Writes records to a configurable number of WARC files."""

import os
import Queue
import logging
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
		self.pool = Queue.Queue()
		self.warcs = {}
		self.total = 0

		if gzip:
			self.suffix = ".gz"
		else:
			self.suffix = ""
		logger.debug( "Pooling %i WARCs." % pool_size )
		self.add_warcs( pool_size )

	def add_warcs( self, number ):
		"""Initialises a numer of filehandles and builds a Queue of their paths."""
		for n in range( number ):
			name = "%s/%s-%s-%s.warc%s" % ( self.output_dir, self.prefix, datetime.now().strftime( "%Y%m%d%H%M%S%f" ), self.total, self.suffix )
			self.total += 1
			fh = open( name, "wb" )
			self.warcs[ name ]  = fh
			logger.debug( "Added %s" % name )
		with self.pool.mutex:
			self.pool.queue.clear()
		x = [ self.pool.put( warc ) for warc in self.warcs.keys() ]

	def warc_reached_max_size( self, path ):
		"""Checks whether a given WARC has reached the maximum filesize."""
		stat = os.stat( path )
		if stat.st_size >= self.max_size:
			logger.info( "Size limit exceeded for %s" % path )
			self.warcs[ path ].close()
			del self.warcs[ path ]
			self.add_warcs( 1 )
			logger.debug( "Checked size: %s" % str( self.warcs.keys() ) )
			return True
		logger.debug( "Checked size: %s" % str( self.warcs.keys() ) )
		return False

	def write_record( self, headers, mime, data ):
		"""Given an array of WARC headers and content, writes the data to the
		first available WARC writer, blocking until one is available."""
		record = WarcRecord( headers=headers, content=( mime, data ) ) 
		logger.debug( "Getting WARC: %s" % str( self.warcs.keys() ) )
		name = self.pool.get()
		logger.debug( "Writing to: %s" % name )
		fh = self.warcs[ name ]
		record.write_to( fh, gzip=self.gzip )
		fh.flush()
		if not self.warc_reached_max_size( name ):
			logger.debug( "%s undersized; adding back to the pool." % name )
			self.pool.put( name )

	def cleanup( self ):
		"""Closes any open file handles."""
		for name, fh in self.warcs.iteritems():
			if not fh.closed:
				fh.close()

