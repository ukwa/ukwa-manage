#!/usr/bin/env python
"""Writes records to a configurable number of WARC files."""

import os
import re
import uuid
import Queue
import shutil
import socket
import logging
from datetime import datetime
from hanzo.warctools import WarcRecord
from hanzo.warctools.warc import warc_datetime_str

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "warcwriterpool" )

__version__ = "0.1.2"

class WarcWriterPool:
	def __init__( self, pool_size=1, gzip=True, prefix="BL", output_dir=".", max_size=1073741824, description=None, write_warcinfo=True ):
		self.gzip = gzip
		self.prefix = prefix
		self.output_dir = output_dir
		self.max_size = max_size
		self.pool = Queue.Queue()
		self.warcs = {}
		self.total = 0
		self.hostname = socket.gethostname()
		self.ip = socket.gethostbyname( socket.gethostname() )
		self.description = description
		self.write_warcinfo = write_warcinfo
		self.software = "%s/%s" % ( __name__, __version__ )

		if gzip:
			self.suffix = ".gz"
		else:
			self.suffix = ""
		logger.debug( "Pooling %i WARCs." % pool_size )
		self.add_warcs( pool_size )

	def write_warcinfo_record( self, warc ):
		"""Writes the initial warcinfo record."""
		headers = [
			( WarcRecord.TYPE, WarcRecord.WARCINFO ),
			( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
			( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
		]
		data = "software=%s\nhostname=%s\nip=%s" % ( self.software, self.hostname, self.ip )
		if self.description is not None:
			data += "\ndescription=%s" % self.description
		record = WarcRecord( headers=headers, content=( "application/warc-fields", data ) )
		record.write_to( warc, gzip=self.gzip )
		warc.flush()

	def add_warcs( self, number ):
		"""Adds a new WARC and rebuilds the Queue."""
		for n in range( number ):
			name = "%s/%s-%s-%s.warc%s.open" % ( self.output_dir, self.prefix, datetime.now().strftime( "%Y%m%d%H%M%S%f" ), self.total, self.suffix )
			self.total += 1
			fh = open( name, "wb" )
			if self.write_warcinfo:
				self.write_warcinfo_record( fh )
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
			shutil.move( path, re.sub( "\.open$", "", path ) )
			del self.warcs[ path ]
			self.add_warcs( 1 )
			logger.debug( "Checked size: %s" % str( self.warcs.keys() ) )
			return True
		logger.debug( "Checked size: %s" % str( self.warcs.keys() ) )
		return False

	def write_record( self, headers, mime, data ):
		"""Writes a WARC record.

		Arguments:
		headers -- Array of WARC headers.
		mime -- MIME type of the data.
		data -- the data block.

		"""
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
			if name.endswith( ".open" ):
				shutil.move( name, re.sub( "\.open$", "", name ) )

