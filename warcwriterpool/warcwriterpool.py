#!/usr/bin/env python
"""Writes records to a configurable number of WARC files."""

import itertools
from datetime import datetime
from hanzo.warctools import WarcRecord
from hanzo.warctools.warc import warc_datetime_str

class WarcWriterPool:
	def __init__( self, num_warcs=1, gzip=True, prefix="BL" ):
		self.gzip = gzip
		self.current = 0
		self.warc_files = []

		self.warcs = []
		if gzip:
			suffix = ".gz"
		else:
			suffix = ""
		for n in range( num_warcs ):
			self.warcs.append( open( "%s-%s-%s.warc%s" % ( prefix, datetime.now().strftime("%Y%m%d%H%M%S"), n, suffix ), "wb" ) )
		self.pool = itertools.cycle( self.warcs )

	def write_record( self, headers, mime, data ):
		record = WarcRecord( headers=headers, content=( mime, data ) ) 
		warc = self.pool.next()
		record.write_to( warc, gzip=self.gzip )
		warc.flush()

	def cleanup( self ):
		for w in self.warcs:
			if not w.closed:
				w.close()

