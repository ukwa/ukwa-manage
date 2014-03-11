#!/usr/bin/env python

import os
import re
import sys
import uuid
import base64
import argparse
import requests
import subprocess
from lxml import etree
from datetime import datetime
from hanzo.warctools import WarcRecord
from dateutil import parser as dateparser
from warcwriterpool import WarcWriterPool, warc_datetime_str

__version__ = "0.1.1"

def httpheaders( original ):
	status_line = "HTTP/%s %s %s" % ( 
		".".join( str( original.version ) ),
		original.status,
		original.reason
	)
	headers = [ status_line ]
	try:
		headers.extend( "%s: %s" % header for header in original.msg._headers )
	except AttributeError:
		headers.extend( h.strip() for h in original.msg.headers )
	return "%s\r\n\r\n" % "\r\n".join( headers )

def writemetadata( video_url, video_uuid, timestamp, xpath, page ):
	headers = [
		( WarcRecord.TYPE, WarcRecord.METADATA ),
		( WarcRecord.URL, page ),
		( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
		( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
		( WarcRecord.CONCURRENT_TO, video_uuid ),
		( WarcRecord.CONTENT_TYPE, "text/plain" ),
	]
	block = "embedded-video: %s\nembedding-timestamp: %s\nembedded-video-xpath: %s" % ( video_url, timestamp, xpath )
	warcwriter.write_record( headers, "text/plain", block )

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument( "-p", dest="page", help="Embedding page." )
	parser.add_argument( "-t", dest="timestamp", help="Embedding page timestamp." )
	parser.add_argument( "-x", dest="xpath", help="XPath to element." )
	parser.add_argument( "-u", dest="url", help="Video URL." )
	parser.add_argument( "-f", dest="filename", help="Filename on disk." )
	
	args = parser.parse_args()
	warcwriter = WarcWriterPool( gzip=True, write_warcinfo=False )
	video_uuid = "<urn:uuid:%s>" % uuid.uuid1()
	if args.url.startswith( "http" ):
		r = requests.get( args.url )
		if not r.ok:
			print "ERROR: %s" % r.content
			sys.exit( 1 )
		writemetadata( args.url, video_uuid, args.timestamp, args.xpath, args.page )
		headers = [
			( WarcRecord.TYPE, WarcRecord.RESPONSE ),
			( WarcRecord.URL, r.url ),
			( WarcRecord.DATE, warc_datetime_str( dateparser.parse( r.headers[ "date" ] ) ) ),
			( WarcRecord.ID, video_uuid ),
			( WarcRecord.CONTENT_TYPE, "application/http; msgtype=response" ),
		]
		block = "".join( [ httpheaders( r.raw._original_response ), r.content ] )
		warcwriter.write_record( headers, "application/http; msgtype=response", block )
	elif args.url.startswith( "rtmp" ):
		data = None
		with open( args.filename, "rb" ) as d:
			data = d.read()
		if len( data ) == 0 or data is None:
			print "ERROR: %s" % args.filename
			sys.exit( 1 )
		writemetadata( args.url, video_uuid, args.timestamp, args.xpath, args.page )
		headers = [
			( WarcRecord.TYPE, WarcRecord.RESOURCE ),
			( WarcRecord.URL, args.url ),
			( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
			( WarcRecord.ID, video_uuid ),
			( WarcRecord.CONTENT_TYPE, "video/x-flv" ),
		]
		warcwriter.write_record( headers, "video/x-flv", data )
	warcwriter.cleanup()
