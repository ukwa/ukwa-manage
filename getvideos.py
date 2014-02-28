#!/usr/bin/env python

import os
import sys
import uuid
import base64
import requests
from lxml import etree
from dateutil import parser
from datetime import datetime
from hanzo.warctools import WarcRecord
from warcwriterpool import WarcWriterPool
from hanzo.warctools.warc import warc_datetime_str

__version__ = "0.0.2"

WAYBACK="http://opera.bl.uk:8080/wayback"
BBC_MEDIA="http://www.bbc.co.uk/mediaselector/4/mtis/stream"

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

def try_archived_version( url, timestamp ):
	"""Tries to get archived playlist before getting live version."""
	if WAYBACK not in url:
		r = requests.get( "%s/%s/%s" % ( WAYBACK, timestamp, url ) )
		if r.ok:
			return r
	return requests.get( url )

def getvideo( page, timestamp=None ):
	if timestamp is None:
		r = requests.get( page )
		timestamp = datetime.now().strftime( "%Y%m%d%H%M%S" )
	else:
		r = requests.get( "%s/%s/%s" % ( WAYBACK, timestamp, page ) )
	htmlparser = etree.HTMLParser()
	root = etree.fromstring( r.content, htmlparser )
	tree = etree.ElementTree( root )
	for index, object in enumerate( root.xpath( "//object[param[@name='playlist']]" ) ):
		for param in object.xpath( "//param[@name='playlist']" ):
			video_uuid = "<urn:uuid:%s>" % uuid.uuid1()
			playlist = try_archived_version( param.attrib[ "value" ], timestamp )
			if not playlist.ok:
				print "ERROR: Couldn't find playlist; %s" % param.attrib[ "value" ]
				return
			if WAYBACK not in playlist.url:
				print "TODO: Should probably store this in a 'response'?"

			xml = etree.fromstring( playlist.content )
			item = xml.find( "{http://bbc.co.uk/2008/emp/playlist}item" )
			group = item.attrib[ "group" ]
			media = try_archived_version( "%s/%s" %( BBC_MEDIA, group ), timestamp )
			if not media.ok:
				print "ERROR: Couldn't find media; %s" % "%s/%s" %( BBC_MEDIA, group )
				return
			if WAYBACK not in media.url:
				headers = [
					( WarcRecord.TYPE, WarcRecord.RESPONSE ),
					( WarcRecord.URL, media.url ),
					( WarcRecord.DATE, warc_datetime_str( parser.parse( media.headers[ "date" ] ) ) ),
					( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
					( WarcRecord.CONCURRENT_TO, video_uuid ),
					( WarcRecord.CONTENT_TYPE, "application/http; msgtype=response" ),
				]
				block = "".join( [ httpheaders( media.raw._original_response ), media.content ] )
				warcwriter.write_record( headers, "application/http; msgtype=response", block )

			xml = etree.fromstring( media.content )
			best = None
			for media in xml.findall( "{http://bbc.co.uk/2008/mp/mediaselection}media" ):
				if best is None:
					best = media
				if int( media.attrib[ "bitrate" ] ) > int( best.attrib[ "bitrate" ] ) and media.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib.has_key( "href" ):
					best = media
			url = best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "href" ]

			video = requests.get( url )
			if not video.ok:
				print "ERROR: Couldn't find video; %s" % url
				return
			headers = [
				( WarcRecord.TYPE, WarcRecord.METADATA ),
				( WarcRecord.URL, url ),
				( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
				( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
				( WarcRecord.CONCURRENT_TO, video_uuid ),
				( WarcRecord.CONTENT_TYPE, "application/warc-fields" ),
			]
			b64string = base64.b64encode( etree.tostring( object ).strip() )
			block = "embedding-page: %s\nembedding-timestamp: %s\nelement-xpath: (//object[param[@name='playlist']])[%i]\nelement-base64-string: %s" % ( page, timestamp, index+1, b64string )
			warcwriter.write_record( headers, "application/warc-fields", block )

			headers = [
				( WarcRecord.TYPE, WarcRecord.RESPONSE ),
				( WarcRecord.URL, video.url ),
				( WarcRecord.DATE, warc_datetime_str( parser.parse( video.headers[ "date" ] ) ) ),
				( WarcRecord.ID, video_uuid ),
				( WarcRecord.CONTENT_TYPE, "application/http; msgtype=response" ),
			]
			block = "".join( [ httpheaders( video.raw._original_response ), video.content ] )
			warcwriter.write_record( headers, "application/http; msgtype=response", block )

if __name__ == "__main__":
	warcwriter = WarcWriterPool( gzip=True, description="%s/%s; Trac #2301; videos for Margaret Thatcher Collection." % ( "python-getvideos", __version__ ) )
	for arg in sys.argv[ 1: ]:
		if arg[ 0 ].isdigit():
			timestamp, url = arg.split( "/", 1 )
			getvideo( url, timestamp=timestamp )
		else:
			getvideo( url )
	warcwriter.cleanup()
