#!/usr/bin/env python

import os
import re
import sys
import uuid
import base64
import librtmp
import requests
import subprocess
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

def getbestvideo( xml ):
	"""Finds the video with the highest bitrate; ideally HTTP."""
	best = None
	for media in xml.findall( "{http://bbc.co.uk/2008/mp/mediaselection}media" ):
		if best is None:
			best = media
		if int( media.attrib[ "bitrate" ] ) > int( best.attrib[ "bitrate" ] ):
			best = media
		if int( media.attrib[ "bitrate" ] ) == int( best.attrib[ "bitrate" ] ) and \
			media.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "protocol" ] == "http" and \
			best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "protocol" ] == "rtmp":
			best = media
	if best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "protocol" ] == "http":
		return best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "href" ]
	else:
		connection = best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" )
		return "rtmp://%s/%s/%s?%s" % (
			connection.attrib[ "server" ],
			connection.attrib[ "application" ],
			connection.attrib[ "identifier" ],
			re.sub( "&slist=.+$", "", connection.attrib[ "authString" ] )
		)

def streamvideo( url ):
	"""Get RTMP using an external program."""
#TODO: Doesn't work on Opera?!
	output = "/dev/shm/%s.mp4" % datetime.now().strftime( "%Y%m%d%H%M%S" )
	subprocess.check_output( [ "mplayer", url, "-dumpstream", "-dumpfile", output ] )
	data = None
	if os.path.exists( output ) and os.stat( output ).st_size > 0:
		with open( output, "rb" ) as o:
			data = o.read()
		os.remove( output )
	return data

def rtmpvideo( url ):
	"""Get RTMP using native libraries."""
	conn = librtmp.RTMP( url, live=False )
	conn.connect()
	stream = conn.create_stream()
	data = stream.read( 4096 )
	while len( data ) > 0:
		data += stream.read( 4096 )
	return data

def writemetadata( video_url, video_uuid, b64string, index, page ):
	headers = [
		( WarcRecord.TYPE, WarcRecord.METADATA ),
		( WarcRecord.URL, video_url ),
		( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
		( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
		( WarcRecord.CONCURRENT_TO, video_uuid ),
		( WarcRecord.CONTENT_TYPE, "application/warc-fields" ),
	]
	block = "embedding-page: %s\nembedding-timestamp: %s\nelement-xpath: (//object[param[@name='externalIdentifier']])[%i]\nelement-base64-string: %s" % ( page, timestamp, index+1, b64string )
	warcwriter.write_record( headers, "application/warc-fields", block )

def getvideo( page, timestamp=None ):
	if timestamp is None:
		r = requests.get( page )
		timestamp = datetime.now().strftime( "%Y%m%d%H%M%S" )
	else:
		r = requests.get( "%s/%s/%s" % ( WAYBACK, timestamp, page ) )
	htmlparser = etree.HTMLParser()
	root = etree.fromstring( r.content, htmlparser )
	tree = etree.ElementTree( root )
	for index, object in enumerate( root.xpath( "//object[param[@name='externalIdentifier']]" ) ):
		for param in object.xpath( "param[@name='externalIdentifier']" ):
			video_uuid = "<urn:uuid:%s>" % uuid.uuid1()
			externalid = param.attrib[ "value" ]
			media = try_archived_version( "%s/%s" %( BBC_MEDIA, externalid ), timestamp )
			if not media.ok:
				print "ERROR: Couldn't find media; %s" % ( "%s/%s" %( BBC_MEDIA, externalid ) )
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

			url = getbestvideo( etree.fromstring( media.content ) )
			print url
			if url.startswith( "http" ):
				video = requests.get( url )
				if not video.ok:
					print "ERROR: Couldn't find video; %s" % url
					return
				video_url = video.url
				video_date = warc_datetime_str( parser.parse( video.headers[ "date" ] ) )
				video_type = WarcRecord.RESPONSE
				content_type = "application/http; msgtype=response"
				videoblock = "".join( [ httpheaders( video.raw._original_response ), video.content ] )
				writemetadata( video_url, video_uuid, base64.b64encode( etree.tostring( object ).strip() ), index, page )
			else:
				video_url = url
				video_date = warc_datetime_str( datetime.now() )
				video_type = WarcRecord.RESOURCE
				content_type = "video/mp4"
				writemetadata( video_url, video_uuid, base64.b64encode( etree.tostring( object ).strip() ), index, page )
				videoblock = rtmpvideo( video_url )
				if len( videoblock ) == 0 or videoblock is None:
					print "ERROR: Couldn't stream video; %s" % video_url
					continue
			headers = [
				( WarcRecord.TYPE, video_type ),
				( WarcRecord.URL, video_url ),
				( WarcRecord.DATE, video_date ),
				( WarcRecord.ID, video_uuid ),
				( WarcRecord.CONTENT_TYPE, content_type ),
			]
			warcwriter.write_record( headers, content_type, videoblock )

if __name__ == "__main__":
	warcwriter = WarcWriterPool( gzip=True, write_warcinfo=False )
	for arg in sys.argv[ 1: ]:
		if arg[ 0 ].isdigit():
			timestamp, url = arg.split( "/", 1 )
			getvideo( url, timestamp=timestamp )
		else:
			getvideo( url )
	warcwriter.cleanup()
