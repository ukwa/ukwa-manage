#!/usr/bin/env python

import os
import sys
import uuid
import requests
from lxml import etree
from datetime import datetime
from hanzo.warctools import WarcRecord
from warcwriterpool import WarcWriterPool
from hanzo.warctools.warc import warc_datetime_str

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

def getvideo( page ):
	r = requests.get( page )
	parser = etree.HTMLParser()
	root = etree.fromstring( r.content, parser )
	tree = etree.ElementTree( root )
	for div in root.xpath( "//div[contains(@class, 'videoInStory')]" ):
		for param in div.xpath( "//param[@name='playlist']" ):
			playlist = requests.get( param.attrib[ "value" ] )
#TODO: Should probably store this is a 'response'?
			xml = etree.fromstring( playlist.content )
			item = xml.find( "{http://bbc.co.uk/2008/emp/playlist}item" )
			group = item.attrib[ "group" ]
			r = requests.get( "%s/%s" %( BBC_MEDIA, group ) )
#TODO: Should probably store this is a 'response'?
			xml = etree.fromstring( r.content )
			best = None
			for media in xml.findall( "{http://bbc.co.uk/2008/mp/mediaselection}media" ):
				if best is None:
					best = media
				if int( media.attrib[ "bitrate" ] ) > int( best.attrib[ "bitrate" ] ) and media.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib.has_key( "href" ):
					best = media
			url = best.find( "{http://bbc.co.uk/2008/mp/mediaselection}connection" ).attrib[ "href" ]
			r = requests.get( url )

			response_uuid = "<urn:uuid:%s>" % uuid.uuid1()
			headers = [
				( WarcRecord.TYPE, WarcRecord.METADATA ),
				( WarcRecord.URL, url ),
				( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
				( WarcRecord.ID, response_uuid ),
				( WarcRecord.CONCURRENT_TO, "<urn:uuid:%s>" % uuid.uuid1() ),
				( WarcRecord.CONTENT_TYPE, "application/warc-fields" ),
			]
			block = "embedding-page: %s\nelement-xpath: %s" % ( page, tree.getpath( div ) )
			warcwriter.write_record( headers, "application/warc-fields", block )

			headers = [
				( WarcRecord.TYPE, WarcRecord.RESPONSE ),
				( WarcRecord.URL, url ),
				( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
				( WarcRecord.ID, response_uuid ),
				( WarcRecord.CONTENT_TYPE, "application/http; msgtype=response" ),
			]
			block = "".join( [ httpheaders( r.raw._original_response ), r.content ] )
			warcwriter.write_record( headers, r.headers[ "content-type" ], block )

if __name__ == "__main__":
	warcwriter = WarcWriterPool( gzip=True )
	for arg in sys.argv[ 1: ]:
		getvideo( arg )
	warcwriter.cleanup()

