#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import uuid
import base64
import argparse
import requests
import mimetypes
import subprocess
import youtube_dl
from lxml import etree
from datetime import datetime
from hanzo.warctools import WarcRecord
from dateutil import parser as dateparser
from warcwriterpool import WarcWriterPool, warc_datetime_str

__version__ = "0.2.0"

DASH_AUDIO = 140
DASH_VIDEO = 137

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

def write_metadata( video_url, video_uuid, timestamp, xpath, page, warcdate ):
    headers = [
        ( WarcRecord.TYPE, WarcRecord.METADATA ),
        ( WarcRecord.URL, page ),
        ( WarcRecord.DATE, warcdate ),
        ( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
        ( WarcRecord.CONCURRENT_TO, video_uuid ),
        ( WarcRecord.CONTENT_TYPE, "text/plain" ),
    ]
    timestamp = re.sub( "[^0-9]", "", timestamp )
    block = "embedded-video: %s\nembedding-timestamp: %s\nembedded-video-xpath: %s" % ( str(video_url), timestamp, xpath )
    warcwriter.write_record( headers, "text/plain", block )

def write_record( url, video_uuid, timestamp, xpath, page, concurrent_to=None ):
    r = requests.get( url )
    if not r.ok:
        print "ERROR: %s" % r.content
        sys.exit( 1 )
    warcdate = warc_datetime_str( dateparser.parse( r.headers[ "date" ] ) )
    if args.multiple:
        for pair in args.multiple.split( "," ):
            t, p = pair.split( "/", 1 )
            write_metadata( r.url, video_uuid, t, xpath, p, warcdate )
    else:
        write_metadata( r.url, video_uuid, timestamp, xpath, page, warcdate )
    headers = [
        ( WarcRecord.TYPE, WarcRecord.RESPONSE ),
        ( WarcRecord.URL, r.url ),
        ( WarcRecord.DATE, warcdate ),
        ( WarcRecord.ID, video_uuid ),
        ( WarcRecord.CONTENT_TYPE, "application/http; msgtype=response" ),
    ]
    if concurrent_to is not None:
        headers.append( ( WarcRecord.CONCURRENT_TO, concurrent_to) )
    block = "".join( [ httpheaders( r.raw._original_response ), r.content ] )
    warcwriter.write_record( headers, "application/http; msgtype=response", block )


def write_playlist( page, timestamp, xpath, videos, filenames ):
    urls = videos.split( "," )
    files = filenames.split( "," )
    if len( urls ) != len( files ):
        print "ERROR: Incorrect number of videos/filenames passed."
        return
    headers = [
        ( WarcRecord.TYPE, WarcRecord.METADATA ),
        ( WarcRecord.URL, page ),
        ( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
        ( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
        ( WarcRecord.CONTENT_TYPE, "text/plain" ),
    ]
    block = ""
    for index, url in enumerate( urls ):
        block += "embedded-playlist-item-%s: %s\n" % ( index, url )
    block += "embedding-timestamp: %s\nembedded-playlist-xpath: %s" % ( timestamp, xpath )
    warcwriter.write_record( headers, "text/plain", block )
    for url, file in zip( urls, files ):
        data = None
        with open( file, "rb" ) as d:
            data = d.read()
        if len( data ) == 0 or data is None:
            print "ERROR: %s" % file
            return
        mime, encoding = mimetypes.guess_type( file )
        mtime = os.stat( file ).st_mtime
        headers = [
            ( WarcRecord.TYPE, WarcRecord.RESOURCE ),
            ( WarcRecord.URL, url ),
            ( WarcRecord.DATE, warc_datetime_str( datetime.fromtimestamp( mtime ) ) ),
            ( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
            ( WarcRecord.CONTENT_TYPE, mime ),
        ]
        warcwriter.write_record( headers, mime, data )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument( "-m", dest="multiple", help="Multiple, comma-separated timestamp/page values." )
    parser.add_argument( "-p", dest="page", help="Embedding page." )
    parser.add_argument( "-t", dest="timestamp", help="Embedding page timestamp." )
    parser.add_argument( "-x", dest="xpath", help="XPath to element." )
    parser.add_argument( "-u", dest="url", help="Video URL." )
    parser.add_argument( "-f", dest="filename", help="Filename on disk." )
    parser.add_argument( "-l", dest="playlist", help="Playlist of videos." )
    parser.add_argument( "-y", action="store_true", help="YouTube videos [iframes only]." )
    
    args = parser.parse_args()
    warcwriter = WarcWriterPool( gzip=True, write_warcinfo=False )
    
    if args.playlist:
        write_playlist( args.page, args.timestamp, args.xpath, args.playlist, args.filename )
    elif not args.filename:
        if args.y:
            r = requests.get( args.page )
            if not r.ok:
                print "ERROR: %s" % r.content
                sys.exit( 1 )
            ydl = youtube_dl.YoutubeDL()
            ydl.add_default_info_extractors()
            htmlparser = etree.HTMLParser()
            root = etree.fromstring( r.content, htmlparser )
            for iframe in root.xpath( "//iframe[contains(@src,'www.youtube.com/embed/')]" ):
                yurl = iframe.attrib["src"]
                results = ydl.extract_info(yurl, download=False )
		headers = [
                    ( WarcRecord.TYPE, WarcRecord.WARCINFO ),
                    ( WarcRecord.DATE, warc_datetime_str( datetime.now() ) ),
                    ( WarcRecord.ID, "<urn:uuid:%s>" % uuid.uuid1() ),
                ]
		warcwriter.write_record( headers, "application/json", json.dumps( results ) )
                xpath = "//iframe[contains(@src,'%s')]" % results["id"]
                for format in results["formats"]:
                    date = warc_datetime_str( datetime.now() )
                    audio_uuid = "<urn:uuid:%s>" % uuid.uuid1()
                    video_uuid = "<urn:uuid:%s>" % uuid.uuid1()
                    if format["format_id"] == DASH_AUDIO:
                        write_record(format["url"], audio_uuid, date, xpath, args.page, concurrent_to=video_uuid)
                    elif format["format_id"] == DASH_VIDEO:
                            write_record(format["url"], video_uuid, date, xpath, args.page, concurrent_to=audio_uuid)
                    else:
                            print format
                            write_record(format["url"], "<urn:uuid:%s>" % uuid.uuid1(), warc_datetime_str( datetime.now() ), xpath, args.page, concurrent_to=None )
        else:
            write_record( args.url, "<urn:uuid:%s>" % uuid.uuid1(), args.timestamp, args.xpath, args.page, concurrent_to=None )
        
    elif args.filename:
        data = None
        with open( args.filename, "rb" ) as d:
            data = d.read()
        if len( data ) == 0 or data is None:
            print "ERROR: %s" % args.filename
            sys.exit( 1 )
        mime, encoding = mimetypes.guess_type( args.filename )
        mtime = os.stat( args.filename ).st_mtime
        warcdate = warc_datetime_str( datetime.fromtimestamp( mtime ) )
        if args.multiple:
            for pair in args.multiple.split( "," ):
                t, p = pair.split( "/", 1 )
                write_metadata( args.url, video_uuid, t, args.xpath, p, warcdate )
        else:
            write_metadata( args.url, video_uuid, args.timestamp, args.xpath, args.page, warcdate )
        headers = [
            ( WarcRecord.TYPE, WarcRecord.RESOURCE ),
            ( WarcRecord.URL, args.url ),
            ( WarcRecord.DATE, warc_datetime_str( datetime.fromtimestamp( mtime ) ) ),
            ( WarcRecord.ID, video_uuid ),
            ( WarcRecord.CONTENT_TYPE, mime ),
        ]
        warcwriter.write_record( headers, mime, data )
    warcwriter.cleanup()
