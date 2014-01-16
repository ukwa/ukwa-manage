#!/usr/local/bin/python2.7

import os
import re
import sys
import time
import shutil
import httplib
import rfc3987
import urllib2
import logging
import argparse
import heritrix
import StringIO
import subprocess
import dateutil.parser
from lxml import etree
from datetime import datetime
from retry_decorator import *
from optparse import OptionParser
from hanzo.warctools import WarcRecord
from requests.exceptions import ConnectionError

STRIP_PROTOCOL_REGEX = re.compile( "^(https?://)(?:.*)$" )
STRIP_USERINFO_REGEX = re.compile( "^(?:(?:(?:https?)|(?:ftps?))://)([^/]+@)(?:.*)$" )
STRIP_WWW_REGEX = re.compile( "(?i)^(?:https?://)(www[0-9]*\.)(?:[^/]*/.*)$")
STRIP_PHPSESSION_ID_REGEX = re.compile( "^.*(phpsessid=[0-9a-zA-Z]{32})&?$" )
STRIP_JSESSION_ID_REGEX = re.compile( "^.*(jsessionid=[0-9a-zA-Z]{32}&?).*$" )
STRIP_ASPSESSION_REGEX = re.compile( "^.*(ASPSESSIONID[a-zA-Z]{8}=[a-zA-Z]{24})&?$" )
STRIP_ASPSESSION2_REGEX = re.compile( ".*/(\([0-9a-z]{24}\)/)(?:[^\?]+\.aspx.*)$" )
STRIP_ASPSESSION3_REGEX = re.compile( ".*/(\((?:[a-z]\([0-9a-z]{24}\))+\)/)[^\?]+\.aspx.*$" )
STRIP_SID_REGEX = re.compile( "^.*(sid=[0-9a-zA-Z]{32})&?$" )
STRIP_CFSESSION_REGEX = re.compile( ".+(cfid=[^&]+&cftoken=[^&]+(?:&jsessionid=[^&]+)?&?).*$" )

STRIP_PROTOCOL_CHOOSER = ""
STRIP_USERINFO_CHOOSER = "@"
STRIP_WWW_CHOOSER = "/www"
STRIP_PHPSESSION_ID_CHOOSER = "phpsessid="
STRIP_JSESSION_ID_CHOOSER = "jsessionid="
STRIP_ASPSESSION_CHOOSER = "aspsessionid"
STRIP_ASPSESSION2_CHOOSER = ".aspx"
STRIP_ASPSESSION3_CHOOSER = ".aspx"
STRIP_SID_CHOOSER = "sid="
STRIP_CFSESSION_CHOOSER = "cftoken="

expressions = [ ( STRIP_USERINFO_CHOOSER, STRIP_USERINFO_REGEX ), ( STRIP_WWW_CHOOSER, STRIP_WWW_REGEX ), ( STRIP_PHPSESSION_ID_CHOOSER, STRIP_PHPSESSION_ID_REGEX ), ( STRIP_JSESSION_ID_CHOOSER, STRIP_JSESSION_ID_REGEX ), ( STRIP_ASPSESSION_CHOOSER, STRIP_ASPSESSION_REGEX ), ( STRIP_ASPSESSION2_CHOOSER, STRIP_ASPSESSION2_REGEX ), ( STRIP_ASPSESSION3_CHOOSER, STRIP_ASPSESSION3_REGEX ), ( STRIP_SID_CHOOSER, STRIP_SID_REGEX ), ( STRIP_CFSESSION_CHOOSER, STRIP_CFSESSION_REGEX ), ( STRIP_PROTOCOL_CHOOSER, STRIP_PROTOCOL_REGEX ) ]

def canonicalize( url ):
	url = url.lower()
	if url.startswith( "dns" ):
		return url
	url = url.replace( "+", "%2b" )
	#url = urllib2.unquote( url )
	url = url.replace( "%20", "+" )
	for ( chooser, regex ) in expressions:
		if chooser in url:
			match = regex.match( url )
			if match is None:
				continue
			url = url[ 0:match.start( 1 ) ] + url[ match.end(1): ]
	while( url.endswith( "?" ) or url.endswith( "&" ) ):
		url = url[ :-1 ]
	return url

def find( name, path ):
	for root, dirs, files in os.walk( path ):
		if name in files:
			return os.path.join( root, name )

def get_revisited( url, digest, cdx, revisit_warcs ):
	try:
		lines = subprocess.check_output( [ "look", url + " ", cdx ] )
	except subprocess.CalledProcessError:
		return
	io = StringIO.StringIO( lines )
	for line in io.readlines():
		if line.split()[ 5 ] == digest:
			fields = line.strip().split()
			local_path = fields[ 9 ].split( "?" )[ 0 ]
			if os.path.exists( local_path ):
				revisit_warcs.append( local_path )
				fields[ 9 ] = os.path.basename( local_path )
	 			return " ".join( fields )
	print "Couldn't find: " + "look \"" + url + " \" " + cdx + "; " + digest + "\n"

def index_warcs( warcs, cdx, base_cdx=None ):
	output = open( cdx, "wb" )
	lines = []
	revisit_warcs = []
	for warc in warcs:
		fh = WarcRecord.open_archive( warc, gzip="auto" )
		for( offset, record, errors ) in fh.read_records( limit=None ):
			if record and ( record.type == "response" or record.type == "revisit" ):
				digest = record.get_header( "WARC-Payload-Digest" )
				if not digest:
					digest = "-"
				if record.type == "response":
					content_type, content_body = record.content
					stream = StringIO.StringIO( content_body )
					http_code = "-"
					for line in stream:
						if line.startswith( "HTTP" ):
							http_code = line.split( " " )[ 1 ].strip()
						elif line.lower().startswith( "content-type" ):
							content_type = re.split( ":\s+", line )[ 1 ]
						elif len( line.strip() ) == 0:
							break
					if ";" in content_type:
						content_type = content_type.split( ";" )[ 0 ]
					lines.append( " ".join( [ canonicalize( record.url ), re.sub( "[^0-9]", "", record.date ), record.url, content_type.strip(), http_code, digest, "-", "-", str( offset ), os.path.basename( warc ) ] ) )
				elif record.type == "revisit" and digest != "-" and base_cdx is not None:
					revisit = get_revisited( canonicalize( record.url ), digest.split( ":" )[ 1 ], base_cdx, revisit_warcs )
					if revisit is not None:
						lines.append( revisit )
					else:
						#If we can't find the original, put in the revisit record and hope for the best.
						lines.append( " ".join( [ canonicalize( record.url ), re.sub( "[^0-9]", "", record.date ), record.url, "warc/revisit", "-", digest, "-", "-", str( offset ), os.path.basename( warc ) ] ) )
	for warc in revisit_warcs:
		warcs.append( warc )
					
	lines.sort()
	for line in lines:
		if line is not None:
			output.write( line + "\n" )
	output.close()

def generate_path_index( warcs, index ):
	lines = []
	for warc in warcs:
		lines.append( os.path.basename( warc ) + "\t" + warc )
	lines.sort()
	set( lines )
	path_index = open( index, "wb" )
	for line in lines:
		path_index.write( line + "\n" )
	path_index.close()

#TODO: Get this working!
def binary_search( key, sorted_file ):
	fp = open( sorted_file, "rb" )
	fp.seek( 0, 2 )
	begin = 0
	end = fp.tell()
	while( begin < end ):
		fp.seek( ( end - begin ) / 2, 0 )
		fp.readline()
		line = fp.readline()
		line_key = line.split()[ 0 ]
		print line_key
		if( key == line_key ):
			print line
		elif( key > line_key ):
			begin = fp.tell()
		else:
			end = fp.tell()
	fp.close()

if __name__ == "__main__":
	warcs = [ sys.argv[ 1: ] ]
	index_warcs( warcs, "test.cdx" )

