#!/usr/local/bin/python2.7

import os
import sys
import time
import shutil
import rfc3987
import urllib2
import logging
import argparse
import heritrix
import dateutil.parser
from lxml import etree
from datetime import datetime
from optparse import OptionParser
from requests.exceptions import ConnectionError

parser = OptionParser()
parser.add_option( "-c", "--clamd", dest="clamd", help="Clamd port", default="3310" )
parser.add_option( "-x", "--heritrix", dest="heritrix", help="Heritrix port", default="8443" )
( options, args ) = parser.parse_args()

frequencies = [ "daily", "weekly", "monthly", "quarterly", "sixmonthly" ]
ports = { "daily": "8444", "weekly": "8443", "monthly": "8443", "quarterly": "8443", "sixmonthly": "8443" }

DEFAULT_HOUR = 12
DEFAULT_DAY = 8
DEFAULT_WEEKDAY = 0
DEFAULT_MONTH = 1
DEFAULT_WAIT = 10

CONFIG_ROOT = "/heritrix/git/heritrix_bl_configs"
HERITRIX_PROFILES = CONFIG_ROOT + "/profiles"
HERITRIX_EXCLUDE = CONFIG_ROOT + "/exclude.txt"
HERITRIX_SHORTENERS = CONFIG_ROOT + "/url.shorteners.txt"
HERITRIX_SURTS = CONFIG_ROOT + "/surts.txt"
HERITRIX_JOBS = "/opt/heritrix/jobs"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "frequency" )

api = None

class Seed:
	def tosurt( self, url ):
		parsed = rfc3987.parse( url, rule="URI" )
		authority = parsed[ "authority" ].split( "." )
		authority.reverse()
		return "http://(" + ",".join( authority ) + ","

	def __init__( self, url, depth="capped" ):
		self.url = url
		self.depth = depth #capped=default, capped_large=higherLimit, deep=noLimit
		self.surt = self.tosurt( self.url )

def verify():
	try:
		api.rescan()
	except ConnectionError:
		logger.error( "Can't connect to Heritrix: ConnectionError" )
		sys.exit( 1 )

def waitfor( job, status ):
	while api.status( job ) != status:
		time.sleep( 10 )

def kill( newjob ):
	for job in api.listjobs():
		if job.startswith( newjob ) and api.status( job ) != "":
			logger.info( "Killing alread-running job: " + job )
			api.pause( job )
			waitfor( job, "PAUSED" )
			api.terminate( job )
			waitfor( job, "FINISHED" )
			api.teardown( job )
			waitfor( job, "" )

def setupjobdir( newjob ):
	root = HERITRIX_JOBS + "/" + newjob
	if not os.path.isdir( root ):
		logger.info( "Adding %s to Heritrix" % newjob )
		os.mkdir( root )
		api.add( HERITRIX_JOBS + "/" + newjob )
	return root

def addSurtAssociations( seeds, job ):
	for seed in seeds:
		if seed.depth == "capped_large":
			script = "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"higherLimit\" );" % seed.surt
		if seed.depth == "deep":
			script = "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"noLimit\" );" % seed.surt
		if seed.depth != "capped":
			logger.info( "Amending cap for SURT " + seed.surt + " to " + seed.depth )
			api.execute( engine="beanshell", script=script, job=job )

def submitjob( newjob, seeds, frequency ):
	verify()
	kill( newjob )
	root = setupjobdir( newjob )

	#Set up profile, etc.
	profile = HERITRIX_PROFILES + "/profile-" + frequency + ".cxml"
	if not os.path.exists( profile ):
		logger.error( "Cannot find profile for " + frequency + " [" + profile + "]" )
		return
	tree = etree.parse( profile )
	tree.xinclude()
	xmlstring = etree.tostring( tree, pretty_print=True, xml_declaration=True, encoding="UTF-8" )
	#Replace values
	xmlstring = xmlstring.replace( "REPLACE_JOB_NAME", newjob )
	xmlstring = xmlstring.replace( "REPLACE_CLAMD_PORT", options.clamd )
	xmlstring = xmlstring.replace( "REPLACE_JOB_ROOT", newjob )
	xmlstring = xmlstring.replace( "REPLACE_HERITRIX_JOBS", HERITRIX_JOBS )
	cxml = open( root + "/crawler-beans.cxml", "w" )
	cxml.write( xmlstring )
	cxml.close()
	#Copy files
	shutil.copy( HERITRIX_SHORTENERS, root )
	shutil.copy( HERITRIX_EXCLUDE, root )
	shutil.copy( HERITRIX_SURTS, root )
	#Write seeds
	seedstxt = open( root + "/seeds.txt", "w" )
	for seed in seeds:
		seedstxt.write( seed.url + "\n" )
	seedstxt.close()
	#Start the new job
	logger.info( "Building %s", newjob )
	api.build( newjob )
	waitfor( newjob, "NASCENT" )
	logger.info( "Launching %s", newjob )
	api.launch( newjob )
	waitfor( newjob, "PAUSED" )
	#Add SURT associations for caps.
	addSurtAssociations( seeds, newjob )
	logger.info( "Unpausing %s", newjob )
	api.unpause( newjob )
	waitfor( newjob, "RUNNING" )
	logger.info( "%s running. Exiting.", newjob )

for frequency in frequencies:
	URL_ROOT = "http://www.webarchive.org.uk/act/websites/export/"
	SEED_FILE = "/heritrix/" + frequency + "-seeds.txt"
	seeds = []

	try:
		xml = urllib2.urlopen( URL_ROOT + frequency ).read()
	except urllib2.URLError, e:
		logger.error( "Cannot read ACT! " + str( e ) )
		sys.exit( 1 )

	def add_seeds( urls, depth ):
		for url in urls.split():
			try:
				seed = Seed( url=url, depth=depth )
				seeds.append( seed )
			except ValueError, v:
				logger.error( "INVALID URL: " + url )

	dom = etree.fromstring( xml )
	now = datetime.now()
	start_date = None
	#crawlDateRange will be:
	#	blank			Determined by frequency.
	#	"start_date"		Determined by frequency if start_date < now.
	#	"start_date end_date"	Determined by frequency if start_date < now and end_date > now
	for node in dom.findall( "node" ):
		start_date = node.find( "crawlStartDate" ).text
		end_date = node.find( "crawlEndDate" ).text
		depth = node.find( "depth" ).text

		# If there's no end-date or it's in the future, we're okay.
		if end_date is None or dateutil.parser.parse( end_date ) > now:
			# If there's no start date, use the defaults.
			if start_date is None:
				start_date = now
				hotd = DEFAULT_HOUR
				dotw = DEFAULT_WEEKDAY
				dotm = DEFAULT_DAY
				moty = DEFAULT_MONTH
			else:
				start_date = dateutil.parser.parse( start_date )
				hotd = start_date.hour
				dotw = start_date.weekday()
				dotm = start_date.day
				moty = start_date.month

			if start_date <= now:
				# All frequencies are hour-dependent.
				if hotd == now.hour:
					if frequency == "daily": 
						add_seeds( node.find( "urls" ).text, depth )
					if frequency == "weekly" and dotw == now.weekday():
						add_seeds( node.find( "urls" ).text, depth )
					# Remaining frequencies all run on a specific day.
					if dotm == now.day:
						if frequency == "monthly":
							add_seeds( node.find( "urls" ).text, depth )
						if frequency == "quarterly" and moty%3 == now.month%3:
							add_seeds( node.find( "urls" ).text, depth )
						if frequency == "sixmonthly" and moty%6 == now.month%6:
							add_seeds( node.find( "urls" ).text, depth )
	if len( seeds ) > 0:
		if frequency == "daily": 
			jobname = frequency + "-" + start_date.strftime( "%H" ) + "00"
		if frequency == "weekly":
			jobname = frequency + "-" + start_date.strftime( "%a%H" ).lower() + "00"
		if frequency == "monthly":
			jobname = frequency + "-" + start_date.strftime( "%d%H" ) + "00"
		if frequency == "quarterly" or frequency == "sixmonthly":
			jobname = frequency + "-" + start_date.strftime( "%m%d%H" ) + "00"
		api = heritrix.API( host="https://opera.bl.uk:" + ports[ frequency ] + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False )
		submitjob( jobname, seeds, frequency )
