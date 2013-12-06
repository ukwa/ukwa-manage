#!/usr/local/bin/python2.7

import os
import sys
import time
import shutil
import httplib
import rfc3987
import urllib2
import logging
import argparse
import heritrix
import dateutil.parser
from lxml import etree
from datetime import datetime
from retry_decorator import *
from optparse import OptionParser
from hanzo.warctools import WarcRecord
from requests.exceptions import ConnectionError

parser = OptionParser()
parser.add_option( "-c", "--clamd", dest="clamd", help="Clamd port", default="3310" )
parser.add_option( "-x", "--heritrix", dest="heritrix", help="Heritrix port", default="8443" )
( options, args ) = parser.parse_args()

frequencies = [ "daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual" ]
heritrix_ports = { "daily": "8444", "weekly": "8443", "monthly": "8443", "quarterly": "8443", "sixmonthly": "8443", "annual": "8443" }
clamd_ports = { "daily": "3311", "weekly": "3310", "monthly": "3310", "quarterly": "3310", "sixmonthly": "3310", "annual": "3310" }

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
WARC_ROOT = "/heritrix/output/warcs"
LOG_ROOT = "/heritrix/output/logs"
URL_ROOT = "http://www.webarchive.org.uk/act/websites/export/"

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

	def __init__( self, url, depth="capped", scope="root", ignore_robots=False ):
		self.url = url
		self.depth = depth #capped=default, capped_large=higherLimit, deep=noLimit
		self.scope = scope
		self.surt = self.tosurt( self.url )
		self.ignore_robots = ignore_robots

def verify():
	try:
		api.rescan()
	except ConnectionError:
		logger.error( "Can't connect to Heritrix: ConnectionError" )
		sys.exit( 1 )

def jobs_by_status( api, status  ):
	empty = [] 
	for job in api.listjobs(): 
		if api.status( job ) == status:
			empty.append( ( job, api.launchid( job ) ) )
	return empty

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

def writeJobScript( job, script ):
	with open( HERITRIX_JOBS + "/" + job + "/" + script, "wb" ) as o:
		o.writelines( script )

def runJobScript( input ):
	with open( HERITRIX_JOBS + "/" + job + "/" + script, "rb" ) as i:
		script = i.read()
		api.execute( engine="beanshell", script=script, job=job )

def addSurtAssociations( seeds, job ):
	script= []
	for seed in seeds:
		if seed.depth == "capped_large":
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"higherLimit\" );" % seed.surt )
		if seed.depth == "deep":
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"noLimit\" );" % seed.surt )
		if seed.depth != "capped":
			logger.info( "Amending cap for SURT " + seed.surt + " to " + seed.depth )
	return script

def addScopingRules( seeds, job ):
	script = []
	for seed in seeds:
		if seed.scope == "resource":
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).getOrCreateSheet( \"resourceScope\" ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).putSheetOverlay( \"resourceScope\", \"hopsCountReject.maxHops\", 1 ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"resourceScope\" ); " % seed.surt )
		if seed.scope == "plus1":
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).getOrCreateSheet( \"plus1Scope\" ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).putSheetOverlay( \"plus1Scope\", \"hopsCountReject.maxHops\", 1 ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).putSheetOverlay( \"plus1Scope\", \"redirectAccept.enabled\", true ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"plus1Scope\" ); " % seed.surt )
		if seed.scope == "subdomains":
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).getOrCreateSheet( \"subdomainsScope\" ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).putSheetOverlay( \"subdomainsScope\", \"onDomainAccept.enabled\", true ); " )
			script.append( "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"subdomainsScope\" ); " % seed.surt )
		if seed.scope != "root":
			logger.info( "Setting scope for SURT %s to %s." % ( seed.surt, seed.scope ) )
	return script

def ignoreRobots( seeds, job ):
	for seed in seeds:
		if seed.ignore_robots:
			script = "appCtx.getBean( \"sheetOverlaysManager\" ).addSurtAssociation( \"%s\", \"ignoreRobots\" );" % seed.surt
			logger.info( "Ignoring robots.txt for SURT " + seed.surt )
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
	xmlstring = xmlstring.replace( "REPLACE_CLAMD_PORT", clamd_ports[ frequency ] )
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
	script = addSurtAssociations( seeds, newjob )
	#TODO
	#script += addScopingRules( seeds, newjob )
	if len( script ) > 0:
		writeJobScript( newjob, script )
		runJobScript( newjob )
	ignoreRobots( seeds, newjob )
	logger.info( "Unpausing %s", newjob )
	api.unpause( newjob )
	waitfor( newjob, "RUNNING" )
	logger.info( "%s running. Exiting.", newjob )

@retry( urllib2.URLError, tries=20, timeout_secs=10 )
def callAct( url ):
	return urllib2.urlopen( url )

for frequency in frequencies:
	SEED_FILE = "/heritrix/" + frequency + "-seeds.txt"
	seeds = []
	now = datetime.now()

	try:
		xml = callAct( URL_ROOT + frequency )
		xml = xml.read()
	except urllib2.URLError, e:
		logger.error( "Cannot read ACT! " + str( e ) + " [" + frequency + "]" )
		continue
	except httplib.IncompleteRead, i:
		logger.error( "IncompleteRead: " + str( i.partial ) + " [" + frequency + "]" )
		continue

	def add_seeds( urls, depth, scope, ignore_robots ):
		for url in urls.split():
			try:
				seed = Seed( url=url, depth=depth, scope=scope, ignore_robots=ignore_robots )
				seeds.append( seed )
			except ValueError, v:
				logger.error( "INVALID URL: " + url )

	dom = etree.fromstring( xml )
	#crawlDateRange will be:
	#	blank			Determined by frequency.
	#	"start_date"		Determined by frequency if start_date < now.
	#	"start_date end_date"	Determined by frequency if start_date < now and end_date > now
	for node in dom.findall( "node" ):
		start_date = node.find( "crawlStartDate" ).text
		end_date = node.find( "crawlEndDate" ).text
		depth = node.find( "depth" ).text
		scope = node.find( "scope" ).text
		ignore_robots = ( node.find( "ignoreRobots.txt" ).text != None )

		# If there's no end-date or it's in the future, we're okay.
		if end_date is None or dateutil.parser.parse( end_date ) > now:
			# If there's no start date, use the defaults.
			if start_date is None:
				logger.error( "Empty start_date: " + node.find( "urls" ).text )
				continue
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
						add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
					if frequency == "weekly" and dotw == now.weekday():
						add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
					# Remaining frequencies all run on a specific day.
					if dotm == now.day:
						if frequency == "monthly":
							add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
						if frequency == "quarterly" and moty%3 == now.month%3:
							add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
						if frequency == "sixmonthly" and moty%6 == now.month%6:
							add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
						if frequency == "annual" and moty == now.month:
							add_seeds( node.find( "urls" ).text, depth, scope, ignore_robots )
	if len( seeds ) > 0:
		if frequency == "daily": 
			jobname = frequency + "-" + now.strftime( "%H" ) + "00"
		if frequency == "weekly":
			jobname = frequency + "-" + now.strftime( "%a%H" ).lower() + "00"
		if frequency == "monthly":
			jobname = frequency + "-" + now.strftime( "%d%H" ) + "00"
		if frequency == "quarterly":
			jobname = frequency + "-" + str( now.month%3 ) + "-" + now.strftime( "%d%H" ) + "00"
		if frequency == "sixmonthly":
			jobname = frequency + "-" + str( now.month%6 ) + "-" + now.strftime( "%d%H" ) + "00"
		if frequency == "annual":
			jobname = frequency + "-" + now.strftime( "%m%d%H" ) + "00"
		api = heritrix.API( host="https://opera.bl.uk:" + heritrix_ports[ frequency ] + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False )
		submitjob( jobname, seeds, frequency )

