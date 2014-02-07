#!/usr/bin/env python

import os
import hashlib
import commands
from datetime import datetime
from lxml import etree
from settings import *

SOFTWARE_VERSION="create_mets;0.1682"
CLAMDSCAN = commands.getstatusoutput( "clamdscan --config-file=" + CLAMD_CONF + " --version" )[ 1 ].strip()
METS= "{http://www.loc.gov/METS/}"
MODS = "{http://www.loc.gov/mods/v3}"
PREMIS = "{info:lc/xmlns/premis-v2}"
WCT = "{http://www.bl.uk/namespaces/wct}"
XSI = "{http://www.w3.org/2001/XMLSchema-instance}"
XLINK = "{http://www.w3.org/1999/xlink}"

schemaLocation = "http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-3.xsd info:lc/xmlns/premis-v2 http://www.loc.gov/standards/premis/premis.xsd http://www.w3.org/1999/xlink http://www.loc.gov/standards/xlink/xlink.xsd"

def calculateHash( path ):
	sha = hashlib.sha512()
	file = open( path, "rb" )
	while True:
		data = file.read( 10485760 )
		if not data:
			file.close()
			break
		sha.update( data )
	return sha.hexdigest()

count = 1
def getCount():
	global count
	val = "%04d" % count
	count += 1
	return val

class Warc:
	path = ""
	hash = ""
	size= -1
	admid = ""
	job = ""

	def __init__( self, path, job ):
		self.path = path
		self.job = job
		self.hash = calculateHash( path )
		self.size = os.path.getsize( path )
		self.admid = getCount()

class ZipContainer:
	path = ""
	hash = ""
	size = -1
	admid = ""
	job = ""

	def __init__( self, path, job ):
		self.path = path
		self.job = job
		self.admid = getCount()

	def updateMetadata( self ):
		self.hash = calculateHash( self.path )
		self.size = os.path.getsize( self.path )

def buildZipPremis( root, zip, identifier ):
	amdSec = etree.SubElement( root, METS + "amdSec", ID="AMDZIP" + zip.admid )
	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DMDZIP" + zip.admid )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:OBJECT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	object = etree.SubElement( xmlData, PREMIS + "object" )
	object.set( XSI + "type", "premis:file" )
	objectIdentifier = etree.SubElement( object, PREMIS + "objectIdentifier" )
	objectIdentifierType = etree.SubElement( objectIdentifier, PREMIS + "objectIdentifierType" )
	objectIdentifierType.text = "ARK"
	objectIdentifierValue = etree.SubElement( objectIdentifier, PREMIS + "objectIdentifierValue" )
	objectIdentifierValue.text = identifier
	objectCharacteristics = etree.SubElement( object, PREMIS + "objectCharacteristics" )
	compositionLevel = etree.SubElement( objectCharacteristics, PREMIS + "compositionLevel" )
	compositionLevel.text = "1" 
	fixity = etree.SubElement( objectCharacteristics, PREMIS + "fixity" )
	messageDigestAlgorithm = etree.SubElement( fixity, PREMIS + "messageDigestAlgorithm" )
	messageDigestAlgorithm.text = "SHA-512"
	messageDigest = etree.SubElement( fixity, PREMIS + "messageDigest" )
	messageDigest.text = zip.hash
	size = etree.SubElement( objectCharacteristics, PREMIS + "size" )
	size.text = str( zip.size )
	format = etree.SubElement( objectCharacteristics, PREMIS + "format" )
	formatDesignation = etree.SubElement( format, PREMIS + "formatDesignation" )
	formatName = etree.SubElement( formatDesignation, PREMIS + "formatName" )
	formatName.text = "application/zip"

def buildPremis( root, warc, identifier, virus=False ):
	amdSec = etree.SubElement( root, METS + "amdSec", ID="AMDWARC" + warc.admid )
	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DMDWARC" + warc.admid )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:OBJECT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	object = etree.SubElement( xmlData, PREMIS + "object" )
	object.set( XSI + "type", "premis:file" )
	objectIdentifier = etree.SubElement( object, PREMIS + "objectIdentifier" )
	objectIdentifierType = etree.SubElement( objectIdentifier, PREMIS + "objectIdentifierType" )
	objectIdentifierType.text = "ARK"
	objectIdentifierValue = etree.SubElement( objectIdentifier, PREMIS + "objectIdentifierValue" )
	objectIdentifierValue.text = identifier
	objectCharacteristics = etree.SubElement( object, PREMIS + "objectCharacteristics" )
	compositionLevel = etree.SubElement( objectCharacteristics, PREMIS + "compositionLevel" )
	compositionLevel.text = "1" 
	fixity = etree.SubElement( objectCharacteristics, PREMIS + "fixity" )
	messageDigestAlgorithm = etree.SubElement( fixity, PREMIS + "messageDigestAlgorithm" )
	messageDigestAlgorithm.text = "SHA-512"
	messageDigest = etree.SubElement( fixity, PREMIS + "messageDigest" )
	messageDigest.text = warc.hash
	size = etree.SubElement( objectCharacteristics, PREMIS + "size" )
	size.text = str( warc.size )
	format = etree.SubElement( objectCharacteristics, PREMIS + "format" )
	formatDesignation = etree.SubElement( format, PREMIS + "formatDesignation" )
	formatName = etree.SubElement( formatDesignation, PREMIS + "formatName" )
	formatName.text = "application/warc"

	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DMDWARC" + warc.admid + "_EVENT" )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:EVENT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	event = etree.SubElement( xmlData, PREMIS + "event" )
	eventIdentifier = etree.SubElement( event, PREMIS + "eventIdentifier" )
	eventIdentifierType = etree.SubElement( eventIdentifier, PREMIS + "eventIdentifierType" )
	eventIdentifierType.text = "local"
	eventIdentifierValue = etree.SubElement( eventIdentifier, PREMIS + "eventIdentifierValue" )
	eventIdentifierValue.text = "EVENT" + warc.admid
	eventType = etree.SubElement( event, PREMIS + "eventType" )
	eventType.text = "virusCheck"
	eventDateTime = etree.SubElement( event, PREMIS + "eventDateTime" )
	eventDateTime.text = datetime.fromtimestamp( os.path.getmtime( warc.path ) ).strftime( "%Y-%m-%dT%H:%M:%S" )
	eventOutcomeInformation = etree.SubElement( event, PREMIS + "eventOutcomeInformation" )
	eventOutcome = etree.SubElement( eventOutcomeInformation, PREMIS + "eventOutcome" )
	if( virus ):
		eventOutcome.text = "viral, failed but forced"
	else:
		eventOutcome.text = "no virus detected"
	linkingAgentIdentifier = etree.SubElement( event, PREMIS + "linkingAgentIdentifier" )
	linkingAgentIdentifierType = etree.SubElement( linkingAgentIdentifier, PREMIS + "linkingAgentIdentifierType" )
	linkingAgentIdentifierType.text = "local"
	linkingAgentIdentifierValue = etree.SubElement( linkingAgentIdentifier, PREMIS + "linkingAgentIdentifierValue" )
	linkingAgentIdentifierValue.text = "AGENT" + warc.admid

	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DMDWARC" + warc.admid + "_AGENT" )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:AGENT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	agent = etree.SubElement( xmlData, PREMIS + "agent" )
	agentIdentifier = etree.SubElement( agent, PREMIS + "agentIdentifier" )
	agentIdentifierType = etree.SubElement( agentIdentifier, PREMIS + "agentIdentifierType" )
	agentIdentifierType.text = "local"
	agentIdentifierValue = etree.SubElement( agentIdentifier, PREMIS + "agentIdentifierValue" )
	agentIdentifierValue.text = "AGENT" + warc.admid
	agentName = etree.SubElement( agent, PREMIS + "agentName" )
	agentName.text = CLAMDSCAN
	agentType = etree.SubElement( agent, PREMIS + "agentType" )
	agentType.text = "software"

def buildWarcFileGrp( root, files, use, subdir="/warcs/" ):
	fileGrp = etree.SubElement( root, METS + "fileGrp", USE=use )
	for warc in files:
		metsFile = etree.SubElement( fileGrp, METS + "file", ID="WARC" + warc.admid, ADMID="AMDWARC" + warc.admid, SIZE=str( warc.size ), CHECKSUM=warc.hash, CHECKSUMTYPE="SHA-512", MIMETYPE="application/warc" )
		fLocat = etree.SubElement( metsFile, METS + "FLocat", LOCTYPE="URL",  )
		fLocat.set( XLINK + "href", HOOP + warc.path + HOOP_SUFFIX )
		transformFile = etree.SubElement( metsFile, METS + "transformFile", TRANSFORMTYPE="decompression", TRANSFORMALGORITHM="WARC", TRANSFORMORDER="1" )

def buildLogFileGrp( root, logs ):
	fileGrp = etree.SubElement( root, METS + "fileGrp", USE="Logfiles" )
	for zip in logs:
		metsFile = etree.SubElement( fileGrp, METS + "file", ID="ZIP" + zip.admid, ADMID="AMDZIP" + zip.admid, SIZE=str( zip.size ), CHECKSUM=zip.hash, CHECKSUMTYPE="SHA-512", MIMETYPE="application/zip" )
		fLocat = etree.SubElement( metsFile, METS + "FLocat", LOCTYPE="URL",  )
		fLocat.set( XLINK + "href", HOOP + zip.path + HOOP_SUFFIX )
		transformFile = etree.SubElement( metsFile, METS + "transformFile", TRANSFORMTYPE="decompression", TRANSFORMALGORITHM="ZIP", TRANSFORMORDER="1" )

def outputStructMap( root, warcs, viral, logs ):
	structMap = etree.SubElement( root, METS + "structMap", TYPE="logical" )
	div = etree.SubElement( structMap, METS + "div", ID="div0000", TYPE="uk-web-domain", ADMID="AMD0000" )
	for warc in warcs:
		fptr = etree.SubElement( div, METS + "fptr", FILEID="WARC" + warc.admid )
	for viral in viral:
		fptr = etree.SubElement( div, METS + "fptr", FILEID="WARC" + viral.admid )
	for zip in logs:
		fptr = etree.SubElement( div, METS + "fptr", FILEID="ZIP" + zip.admid )
	

def createCrawlerMets( mets, warcs, viral, logs, identifiers ):
	for warc in warcs:
		buildPremis( mets, warc, identifiers.pop() )

	for warc in viral:
		buildPremis( mets, warc, identifiers.pop(), True )
	
	for zip in logs:
		buildZipPremis( mets, zip, identifiers.pop() )

	fileSec = etree.SubElement( mets, METS + "fileSec" )
	buildWarcFileGrp( fileSec, warcs, "DigitalManifestation" )
	buildWarcFileGrp( fileSec, viral, "ViralFiles", "/viral/" )
	buildLogFileGrp( fileSec, logs )
	
	outputStructMap( mets, warcs, viral, logs )

#	return etree.tostring( mets, pretty_print=True, xml_declaration=True, encoding="UTF-8" )
	return mets