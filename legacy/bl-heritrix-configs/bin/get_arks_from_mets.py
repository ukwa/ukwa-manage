#!/usr/bin/env python

import os
import re
import argparse
import xml.etree.ElementTree as ET

METS = "{http://www.loc.gov/METS/}"
PREMIS = "{info:lc/xmlns/premis-v2}"
XLINK = "{http://www.w3.org/1999/xlink}"

parser = argparse.ArgumentParser( description="Extract ARK-WARC lookup from METS." )
parser.add_argument( "-i", dest="input", type=str, required=True )
args = parser.parse_args()

class Warc:
	ark = ""
	path = ""

tree = ET.parse( args.input )
root = tree.getroot()

warcs = {}

for amdSec in root.findall( METS + "amdSec" ):
	digiprovMD = amdSec.find( METS + "digiprovMD" )
	mdWrap = digiprovMD.find( METS + "mdWrap" )
	if mdWrap.attrib[ "MDTYPE" ] == "PREMIS:OBJECT":
		xmlData = mdWrap.find( METS + "xmlData" )
		object = xmlData.find( PREMIS + "object" )
		objectCharacteristics = object.find( PREMIS + "objectCharacteristics" )
		format = objectCharacteristics.find( PREMIS + "format" )
		formatDesignation = format.find( PREMIS + "formatDesignation" )
		formatName = formatDesignation.find( PREMIS + "formatName" )
		if formatName.text == "application/warc":
			warc = Warc()
			objectIdentifier = object.find( PREMIS + "objectIdentifier" )
			objectIdentifierValue = objectIdentifier.find( PREMIS + "objectIdentifierValue" )
			warc.ark = objectIdentifierValue.text
			warcs[ amdSec.attrib[ "ID" ] ] = warc
for fileSec in root.findall( METS + "fileSec" ):
	for fileGrp in fileSec.findall( METS + "fileGrp" ):
		if fileGrp.attrib[ "USE" ] == "DigitalManifestation" or fileGrp.attrib[ "USE" ] == "ViralFiles":
			for file in fileGrp.findall( METS + "file" ):
				FLocat = file.find( METS + "FLocat" )
				warcs[ file.attrib[ "ADMID" ] ].path = re.sub( "\?.*$", "", os.path.basename( FLocat.attrib[ XLINK + "href" ] ) )

for id, warc in warcs.iteritems():
	print warc.path + "\t" + warc.ark

