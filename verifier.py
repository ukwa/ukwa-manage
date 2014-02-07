#!/usr/local/bin/python2.7

import xml.etree.ElementTree as ET

METS = "{http://www.loc.gov/METS/}"
PREMIS = "{info:lc/xmlns/premis-v2}"

class Warc:
	premisHash = ""
	premisSize = ""
	fileSecHash = ""
	fileSecSize = ""

tree = ET.parse( "/home/rcoram/20130522151742.xml" )
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
			warc.premisHash = objectCharacteristics.find( PREMIS + "fixity" ).find( PREMIS + "messageDigest" ).text
			warc.premisSize = objectCharacteristics.find( PREMIS + "size" ).text
			warcs[ amdSec.attrib[ "ID" ] ] = warc
for fileSec in root.findall( METS + "fileSec" ):
	for fileGrp in fileSec.findall( METS + "fileGrp" ):
		if fileGrp.attrib[ "USE" ] == "DigitalManifestation":
			for file in fileGrp.findall( METS + "file" ):
				warcs[ file.attrib[ "ADMID" ] ].fileSecHash = file.attrib[ "CHECKSUM" ]
				warcs[ file.attrib[ "ADMID" ] ].fileSecSize = file.attrib[ "SIZE" ]

for id, warc in warcs.iteritems():
	if warc.premisHash != warc.fileSecHash:
		print "INVALID HASH: " + id
	if warc.premisSize != warc.fileSecSize:
		print "INVALID SIZE: " + id
