#!/usr/bin/env python

from lxml import etree
from settings import *
from datetime import datetime

SOFTWARE_VERSION="mets;0.1684"

METS = "{http://www.loc.gov/METS/}"
PREMIS = "{info:lc/xmlns/premis-v2}"
WCT = "{http://www.bl.uk/namespaces/wct}"
XLINK = "{http://www.w3.org/1999/xlink}"
XSI = "{http://www.w3.org/2001/XMLSchema-instance}"

schemaLocation = "http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd info:lc/xmlns/premis-v2 http://www.loc.gov/standards/premis/premis.xsd http://www.w3.org/1999/xlink http://www.loc.gov/standards/xlink/xlink.xsd"

def createDomainMets( date ):
	etree.register_namespace( "mets", "http://www.loc.gov/METS/" )
	etree.register_namespace( "mods", "http://www.loc.gov/mods/v3" )
	etree.register_namespace( "premis", "info:lc/xmlns/premis-v2" )
	etree.register_namespace( "wct", "http://www.bl.uk/namespaces/wct" )
	etree.register_namespace( "xsi", "http://www.w3.org/2001/XMLSchema-instance" )
	etree.register_namespace( "xlink", "http://www.w3.org/1999/xlink" )
	mets = etree.Element( METS + "mets", TYPE="webarchive_domain", attrib={ XSI + "schemaLocation" : schemaLocation } )

	metsHdr = etree.SubElement( mets, METS + "metsHdr", CREATEDATE=datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ) )
	agent = etree.SubElement( metsHdr, METS + "agent", ROLE="CREATOR", TYPE="OTHER", OTHERTYPE="software" )
	name = etree.SubElement( agent, METS + "name" )
	name.text = SOFTWARE_VERSION

	amdSec = etree.SubElement( mets, METS + "amdSec", ID="AMD0000" )
	rightsMD = etree.SubElement( amdSec, METS + "rightsMD", ID="DP0000" )
	mdWrap = etree.SubElement( rightsMD, METS + "mdWrap", MDTYPE="OTHER", OTHERMDTYPE="wctpermissions" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	permissions = etree.SubElement( xmlData, WCT + "Permissions" )
	permission = etree.SubElement( permissions, WCT + "Permission" )
	state = etree.SubElement( permission, WCT + "State" )
	state.text = PERMISSION_STATE
	startdate = etree.SubElement( permission, WCT + "StartDate" )
	startdate.text = PERMISSION_START_DATE
	harvestauthorization = etree.SubElement( permission, WCT + "HarvestAuthorization" )
	name = etree.SubElement( harvestauthorization, WCT + "Name" )
	name.text = PERMISSION_NAME
	ispublished = etree.SubElement(harvestauthorization, WCT + "IsPublished" )
	ispublished.text = PERMISSION_PUBLISHED

	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DP0001" )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:EVENT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	event = etree.SubElement( xmlData, PREMIS + "event" )
	eventIdentifier = etree.SubElement( event, PREMIS + "eventIdentifier" )
	eventIdentifierType = etree.SubElement( eventIdentifier, PREMIS + "eventIdentifierType" )
	eventIdentifierType.text = "local"
	eventIdentifierValue = etree.SubElement( eventIdentifier, PREMIS + "eventIdentifierValue" )
	eventIdentifierValue.text = "EVENT0000"
	eventType = etree.SubElement( event, PREMIS + "eventType" )
	eventType.text = "crawlstart"
	eventDateTime = etree.SubElement( event, PREMIS + "eventDateTime" )
	eventDateTime.text = date
	linkingAgentIdentifier = etree.SubElement( event, PREMIS + "linkingAgentIdentifier" )
	linkingAgentIdentifierType = etree.SubElement( linkingAgentIdentifier, PREMIS + "linkingAgentIdentifierType" )
	linkingAgentIdentifierType.text = "local"
	linkingAgentIdentifierValue = etree.SubElement( linkingAgentIdentifier, PREMIS + "linkingAgentIdentifierValue" )
	linkingAgentIdentifierValue.text = "AGENT0000"

	digiprovMD = etree.SubElement( amdSec, METS + "digiprovMD", ID="DP0002" )
	mdWrap = etree.SubElement( digiprovMD, METS + "mdWrap", MDTYPE="PREMIS:AGENT" )
	xmlData = etree.SubElement( mdWrap, METS + "xmlData" )
	agent = etree.SubElement( xmlData, PREMIS + "agent" )
	agentIdentifier = etree.SubElement( agent, PREMIS + "agentIdentifier" )
	agentIdentifierType = etree.SubElement( agentIdentifier, PREMIS + "agentIdentifierType" )
	agentIdentifierType.text = "local"
	agentIdentifierValue = etree.SubElement( agentIdentifier, PREMIS + "agentIdentifierValue" )
	agentIdentifierValue.text = "AGENT0000"
	agentName = etree.SubElement( agent, PREMIS + "agentName" )
	agentName.text = HERITRIX
	agentType = etree.SubElement( agent, PREMIS + "agentType" )
	agentType.text = "software"

#	return etree.tostring( mets, pretty_print=True, xml_declaration=True, encoding="UTF-8" )
	return mets