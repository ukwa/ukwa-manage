#!/usr/bin/env python

"""
Generic methods used for verifying/indexing SIPs.
"""

import re
import pika
import logging
import tarfile
import webhdfs
from lxml import etree
from StringIO import StringIO

SIP_ROOT="/heritrix/sips"
NS={"mets": "http://www.loc.gov/METS/", "premis": "info:lc/xmlns/premis-v2"}
XLINK="{http://www.w3.org/1999/xlink}"
WEBHDFS="http://194.66.232.90:14000/webhdfs/v1"

def get_identifiers(sip):
	"""Parses the SIP in HDFS and retrieves ARKs."""
	w = webhdfs.API(prefix=WEBHDFS)
	arks = []
	tar = "%s/%s.tar.gz" % (SIP_ROOT, sip)
	if w.exists(tar):
		logger.debug("Found %s" % tar)
		t = w.open(tar)
		tar = tarfile.open(mode="r:gz", fileobj=StringIO(t))
		for i in tar.getmembers():
			if i.name.endswith(".xml"):
				xml = tar.extractfile(i).read()
				tree = etree.fromstring(xml)
				for warc in tree.xpath("//premis:object[premis:objectCharacteristics/premis:format/premis:formatDesignation/premis:formatName='application/warc']", namespaces=NS):
					for id in warc.xpath("premis:objectIdentifier/premis:objectIdentifierValue", namespaces=NS):
						arks.append(id.text.replace("ark:/81055/", ""))
	else:
		logger.warning("Could not find SIP: hdfs://%s" % tar)
	return arks

def get_warc_identifiers(sip):
	"""Parses the SIP in HDFS and retrieves WARC/ARK tuples."""
	w = webhdfs.API(prefix=WEBHDFS)
	identifiers = []
	tar = "%s/%s.tar.gz" % (SIP_ROOT, sip)
	if w.exists(tar):
		logger.debug("Found %s" % tar)
		t = w.open(tar)
		tar = tarfile.open(mode="r:gz", fileobj=StringIO(t))
		for i in tar.getmembers():
			if i.name.endswith(".xml"):
				xml = tar.extractfile(i).read()
				tree = etree.fromstring(xml)
				for warc in tree.xpath("//mets:file[@MIMETYPE='application/warc']", namespaces=NS):
					try:
						admid = warc.attrib["ADMID"]
						amdsec = tree.xpath("//mets:amdSec[@ID='%s']" % admid, namespaces=NS)[0]
						oiv = amdsec.xpath("mets:digiprovMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectIdentifier/premis:objectIdentifierValue", namespaces=NS)[0]
						path = re.findall("^.+(/heritrix.+\.warc\.gz)\?.+$", warc.xpath("mets:FLocat", namespaces=NS)[0].attrib["%shref" % XLINK])[0]
						identifiers.append((path, oiv.text))
					except IndexError as i:
						logger.error("Problem parsing METS for SIP: %s" % sip)
	else:
		logger.warning("Could not find SIP: hdfs://%s" % tar)
	return identifiers

