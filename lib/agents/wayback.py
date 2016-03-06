#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import logging
import requests
import xml.dom.minidom

logger = logging.getLogger( __name__)


def document_available(wb_url, url, ts, exact_match=False):
	"""
	
	Queries Wayback to see if the content is there yet.
	
	e.g.
	http://192.168.99.100:8080/wayback/xmlquery.jsp?type=urlquery&url=https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
	
	<wayback>
	<request>
		<startdate>19960101000000</startdate>
		<resultstype>resultstypecapture</resultstype>
		<type>urlquery</type>
		<enddate>20160204115837</enddate>
		<firstreturned>0</firstreturned>
		<url>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</url>
		<resultsrequested>10000</resultsrequested>
		<resultstype>resultstypecapture</resultstype>
	</request>
	<results>
		<result>
			<compressedoffset>2563</compressedoffset>
			<mimetype>application/pdf</mimetype>
			<redirecturl>-</redirecturl>
			<file>BL-20160204113809800-00000-33~d39c9051c787~8443.warc.gz
</file>
			<urlkey>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</urlkey>
			<digest>JK2AKXS4YFVNOTPS7Q6H2Q42WQ3PNXZK</digest>
			<httpresponsecode>200</httpresponsecode>
			<robotflags>-</robotflags>
			<url>https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</url>
			<capturedate>20160204113813</capturedate>
		</result>
	</results>
</wayback>
	
	"""
	try:
		wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (wb_url, url)
		logger.debug("Checking %s" % wburl);
		r = requests.get(wburl)
		logger.debug("Response: %d" % r.status_code)
		# Is it known, with a matching timestamp?
		if r.status_code == 200:
			dom = xml.dom.minidom.parseString(r.text)
			for de in dom.getElementsByTagName('capturedate'):
				if de.firstChild.nodeValue == ts:
					# Excellent, it's been found:
					return True
	except Exception as e:
		logger.error( "%s [%s %s]" % ( str( e ), url, ts ) )
		logging.exception(e)
	# Otherwise:
	return False
