"""Watched the crawled documents log queue and passes entries to w3act

Input:

{
    "annotations": "ip:173.236.225.186,duplicate:digest",
    "content_digest": "sha1:44KA4PQA5TYRAXDIVJIAFD72RN55OQHJ",
    "content_length": 324,
    "extra_info": {},
    "hop_path": "IE",
    "host": "acid.matkelly.com",
    "jobName": "frequent",
    "mimetype": "text/html",
    "seed": "WTID:12321444",
    "size": 511,
    "start_time_plus_duration": "20160127211938966+230",
    "status_code": 404,
    "thread": 189,
    "timestamp": "2016-01-27T21:19:39.200Z",
    "url": "http://acid.matkelly.com/img.png",
    "via": "http://acid.matkelly.com/",
    "warc_filename": "BL-20160127211918391-00001-35~ce37d8d00c1f~8443.warc.gz",
    "warc_offset": 36748
}

Note that 'seed' is actually the source tag, and is set up to contain the original (Watched) Target ID.

Output:

[
{
"id_watched_target":<long>,
"wayback_timestamp":<String>,
"landing_page_url":<String>,
"document_url":<String>,
"filename":<String>,
"size":<long>
},
<further documents>
]

See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint

i.e.

seed -> id_watched_target
start_time_plus_duration -> wayback_timestamp
via -> landing_page_url
url -> document_url (and filename)
content_length -> size

Note that, if necessary, this process to refer to the
cdx-server and wayback to get more information about
the crawled data and improve the landing page and filename data.


"""

from __future__ import absolute_import

import os
import json
import time
from urlparse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom

from crawl.dex.document_mdex import DocumentMDEx

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)



def document_available(wayback_url, url, ts):
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
        wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (wayback_url, quote(url))
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
        logger.exception(e)
    # Otherwise:
    return False


def send_document_to_w3act(cl, wayback_url, act):
    """
    Passed a document crawl log entry, POSTs it to W3ACT.
    """
    logger.debug("Message received: %s." % cl)
    if cl.has_key('launch_message'):
        logger.warning("Discarding message delivered to wrong queue: %s" % cl)
        return
    url = cl["url"]
    # Skip non http(s) records (?)
    if (not url[:4] == "http"):
        logger.debug("Ignoring non-HTTP log entry: %s" % cl)
        return
    status_code = cl["status_code"]
    # Don't index negative or non 2xx status codes here:
    if (status_code / 100 != 2):
        logger.info("Ignoring <=0 status_code log entry: %s" % cl)
        return
    # grab source tag
    source = cl.get('source', None)
    # Build document info line:
    doc = {}
    doc['wayback_timestamp'] = cl['start_time_plus_duration'][:14]
    doc['landing_page_url'] = cl['via']
    doc['document_url'] = cl['url']
    doc['filename'] = os.path.basename(urlparse(cl['url']).path)
    doc['size'] = int(cl['content_length'])
    # Check if content appears to be in Wayback:
    if document_available(wayback_url, doc['document_url'], doc['wayback_timestamp']):
        # Lookup Target and extract any additional metadata:
        doc = DocumentMDEx(act, doc, source).mdex()
        # Documents may be rejected at this point:
        if doc == None:
            logger.critical("The document based on this message has been REJECTED! :: " + cl)
            return
        # Inform W3ACT it's available:
        logger.debug("Sending doc: %s" % doc)
        r = act.post_document(doc)
        if (r.status_code == 200):
            logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
            return
        else:
            logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
            raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
    else:
        logger.info("Not yet available in wayback: %s" % doc['document_url'])
        raise Exception("Not yet available in wayback: %s" % doc['document_url'])
