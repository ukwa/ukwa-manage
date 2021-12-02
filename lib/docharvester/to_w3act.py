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

import os
import json
import time
import logging
from urllib.parse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom

from w3act.api.client import w3act
from lib.docharvester.document_mdex import DocumentMDEx
from lib.windex.cdx import CdxIndex

logger = logging.getLogger(__name__)

class DocToW3ACT():

    def __init__(self, cdx_server, targets_path, act_url, act_user, act_password):
        # Location of CDX to check:
        self.cdxserver_endpoint = cdx_server
        self.cdx = CdxIndex(cdx_server, filter="!mimetype:warc/revisit")
        # Load in targets:
        with open(targets_path) as fin:
            self.targets = json.load(fin)
        # Set up a W3ACT client:
        self.act = w3act(act_url, act_user, act_password)

    def update(self, doc):
        """
        Passed a document, POSTs it to W3ACT.

        Return an update, None if no update.
        """
        logger.debug("Document received: %s." % doc)
        # Check if content appears to be in Wayback:
        if self.document_available(doc['document_url'], doc['wayback_timestamp']):
            # Lookup Target and extract any additional metadata:
            doc = DocumentMDEx(self.targets, doc, doc['source']).mdex()
            # Documents may be rejected at this point:
            if 'match_failed' in doc:
                doc['status'] = 'REJECTED'
                logger.error(f"The document has been REJECTED! : {doc}")
                return doc
            else:
                doc['status'] = 'ACCEPTED'
                logger.info(f"Sending doc to W3ACT: {doc}")
                # Inform W3ACT it's available:
                r = self.act.post_document(doc)
                if (r.status_code == 200):
                    logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
                    return doc
                else:
                    logger.error("POST to W3ACT failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                    logger.error("Assuming POST to W3ACT failure is transient, will retry later.")
                    # Returning None rather that the doc, so no update happens to the documents_found
                    return None
        else:
            logger.info("Not yet available in wayback: %s" % doc['document_url'])
            return None

    def document_available(self, url, ts):
        try:
            # Check if the item+timestamp is known:
            known = self.check_if_known(url,ts)
            logger.debug(f"TS/URL {ts}/{url} is known = {known}")
            return known
        except Exception as e:
            logger.error("%s [%s %s]" % (str(e), url, ts))
            logger.exception(e)
        # Otherwise:
        return False

    def check_if_known(self, url, ts):
        """
        Checks if a resource with a particular timestamp is available in the index:
        :return:
        """
        for capturedate in self.cdx.get_capture_dates(url):
            if ts == capturedate:
                return True

        # Otherwise, not found:
        return False

    def check_if_available(self, url, ts):
        """
        Checks if the resource is actually accessible/downloadable.
        
        e.g. export WAYBACK_URL_PREFIX=http://prod1.n45.wa.bl.uk:7070/archive

        This is done separately, as using this alone may accidentally get an older version.
        :return:
        """
        wburl = '%s/%sid_/%s' % (self.wayback_prefix, ts, url)
        logger.debug("Checking download %s" % wburl)
        r = requests.head(wburl)
        logger.debug("Download HEAD response: %d" % r.status_code)
        # Resource is present?
        if r.status_code == 200:
            return True
        else:
            return False