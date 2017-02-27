
import os
import logging
import requests
from requests.utils import quote
import xml.dom.minidom
from pywb.warc.recordloader import ArcWarcRecordLoader
from pywb.utils.bufferedreaders import DecompressingBufferedReader

logger = logging.getLogger(__name__)

HDFS_ROOT = os.environ.get('HDFS_ROOT','')
CDX_SERVER = os.environ['CDX_SERVER']

# Should we skip duplicate records?
# It seems OWB cope with them.
skip_duplicates = False

session = requests.Session()

# - 20150914222034 http://www.financeminister.gov.au/                     text/html 200      ZMSA5TNJUKKRYAIM5PRUJLL24DV7QYOO - - 83848 117273 WEB-20150914222031256-00000-43190~heritrix.nla.gov.au~8443.warc.gz
# - 20151201225932 http://anjackson.net/projects/images/keeping-codes.png image/png 200 sha1:DDOWG5GHKEDGUCOCOXZCAPRUXPND7GOK - - -     593544 BL-20151201225813592-00001-37~157a2278f619~8443.warc.gz
# - 20151202001114 http://anjackson.net/robots.txt unknown 302 sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ http://anjackson.net/ - - 773 BL-20151202001107925-00001-41~157a2278f619~8443.warc.gz

# Example de-duplicated CDX line from pywb cdx-indexer:
#
# net,anjackson)/assets/js/ie8-responsive-file-warning.js 20151202230549 http://anjackson.net/assets/js/ie8-responsive-file-warning.js text/html 404 HJ66ECSQVYNX22SEAFF7QAF4AZYKN2BD - - 2755 2945445 BL-20151202230405810-00000-38~101e6c786d7f~8443.warc.gz
# net,anjackson)/assets/js/ie8-responsive-file-warning.js 20151202230632 http://anjackson.net/assets/js/ie8-responsive-file-warning.js warc/revisit - HJ66ECSQVYNX22SEAFF7QAF4AZYKN2BD - - 548 3604638 BL-20151202230405810-00000-38~101e6c786d7f~8443.warc.gz


# [2015-12-02 22:35:45,851] ERROR: Failed with 400 Bad Request
# java.lang.NumberFormatException: For input string: "None"
# At line: - 20151202223545 dns:447119634 text/dns 1001 None - - - None None

def  send_uri_to_tinycdxserver(cdxserver_url, cl):
    """Passed a crawl log entry, it turns it into a CDX line and posts it to the index."""
    if not cdxserver_url:
        cdxserver_url = CDX_SERVER
    # Compose suitable record:
    logger.debug("To post: %s." % cl)
    url = cl["url"]
    # Skip non http(s) records (?)
    if (not url[:4] == "http"):
        return
    redirect = "-"
    status_code = cl["status_code"]
    # Don't index negative status codes here:
    if (status_code <= 0):
        logger.info("Ignoring <=0 status_code log entry for: %s" % url)
        return
    # Record redirects:
    if (status_code / 100 == 3 and ("redirecturl" in cl)):
        redirect = cl["redirecturl"]
    # Don't index revisit records as OW can't handle them (?)
    mimetype = cl["mimetype"]
    if "duplicate:digest" in cl["annotations"]:
        if skip_duplicates:
            logger.info("Skipping de-duplicated resource for: %s" % url)
            return
        else:
            mimetype = "warc/revisit"
            status_code = "-"
    # Catch edge case where line looks okay but no WARC set!
    if cl["warc_filename"] is None:
        logger.critical("Dropping message because WARC filename is unset! Message: %s" % cl)
        return
    # Make up the full path:
    warc_path = "%s/output/warcs/%s/%s/%s" % (HDFS_ROOT, cl['job_name'], cl['launch_id'], cl['warc_filename'])
    # Build CDX line:
    cdx_11 = "- %s %s %s %s %s %s - - %s %s\n" % (
        cl["start_time_plus_duration"][:14],
        url,
        mimetype,
        status_code,
        cl["content_digest"],
        redirect,
        cl["warc_offset"],
        warc_path
    )
    logger.debug("CDX: %s" % cdx_11)
    r = session.post(cdxserver_url, data=cdx_11.encode('utf-8'))
    #  headers={'Content-type': 'text/plain; charset=utf-8'})
    if (r.status_code == 200):
        logger.info("POSTed to cdxserver: %s" % url)
        return
    else:
        logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
        logger.error("Failed submission was: %s" % cdx_11.encode('utf-8'))
        logger.error("Failed source message was: %s" % cl)
        raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))



def get_rendered_original(url, type='screenshot'):
    """
    Grabs a rendered resource.

    Only reason Wayback can't do this is that it does not like the extended URIs
    i.e. 'screenshot:http://' and replaces them with 'http://screenshot:http://'
    """
    # Query URL
    qurl = "%s:%s" % (type, url)
    # Query CDX Server for the item
    #logger.info("Querying CDX for prefix...")
    warc_filename, warc_offset, compressedendoffset = lookup_in_cdx(qurl)

    # If not found, say so:
    if warc_filename is None:
        return None

    # Grab the payload from the WARC and return it.
    WEBHDFS_PREFIX = os.environ['WEBHDFS_PREFIX']
    WEBHDFS_USER = os.environ['WEBHDFS_USER']
    url = "%s%s?op=OPEN&user.name=%s&offset=%s" % (WEBHDFS_PREFIX, warc_filename, WEBHDFS_USER, warc_offset)
    if compressedendoffset:
        url = "%s&length=%s" % (url, compressedendoffset)
    #logger.info("Requesting copy from HDFS: %s " % url)
    r = requests.get(url, stream=True)
    #logger.info("Loading from: %s" % r.url)
    r.raw.decode_content = False
    rl = ArcWarcRecordLoader()
    #logger.info("Passing response to parser...")
    record = rl.parse_record_stream(DecompressingBufferedReader(stream=r.raw))
    #logger.info("RESULT:")
    #logger.info(record)

    #logger.info("Returning stream...")
    return (record.stream, record.content_type)

    #return "Test %s@%s" % (warc_filename, warc_offset)


def lookup_in_cdx(qurl):
    """
    Checks if a resource is in the CDX index.
    :return:
    """
    query = "%s?q=type:urlquery+url:%s" % (CDX_SERVER, quote(qurl))
    r = requests.get(query)
    print(r.url)
    logger.debug("Availability response: %d" % r.status_code)
    print(r.status_code, r.text)
    # Is it known, with a matching timestamp?
    if r.status_code == 200:
        try:
            dom = xml.dom.minidom.parseString(r.text)
            for result in dom.getElementsByTagName('result'):
                file = result.getElementsByTagName('file')[0].firstChild.nodeValue
                compressedoffset = result.getElementsByTagName('compressedoffset')[0].firstChild.nodeValue
                # Support compressed record length if present:
                if( len(result.getElementsByTagName('compressedendoffset')) > 0):
                    compressedendoffset = result.getElementsByTagName('compressedendoffset')[0].firstChild.nodeValue
                else:
                    compressedendoffset = None
                return file, compressedoffset, compressedendoffset
        except Exception as e:
            logger.error("Lookup failed for %s!" % qurl)
            logger.exception(e)
        #for de in dom.getElementsByTagName('capturedate'):
        #    if de.firstChild.nodeValue == self.ts:
        #        # Excellent, it's been found:
        #        return
    return None, None, None