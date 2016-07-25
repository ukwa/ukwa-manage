
import json
import requests

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

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

def send_uri_to_tinycdxserver(cdxserver_url, cl):
    """Passed a crawl log entry, it turns it into a CDX line and posts it to the index."""
    logger.debug("Message received: %s." % cl)
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
        logger.critical("Dropping message because WARC filename is unset! Message: %s" % body)
        return
    # Build CDX line:
    cdx_11 = "- %s %s %s %s %s %s - - %s %s\n" % (
        cl["start_time_plus_duration"][:14],
        url,
        mimetype,
        status_code,
        cl["content_digest"],
        redirect,
        cl["warc_offset"],
        cl["warc_filename"]
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
