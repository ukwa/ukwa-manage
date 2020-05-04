import logging
import urllib.parse
from lib.store.webhdfs import WebHDFSStore
from warcio.archiveiterator import ArchiveIterator

logger = logging.getLogger(__name__)

def follow_redirects(cdxs, url, urls=None):
    if urls is None:
        urls = set()
    logger.info("Looking up: %s" % url)
    store = WebHDFSStore(webhdfs_url="http://hdfs.bapi.wa.bl.uk/")
    for result in cdxs.query(url):
        if result.original == url:
            with store.stream(result.filename, result.offset, result.length) as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type in ['response', 'revisit']:
                        #target = record.rec_headers.get_header('WARC-Target-URI')
                        loc = record.http_headers.get('Location', None)
                        sc = record.http_headers.get_statuscode()
                        logger.info("%s < %s %s" %(loc, sc, url))
                        if loc:
                            # Resolve server-relative redirects if necessary:
                            loc = urllib.parse.urljoin(url, loc)
                            # Add to the list, if we don't already have it:
                            if not loc in urls:
                                urls.add(loc)
                                # See if the URL leads to further redirects...
                                urls = follow_redirects(cdxs, loc, urls)
                        break
    return urls
    