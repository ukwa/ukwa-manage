import logging
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import urllib
from urllib import quote_plus  # python 2
# from urllib.parse import quote_plus # python 3

logger = logging.getLogger(__name__)


class CdxIndex():
    '''
    This class is used to query our CDX server.
    It knows what we've got, and when, but not what is open access or not.
    '''

    def __init__(self, cdx_server='http://bigcdx:8080/data-heritrix'):
        self.cdx_server = cdx_server

    def _capture_dates_generator(self, url):
        '''
        A generator that pages through the CDX results.

        :param url:
        :return: yeilds each capturedate in turn
        '''
        # Paging, as we have a LOT of copies of some URLs:
        batch = 25000
        offset = 0
        next_batch = True
        while next_batch:
            try:
                # Get a batch:
                q = "type:urlquery url:" + quote_plus(url) + (" limit:%i offset:%i" % (batch, offset))
                cdx_query_url = "%s?q=%s" % (self.cdx_server, quote_plus(q))
                logger.info("Getting %s" % cdx_query_url)
                f = urllib.urlopen(cdx_query_url)
                content = f.read()
                f.close()
                # Grab the capture dates:
                dom = xml.dom.minidom.parseString(content)
                new_records = 0
                for de in dom.getElementsByTagName('capturedate'):
                    yield de.firstChild.nodeValue
                    new_records += 1
                # Done?
                if new_records == 0:
                    next_batch = False
                else:
                    # Next batch:
                    offset += batch
            except ExpatError as e:
                logger.warning("Exception on lookup: "  + str(e))
                next_batch = False

    def get_first_capture_date(self, url):
        '''
        Returns the earliest capture date
        :param url:
        :return:
        '''
        return self._capture_dates_generator(url).next()

    def get_capture_dates(self, url):
        '''
        Returns and array of all the hits for a url.

        :param url:
        :return: array of capturedate strings
        '''
        capture_dates = []
        for capture_date in self._capture_dates_generator(url):
            capture_dates.append(capture_date)

        return capture_dates
