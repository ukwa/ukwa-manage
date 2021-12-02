import logging
import requests

logger = logging.getLogger(__name__)

class CDX11():    
    def __init__(self, line):
        self.urlkey, self.timestamp, self.original, self.mimetype, self.statuscode, \
        self.digest, self.redirecturl, self.robotflags, self.length, self.offset, self.filename = line.split(' ')

    def __str__(self):
        return ' '.join([self.urlkey, self.timestamp, self.original, self.mimetype, self.statuscode, \
        self.digest, self.redirecturl, self.robotflags, self.length, self.offset, self.filename])
    
    @property
    def crawl_date(self):
        return datetime.strptime(self.timestamp, '%Y%m%d%H%M%S')
    
    def to_dict(self):
        return {
            'urlkey': self.urlkey,
            'timestamp': self.timestamp, 
            'crawl_date': self.crawl_date,
            'original': self.original,
            'mimetype': self.mimetype,
            'statuscode': self.statuscode,
            'digest': self.digest, 
            'redirecturl': self.redirecturl, 
            'robotflags': self.robotflags, 
            'length': int(self.length), 
            'offset': int(self.offset), 
            'filename': self.filename
        }

class CdxIndex():
    '''
    This class is used to query our CDX server.
    It knows what we've got, and when, but not what is open access or not.
    
    filter="!mimetype:warc/revisit"
    
    '''

    def __init__(self, cdx_server='http://cdx.api.wa.bl.uk/data-heritrix', filter=None, limit=1000000):
        self.cdx_server = cdx_server
        self.filter = filter
        self.limit = limit

    def query(self, url, limit=25, sort='reverse', filter=None):
        '''
        See https://nla.github.io/outbackcdx/api.html#operation/query 
        '''
        # Define the core parameters:
        params = { 'url' : url, 'limit': limit, 'sort': sort }
        if self.filter:
            params['filter'] = self.filter
        # Set up the request
        r = requests.get(self.cdx_server, params=params)
        if r.status_code == 200:
            for line in r.iter_lines(decode_unicode=True):
                cdx = CDX11(line)
                yield cdx
        elif r.status_code != 404:
            print("ERROR! %s" % r)


    def _capture_dates_generator(self, url, sort="reverse"):
        '''
        A generator that pages through the CDX results.

        :param url:
        :return: yeilds each capturedate in turn
        '''
        # Set up a large query and stream through it
        for c in self.query(url, limit=self.limit, sort=sort):
            yield c.timestamp

    def get_first_capture_date(self, url):
        '''
        Returns the earliest capture date
        :param url:
        :return: None if there is none!
        '''
        return next(self._capture_dates_generator(url, sort="default"), None)

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

