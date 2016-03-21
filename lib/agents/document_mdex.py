'''

This class coordinates the automated extraction of metadata for documents that have been crawled for Watched Targets.

i.e. all the per-host post-crawl logic is here.

Created on 8 Feb 2016

@author: andy
'''

import json
import requests
import logging
from urlparse import urljoin
from lxml import html

logger = logging.getLogger(__name__)

class DocumentMDEx(object):
    '''
    Given a Landing Page extract additional metadata.
    '''

    def __init__(self, act, document, source):
        '''
        The connection to W3ACT and the Document to be enhanced.
        '''
        self.act = act
        self.doc = document
        self.source = source

    def mdex(self):
        '''
        Metadata extraction and target association.
        '''
        # Pass the document through a different extractor based on how the URL starts.
        try:
            if( self.doc["landing_page_url"].startswith("https://www.gov.uk/government/")):
                self.mdex_gov_uk_publications()
            elif( self.doc["landing_page_url"].startswith("http://www.ifs.org.uk/publications/")):
                self.mdex_ifs_reports()
        except Exception as e:
            logger.error("Ignoring error during extraction for document %s and landing page %s" % (self.doc['document_url'], self.doc['landing_page_url']))
            logging.exception(e)

        # Look up which Target this URL should be associated with:
        if self.act:
            logger.info("Looking for match for %s source %s and publishers '%s'" % (self.doc['landing_page_url'], self.source, self.doc.get('publishers',[])))
            self.doc['target_id'] = self.act.find_watched_target_for(self.doc['landing_page_url'], self.source, self.doc.get('publishers', []))
        
        # If there is no association, drop it:
        if not self.doc.get('target_id', None):
            logger.critical("Failed to associated document with any target: %s" % self.doc)
            return None

        # If there is no title, use a default:
        if not self.doc.get('title',None):
            self.doc['title'] = '[untitled]'
            
        # If the publisher appears unambiguous, store it where it can be re-used
        if len(self.doc['publishers']) is 1:
            self.doc['publisher'] = self.doc['publishers'][0]
            
        # Or return the modified version:
        return self.doc
    
    def _get0(self, result):
        if len(result) > 0:
            return result[0].strip()
        else:
            return ""
    
    def mdex_gov_uk_publications(self):
        # Start by grabbing the Link-rel-up header to refine the landing page url:
        # e.g. https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
        # Link: <https://www.gov.uk/government/statistics/reported-road-casualties-in-great-britain-estimates-involving-illegal-alcohol-levels-2014>; rel="up"
        r = requests.head(url=self.doc['document_url'])
        if r.links.has_key('up'):
            lpu = r.links['up']
            self.doc["landing_page_url"] = lpu['url']
        # Grab the landing page URL as HTML
        logger.debug("Downloading and parsing: %s" % self.doc['landing_page_url'])
        r = requests.get(self.doc["landing_page_url"])
        h = html.fromstring(r.content)
        # Extract the metadata:
        logger.debug('xpath/title %s' % h.xpath('//header//h1/text()') )
        self.doc['title'] = self._get0(h.xpath('//header//h1/text()'))
        self.doc['publication_date'] = self._get0(h.xpath("//aside[contains(@class, 'meta')]//time/@datetime"))[0:10]
        self.doc['publishers'] = h.xpath("//aside[contains(@class, 'meta')]//a[contains(@class, 'organisation-link')]/text()")
        # Look through landing page for links, find metadata section corresponding to the document:
        for a in h.xpath("//a"):
            if self.doc["document_url"] in urljoin(self.doc["landing_page_url"], a.attrib["href"]):
                if a.getparent().getparent().attrib["class"] == "attachment-details":
                    div = a.getparent().getparent()
                    # Process title, allowing document title metadata to override:
                    lp_title = self._get0(div.xpath("./h2[@class='title']/a/text()"))
                    if len(lp_title) > 0:
                        self.doc['title'] = lp_title
                    # Process references
                    refs = div.xpath("./p/span[@class='references']")
                    # We also need to look out for Command and Act papers and match them by modifying the publisher list
                    for ref in refs:
                        isbn = self._get0(ref.xpath("./span[@class='isbn']/text()"))
                        if len(isbn) > 0:
                            self.doc['isbn'] = isbn
                        if len(ref.xpath("./span[starts-with(text(), 'HC') or starts-with(text(), 'Cm') or starts-with(text(), 'CM')]")) > 0:
                            self.doc['publishers'] = ["Command and Act Papers"]
        if not self.doc['title']:
            raise Exception('Title extraction failed! Metadata extraction for this target should be reviewed.')
    
        
    def mdex_ifs_reports(self):
        # Grab the landing page URL as HTML
        r = requests.get(self.doc["landing_page_url"])
        h = html.fromstring(r.content)
        # Extract the metadata:
        self.doc['title'] = self._get0(h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'name')]/text()")).strip()
        self.doc['publication_date'] = self._get0(h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'datePublished')]/@content"))
        self.doc['authors'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'author')]/a/text()")
        self.doc['publishers'] = h.xpath("//footer//*[contains(@itemtype, 'http://schema.org/Organization')]//*[contains(@itemprop,'name')]/text()")
        self.doc['isbn'] = self._get0(h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//tr[td[1]/text()='ISBN:']/td[2]/text()")).strip()
        self.doc['doi'] = self._get0(h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//tr[td[1]/text()='DOI:']/td[2]/a[1]/text()"))
        

def run_doc_mdex_test(url,lpu,src):
    doc = {}
    doc['document_url'] = url
    doc['landing_page_url'] = lpu
    doc = DocumentMDEx(act, doc, src).mdex()
    print json.dumps(doc)

if __name__ == "__main__":
    '''
    A few test cases
    '''
    
    # Set up a logging handler:
    handler = logging.StreamHandler()
    #handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
    formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
    handler.setFormatter( formatter ) 
    
    # Attach to root logger
    logging.root.addHandler( handler )
    
    # Set default logging output for all modules.
    logging.root.setLevel( logging.INFO )

    # Hook to W3ACT
    import sys
    from w3act import w3act
    act = w3act(sys.argv[1],sys.argv[2],sys.argv[3])

    # the tests:
    
    # - ifs.org.uk
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/cemmap/wps/cwp721515.pdf',
                    'http://www.ifs.org.uk/publications/8080','http://www.ifs.org.uk')
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/publications/bns/BN179.pdf',
                    'http://www.ifs.org.uk/publications/8049','http://www.ifs.org.uk')
    # - gov.uk
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/507081/2904936_Bean_Review_Web_Accessible.pdf',
					'https://www.gov.uk/government/publications/independent-review-of-uk-economic-statistics-final-report',
					'https://www.gov.uk/publications')
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/246770/0121.pdf',
                    'https://www.gov.uk/government/publications/met-office-annual-report-and-accounts-2012-to-2013', 
                    'https://www.gov.uk/')
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497536/rtfo-year-8-report-2.pdf',
                    'https://www.gov.uk/government/statistics/biofuel-statistics-year-8-2015-to-2016-report-2', 'https://www.gov.uk/')
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/495227/harbour-closure-orders-consultation-summary-responses.pdf',
                    'https://www.gov.uk/government/consultations/harbour-closure-and-pilotage-function-removal-orders-draft-guidance', 'https://www.gov.uk/')
