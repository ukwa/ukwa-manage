'''

This class coordinates the automated extraction of metadata for documents that have been crawled for Watched Targets.

i.e. all the per-host post-crawl logic is here.

Created on 8 Feb 2016

@author: andy
'''

import json
import requests
import logging
from lxml import html

logger = logging.getLogger(__name__)

class DocumentMDEx(object):
    '''
    Given a Landing Page extract additional metadata.
    '''

    def __init__(self, act, document):
        '''
        The connection to W3ACT and the Document to be enhanced.
        '''
        self.act = act
        self.doc = document
        

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
            logger.info("Looking for match for %s and publisher '%s'" % (self.doc['landing_page_url'], self.doc.get('publisher',None)))
            self.doc['target_id'] = self.act.find_watched_target_for(self.doc['landing_page_url'], self.doc.get('publisher', None))
        
        # If there is no association, drop it:
        if not self.doc['target_id']:
            logger.critical("Failed to associated document with any target: %s" % self.doc['document_url'])
            return None

        # If there is no title, use a default:
        if not self.doc['title']:
            self.doc['title'] = '[untitled]'
            
        # Or return the modified version:
        return self.doc
    
    
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
        self.doc['title'] = h.xpath('//header//h1/text()')[0]
        self.doc['publication_date'] = h.xpath("//aside[contains(@class, 'meta')]//time/@datetime")[0][0:10]
        self.doc['publisher'] = h.xpath("//aside[contains(@class, 'meta')]//a[contains(@class, 'organisation-link')]/text()")[0]
        if not self.doc['title']:
            raise Exception('Title extraction failed! Metadata extraction for this target should be reviewed.')
    
        
    def mdex_ifs_reports(self):
        # Grab the landing page URL as HTML
        r = requests.get(self.doc["landing_page_url"])
        h = html.fromstring(r.content)
        # Extract the metadata:
        self.doc['title'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'name')]/text()")[0].strip()
        self.doc['publication_date'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'datePublished')]/@content")[0]
        self.doc['author'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//*[contains(@itemprop,'author')]/a/text()")
        self.doc['publisher'] = h.xpath("//footer//*[contains(@itemtype, 'http://schema.org/Organization')]//*[contains(@itemprop,'name')]/text()")[0]
        self.doc['isbn'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//tr[td[1]/text()='ISBN:']/td[2]/text()")
        if self.doc['isbn']:
            self.doc['isbn'] = self.doc['isbn'][0].strip()
        self.doc['doi'] = h.xpath("//*[contains(@itemtype, 'http://schema.org/CreativeWork')]//tr[td[1]/text()='DOI:']/td[2]/a[1]/text()")[0]
        

def run_doc_mdex_test(url,lpu):
    doc = {}
    doc['document_url'] = url
    doc['landing_page_url'] = lpu
    doc = DocumentMDEx(None, doc).mdex()
    print json.dumps(doc)

if __name__ == "__main__":
    '''
    A few test cases
    '''
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/cemmap/wps/cwp721515.pdf',
                    'http://www.ifs.org.uk/publications/8080')
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/publications/bns/BN179.pdf',
                    'http://www.ifs.org.uk/publications/8049')
    #
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/246770/0121.pdf',
                    'https://www.gov.uk/government/publications/met-office-annual-report-and-accounts-2012-to-2013')
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497536/rtfo-year-8-report-2.pdf',
                    'https://www.gov.uk/government/statistics/biofuel-statistics-year-8-2015-to-2016-report-2')
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/495227/harbour-closure-orders-consultation-summary-responses.pdf',
                    'https://www.gov.uk/government/consultations/harbour-closure-and-pilotage-function-removal-orders-draft-guidance')
