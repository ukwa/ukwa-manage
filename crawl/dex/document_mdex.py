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

    def __init__(self, act, document, source, null_if_no_target_found=True):
        '''
        The connection to W3ACT and the Document to be enhanced.
        '''
        self.act = act
        self.doc = document
        self.source = source
        self.null_if_no_target_found = null_if_no_target_found

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
            else:
                self.mdex_default()
        except Exception as e:
            logger.error("Ignoring error during extraction for document %s and landing page %s" % (self.doc['document_url'], self.doc['landing_page_url']))
            logging.exception(e)

        # Look up which Target this URL should be associated with:
        if self.act and self.doc.has_key('landing_page_url'):
            logger.info("Looking for match for %s source %s and publishers '%s'" % (self.doc['landing_page_url'], self.source, self.doc.get('publishers',[])))
            self.doc['target_id'] = self.act.find_watched_target_for(self.doc['landing_page_url'], self.source, self.doc.get('publishers', []))
        
        # If there is no association, drop it:
        if not self.doc.get('target_id', None) and self.null_if_no_target_found:
            logger.critical("Failed to associated document with any target: %s" % self.doc)
            return None

        # If the publisher appears unambiguous, store it where it can be re-used
        if len(self.doc.get('publishers',[])) is 1:
            self.doc['publisher'] = self.doc['publishers'][0]
            
        # Or return the modified version:
        return self.doc
    
    def _get0(self, result):
        if len(result) > 0:
            return result[0].strip()
        else:
            return ""

    def mdex_default(self):
        ''' Default extractor uses landing page for title etc.'''
        # Grab the landing page URL as HTML
        r = requests.get(self.doc["landing_page_url"])
        h = html.fromstring(r.content)
        # Extract a title from the first header, or failing that, the page title:
        self.doc['title'] = self._get0(h.xpath("//h1/text()")).strip()
        if not self.doc['title']:
            self.doc['title'] = self._get0(h.xpath("//title/text()")).strip()
    
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
        # Do not try to extract metadata if we got there from the feed:
        if '/publications/feed/' in self.doc["landing_page_url"]:
                self.mdex_default()
                return
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
        if not 'www.ifs.org.uk' in self.doc['document_url']:
            logger.critical("Dropping off-site publication discovered on the IFS site. %s " % self.doc['document_url'])
            self.doc = dict()
        

