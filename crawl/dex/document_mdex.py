'''

This class coordinates the automated extraction of metadata for documents that have been crawled for Watched Targets.

i.e. all the per-host post-crawl logic is here.

Created on 8 Feb 2016

@author: andy
'''

import json
import requests
from urlparse import urljoin
from lxml import html
from crawl.h3.utils import url_to_surt
from tasks.common import logger, systems


class DocumentMDEx(object):
    '''
    Given a Landing Page extract additional metadata.
    '''

    def __init__(self, targets, document, source, null_if_no_target_found=True):
        '''
        The connection to W3ACT and the Document to be enhanced.
        '''
        self.targets = targets
        self.doc = document
        self.source = source
        self.null_if_no_target_found = null_if_no_target_found

    def lp_wb_url(self):
        wb_url = "%s/%s/%s" % ( systems().wayback, self.doc['wayback_timestamp'], self.doc['landing_page_url'])
        return wb_url

    def doc_wb_url(self):
        wb_url = "%s/%s/%s" % ( systems().wayback, self.doc['wayback_timestamp'], self.doc['document_url'])
        return wb_url

    def find_watched_target_for(self, url, source, publishers):
        '''
        Given a URL and an array of publisher strings, determine which Watched Target to associate them with.
        '''
        # Find the list of Targets where a seed matches the given URL
        surt = url_to_surt(url, host_only=True)
        matches = []
        for t in self.targets:
            if t['watched']:
                a_match = False
                for seed in t['seeds']:
                    if surt.startswith(url_to_surt(seed, host_only=True)):
                        a_match = True
                if a_match:
                    matches.append(t)

        # No matches:
        if len(matches) == 0:
            logger.error("No match found for url %s" % url)
            return None
        # raise Exception("No matching target for url "+url)
        # If one match, assume that is the right Target:
        if len(matches) == 1:
            return int(matches[0]['id'])
        #
        # Else multiple matches, so need to disambiguate.
        #
        # Attempt to disambiguate based on source ONLY:
        if source is not None:
            for t in matches:
                for seed in t['seeds']:
                    logger.info("Looking for source match '%s' against '%s' " % (source, seed))
                    if seed == source:
                        # return int(t['id'])
                        logger.info("Found match source+seed but this is not enough to disambiguate longer crawls.")
                        break
        # Then attempt to disambiguate based on publisher
        # FIXME Make this a bit more forgiving of punctation/minor differences
        title_matches = []
        for t in matches:
            for publisher in publishers:
                logger.info("Looking for publisher match '%s' in title '%s' " % (publisher, t['title']))
                if publisher and publisher.lower() in t['title'].lower():
                    title_matches.append(t)
                    break
        if len(title_matches) == 0:
            logger.warning("No matching title to associate with url %s " % url)
            return None
        # raise Exception("No matching title to associate with url %s " % url)
        elif len(title_matches) == 1:
            return int(title_matches[0]['id'])
        else:
            logger.warning("Too many matching titles for %s" % url)
            for t in title_matches:
                logger.warning("Candidate: %d %s " % (t['id'], t['title']))
            logger.warning("Assuming first match is sufficient... (%s)" % title_matches[0]['title'])
            return int(title_matches[0]['id'])


    def mdex(self):
        '''
        Metadata extraction and target association.
        '''
        # Pass the document through a different extractor based on how the URL starts.
        try:
            if( self.doc["document_url"].startswith("https://www.gov.uk/")):
                self.mdex_gov_uk_publications()
            elif( self.doc["document_url"].startswith("http://www.ifs.org.uk/")):
                self.mdex_ifs_reports()
            else:
                self.mdex_default()
        except Exception as e:
            logger.error("Ignoring error during extraction for document %s and landing page %s" % (self.doc['document_url'], self.doc['landing_page_url']))
            logger.exception(e)

        if not 'title' in self.doc or not self.doc['title']:
            logger.info("Falling back on default extraction logic...")
            self.mdex_default()
            logger.info("GOT %s" % self.doc)

        # Look up which Target this URL should be associated with:
        if self.targets and self.doc.has_key('landing_page_url'):
            logger.info("Looking for match for %s source %s and publishers '%s'" % (self.doc['landing_page_url'], self.source, self.doc.get('publishers',[])))
            self.doc['target_id'] = self.find_watched_target_for(self.doc['landing_page_url'], self.source, self.doc.get('publishers', []))
        
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
        r = requests.get(self.lp_wb_url())
        h = html.fromstring(r.content)
        h.make_links_absolute(self.doc["landing_page_url"])
        logger.info("Looking for links...")
        # Attempt to find the nearest prior header:
        for a in h.xpath("//a[@href]"):
            if self.doc["document_url"] in a.attrib["href"]:
                element = a
                # try a preceding match:
                for hel in a.xpath("./preceding::*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6][1]"):
                    # logger.info("EEE %s" % hel.text_content())
                    logger.info("Preceding header %s" % hel.text_content())
                    self.doc['title'] = hel.text_content().strip()
                    return
                # Try recursing up the tree (I think this is superceded by the preceding-match logic above).
                while element.getparent() is not None:
                    element = element.getparent()
                    #logger.info("ELEMENT %s " % element)
                    #logger.info("ELEMENT %s " % element.text_content())
                    for hel in element.xpath(".//*[self::h2 or self::h3 or self::h4 or self::h5]"):
                        logger.info("header %s" % hel.text_content())
                        self.doc['title'] = hel.text_content().strip()
                        return
                self.doc['title'] = a.text_content()
                return
        # Extract a title from the first header, or failing that, the page title:
        self.doc['title'] = self._get0(h.xpath("//h1/text()")).strip()
        if not self.doc['title']:
            self.doc['title'] = self._get0(h.xpath("//title/text()")).strip()

    def mdex_gov_uk_publications(self):
        # Start by grabbing the Link-rel-up header to refine the landing page url:
        # e.g. https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
        # Link: <https://www.gov.uk/government/statistics/reported-road-casualties-in-great-britain-estimates-involving-illegal-alcohol-levels-2014>; rel="up"
        r = requests.head(url=self.doc_wb_url())
        if r.links.has_key('up'):
            lpu = r.links['up']
            self.doc["landing_page_url"] = lpu['url']
        # Grab the landing page URL as HTML
        logger.debug("Downloading and parsing: %s" % self.doc['landing_page_url'])
        r = requests.get(self.lp_wb_url())
        h = html.fromstring(r.content)
        # Extract the metadata:
        logger.debug('xpath/title %s' % h.xpath('//header//h1/text()') )
        self.doc['title'] = self._get0(h.xpath('//header//h1/text()'))
        self.doc['publication_date'] = self._get0(h.xpath("//aside[contains(@class, 'meta')]//time/@datetime"))[0:10]
        if self.doc['publication_date'] == '':
            self.doc.pop('publication_date')
        self.doc['publishers'] = h.xpath("//aside[contains(@class, 'meta')]//a[contains(@class, 'organisation-link')]/text()")
        # Look through landing page for links, find metadata section corresponding to the document:
        for a in h.xpath("//a"):
            if self.doc["document_url"] in urljoin(self.doc["landing_page_url"], a.attrib["href"]):
                if ("class" in a.getparent().getparent().attrib) and \
                                a.getparent().getparent().attrib["class"] == "attachment-details":
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
        r = requests.get(self.lp_wb_url())
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
        

