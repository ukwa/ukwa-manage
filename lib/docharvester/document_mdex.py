'''

This class coordinates the automated extraction of metadata for documents that have been crawled for Watched Targets.

i.e. all the per-host post-crawl logic is here.

Created on 8 Feb 2016

@author: andy
'''

import re
import time
import json
import logging
import requests
from urllib.parse import urlparse
from lxml import html
from lib.surt import url_to_surt

logger = logging.getLogger('luigi-interface')


class DocumentMDEx(object):
    '''
    Given a Landing Page extract additional metadata.
    '''

    def __init__(self, targets, document, source, null_if_no_target_found=True):
        '''
        The connection to W3ACT and the Document to be enhanced.
        '''
        if not targets:
            raise Exception("The Targets passed to DocumentMDEx cannot by empty!")
        self.targets = targets
        self.doc = document
        self.source = source
        self.null_if_no_target_found = null_if_no_target_found

    def lp_wb_url(self):
        # FIXME Redirect due to timestamp goes through W3ACT! Going direct to live web for now:
        #wb_url = "%s/%s/%s" % ( systems().wayback, self.doc['wayback_timestamp'], self.doc['landing_page_url'])
        wb_url =  self.doc['landing_page_url']
        return wb_url

    def doc_wb_url(self):
        #wb_url = "%s/%s/%s" % ( os.environ.get('WAYBACK_PREFIX','http://openwayback:8080/wayback'), self.doc['wayback_timestamp'], self.doc['document_url'])
        wb_url =  self.doc['document_url']
        return wb_url

    def find_watched_target_for(self, url, source, publishers):
        '''
        Given a URL and an array of publisher strings, determine which Watched Target to associate them with.
        '''
        # Find the list of Targets where a seed matches the given URL
        tsurt = url_to_surt(url)
        matches = []
        for t in self.targets:
            if t['watched']:
                a_match = False
                for seed in t['seeds']:
                    if tsurt.startswith(url_to_surt(seed, host_only=True)):
                        a_match = True
                if a_match:
                    matches.append(t)

        # No matches:
        if len(matches) == 0:
            logger.error("No match found for url %s" % url)
            return None
        # raise Exception("No matching target for url "+url)
        # If one match, assume that is the right Target, unless this is a known multi-stream publisher:
        if len(matches) == 1 and "://www.gov.uk/" not in url:
            return int(matches[0]['id'])

        #
        # Else multiple matches, so need to disambiguate.
        #
        # Attempt to disambiguate based on source ONLY:
        if source is not None:
            for t in matches:
                for seed in t['seeds']:
                    #logger.info("Looking for source match '%s' against '%s' " % (source, seed))
                    if seed == source:
                        # return int(t['id'])
                        logger.info("Found match source+seed but this is not enough to disambiguate longer crawls.")
                        break
        # Then attempt to disambiguate based on publisher
        # FIXME Make this a bit more forgiving of punctation/minor differences
        title_matches = []
        for t in matches:
            for publisher in publishers:
                #logger.debug("Looking for publisher match '%s' in title '%s' " % (publisher, t['title']))
                if publisher and publisher.lower() in t['title'].lower():
                    logger.info("Found publisher match '%s' in title '%s' " % (publisher, t['title']))
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
            if( re.match("^https://.*\.gov\.uk/.*$", self.doc["document_url"]) ):
                logger.info("Matches gov.uk pattern.")
                self.mdex_gov_uk_publications()
            elif( self.doc["document_url"].startswith("http://www.ifs.org.uk/")):
                logger.info("Matches IFS pattern.")
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
        if self.targets and 'landing_page_url' in self.doc:
            logger.info("Looking for match for %s source %s and publishers '%s'" % (self.doc['landing_page_url'], self.source, self.doc.get('publishers',[])))
            self.doc['target_id'] = self.find_watched_target_for(self.doc['landing_page_url'], self.source, self.doc.get('publishers', []))
        
        # If there is no association, flag it to be dropped:
        if not self.doc.get('target_id', None) and self.null_if_no_target_found:
            logger.critical("Failed to associated document with any target: %s" % self.doc)
            self.doc['match_failed'] = True

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
        logger.info("Getting %s" % self.lp_wb_url())
        r = requests.get(self.lp_wb_url(), stream=True, verify=False)
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
        tries = 5
        success = False
        while tries > 0:
            r = requests.head(url=self.doc_wb_url(), allow_redirects=True)
            if 'up' in r.links:
                lpu = r.links['up']
                self.doc["landing_page_url"] = lpu['url']
                success = True
                break
            else:
                logger.info("Could not find 'up' relationship!")
                tries -= 1
                time.sleep(10)
        # Fail if we could not contact the API:
        if not success:
            self.doc['api_call_failed'] = "Could not find rel['up'] relationship."
            return
        # Grab data from the landing page:
        lp_url = urlparse(self.doc["landing_page_url"])
        if "www.gov.uk" == lp_url.hostname:
            # The JSON extractor does not make sense to use with organisation landing pages:
            if not lp_url.path.startswith('/government/organisations/'):
                api_json_url = lp_url._replace( path="/api/content%s" % lp_url.path)
                api_json_url = api_json_url.geturl()
                logger.debug("Downloading and parsing from API: %s" % api_json_url)
                r = requests.get(api_json_url)
                if r.status_code != 200:
                    logger.warning("Got status code %s for URL %s" % (r.status_code, api_json_url))
                    logger.warning("Response: %s" % r.content)
                    raise Exception("Could not download the URL from the Content API!")
                md = json.loads(r.content)
                self.doc['title'] = md['title']
                self.doc['publication_date'] = md['first_published_at']
                # Pick up the 'public updated' date instead, if present:
                if 'public_updated_at' in md:
                    self.doc['publication_date'] = md['public_updated_at']
                self.doc['publishers'] = []
                for org in md['links']['organisations']:
                    self.doc['publishers'].append(org['title'])

            # Grab the landing page URL as HTML:
            # TODO This could all be pulled out of the Content API, if it's stable enough.
            logger.debug("Downloading and parsing: %s" % self.doc['landing_page_url'])
            r = requests.get(self.lp_wb_url())
            if r.status_code != 200:
                logger.warning("Got status code %s for URL %s" % (r.status_code, self.lp_wb_url()))
                logger.warning("Response: %s" % r.content)
                raise Exception("Could not download the landing page!")
            h = html.fromstring(r.content)
            # Attempt to extract resourse-level metadata (overriding publication-level metadata):
            # Look through landing page for links, find metadata section corresponding to the document:
            matches = 0
            for a in h.xpath("//a"):
                #logger.info("Looking for %s in %s" % ( self.doc['filename'], a.attrib["href"]))
                # Match based on just the file name:
                if self.doc["filename"] in a.attrib["href"]:
                    matches += 1
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
            # This is allowed to be empty if we are deferring to the default, and majot problems should throw errors earlier.
            if matches == 0:
                raise Exception("Document HREF matching failed! Can't find %s" % self.doc)
            #if not 'title' in self.doc or self.doc['title']:
            #    raise Exception('Title extraction failed! Metadata extraction for this target should be reviewed.')
    
        
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
        

