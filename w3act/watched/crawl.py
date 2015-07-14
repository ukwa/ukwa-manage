"""
Additional site-specific crawl setup.
"""

import sys
import w3act
import logging
import requests
from lxml import html
from urlparse import urlparse, urljoin

logger = logging.getLogger("w3act")
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()


class CrawlConfiguration(object):
    def __init__(self):
        self.watched_targets = None

    def additional_seeds(self, seeds):
        new_seeds = []
        for seed in seeds:
            key = urlparse(seed).netloc.replace(".", "_")
            try:
                validator = getattr(self, key)
                new_seeds += validator(seeds)
            except AttributeError:
                return []
        return new_seeds

    def www_gov_uk(self, seeds):
        """Add all pages as seeds."""
        new_seeds = []
        try:
            for seed in seeds:
                r = requests.get(seed)
                h = html.fromstring(r.content)
                for span in h.xpath("//span[@class='page-numbers']/text()"):
                    last = int(span.split()[-1]) + 1
                    new_seeds += ["%s&page=%i" % (seed, i) for i in range(2, last)]
        except:
            logger.error("www_gov_uk: %s" % sys.exc_info()[0])
            return []
        return new_seeds

