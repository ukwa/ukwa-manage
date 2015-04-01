"""
Specific validation steps for Watched Targets.
"""

import sys
import logging
import requests
from lxml import html
from urlparse import urlparse, urljoin

logger = logging.getLogger("w3act")
logger.setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()


def validate(seed, doc):
    key = urlparse(doc["landing_page_url"]).netloc.replace(".", "_")
    try:
        validator = getattr(sys.modules[__name__], key)
        return validator(target, doc)
    except AttributeError:
        return True


def www_gov_uk(seed, doc):
    """Dept. links appear in a <aside class="meta"/> tag."""
    try:
        r = requests.get(doc["landing_page_url"])
        h = html.fromstring(r.content)
        hrefs = [urljoin(doc["landing_page_url"], href) for href in h.xpath("//aside[contains(@class, 'meta')]//a/@href")]
        return seed in hrefs
    except:
        logger.error("www_gov_uk: %s" % sys.exc_info()[0])
        return False

