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


def validate(target, doc):
    key = urlparse(doc["landing_page_url"]).netloc.replace(".", "_")
    try:
        validator = getattr(sys.modules[__name__], key)
        return validator(target, doc)
    except AttributeError:
        return True


def www_gov_uk(target, doc):
    """Dept. names appear in a <aside class="meta"/> tag."""
    try:
        r = requests.get(doc["landing_page_url"])
        h = html.fromstring(r.content)
        texts = [t for t in h.xpath("//aside[contains(@class, 'meta')]//a/text()")]
        return len([t for t in texts if t in target["title"]]) > 0
    except:
        logger.error("www_gov_uk: %s" % sys.exc_info()[0])
        return False

