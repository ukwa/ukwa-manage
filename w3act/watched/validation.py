"""
Specific validation steps for Watched Targets.
"""

import logging
import requests
from lxml import html

logger = logging.getLogger("w3act")
logger.setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()

def www_gov_uk(target, doc):
    """The 'organisation-link' text specifies the Gov. Dept."""
    try:
        r = requests.get(doc["landing_page_url"])
        h = html.fromstring(r.content)
        dept = h.xpath("//a[@class='organisation-link']")[0].text
        return (dept in target["title"])
    except:
        logger.error("www_gov_uk: %s" % sys.exc_info()[0])
        return False

