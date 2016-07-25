#!/usr/bin/env python

import json
import w3act
import logging
import requests
from surt import surt

EXPORT = "https://www.webarchive.org.uk/act/targets/export/by/all"
LDHOSTS = "/wayback/ldhosts.txt"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)
log = logging.getLogger("wayback-beta")
log.setLevel(logging.DEBUG)

w = w3act.ACT()
by = w.get_by_export("all")
surts = ["http://(%s" % surt(u["url"]) for t in by for u in t["fieldUrls"]]
with open(LDHOSTS, "wb") as o:
    o.write("\n".join(surts))

