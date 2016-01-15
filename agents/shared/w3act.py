#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import sys
import json
import logging
import requests
import traceback

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)
logger = logging.getLogger("w3act.%s" % __name__)


class w3act():
    def __init__(self, url, email, password):
        self.url = url.rstrip("/")
        loginUrl = "%s/login" % self.url
        logger.info("Logging into %s as %s "% ( loginUrl, email ))
        response = requests.post(loginUrl, data={"email": email, "password": password})
        if not response.history:
            logger.error("Login failed!")
            sys.exit()
        self.cookie = response.history[0].headers["set-cookie"]
        self.headers = {
            "Cookie": self.cookie
        }

    def _get_json(self, url):
        js = None
        try:
            r = requests.get(url, headers=self.headers)
            js = json.loads(r.content)
        except:
            logger.warning(str(sys.exc_info()[0]))
            logger.warning(str(traceback.format_exc()))
        return js

    def get_ld_export(self, frequency):
        return self._get_json( "%s/api/crawl/feed/ld/%s" % (self.url, frequency))

    def get_by_export(self, frequency):
        return self._get_json( "%s/api/crawl/feed/by/%s" % (self.url, frequency))

