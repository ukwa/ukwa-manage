#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import json
import logging
import sys
import traceback

import requests
from crawl import settings

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)
logger = logging.getLogger("w3act.%s" % __name__)


class ACT():
    def __init__(self, email=settings.W3ACT_EMAIL, password=settings.W3ACT_PASSWORD):
        response = requests.post(settings.W3ACT_LOGIN, data={"email": email, "password": password})
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
        return self._get_json("%s/ld/%s" % (settings.W3ACT_EXPORT_BASE, frequency))

    def get_by_export(self, frequency):
        return self._get_json("%s/by/%s" % (settings.W3ACT_EXPORT_BASE, frequency))


    def get_oa_export(self, frequency):
        return self._get_json("%s/oa/%s" % (settings.W3ACT_EXPORT_BASE, frequency))

