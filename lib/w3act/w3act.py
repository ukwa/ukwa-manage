#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import sys
import json
import requests
import traceback
import time
import logging
import dateutil.parser

logger = logging.getLogger(__name__)


class CachedItem(object):
    def __init__(self, key, value, duration=60):
        self.key = key
        self.value = value
        self.duration = duration
        self.timeStamp = time.time()

    def __repr__(self):
        return '<CachedItem {%s:%s} expires at: %s>' % (self.key, self.value, self.timeStamp + self.duration)


class CachedDict(dict):
    def get(self, key, fn, duration):
        if key not in self \
                or self[key].timeStamp + self[key].duration < time.time():
            logger.info('adding new value for %s' % key)
            o = fn(key)
            self[key] = CachedItem(key, o, duration)
        else:
            logger.info('loading from cache for key %s' % key)

        return self[key].value


class w3act():
    def __init__(self, url, email, password):
        self.url = url.rstrip("/")
        loginUrl = "%s/login" % self.url
        logger.info("Logging into %s as %s " % (loginUrl, email))
        response = requests.post(loginUrl, data={"email": email, "password": password})
        if not response.history:
            logger.error("W3ACT Login failed!")
            raise Exception("W3ACT Login Failed!")
        self.cookie = response.history[0].headers["set-cookie"]
        self.get_headers = {
            "Cookie": self.cookie,
        }
        self.up_headers = {
            "Cookie": self.cookie,
            "Content-Type": "application/json"
        }
        self.ld_cache = CachedDict()

    def _get_json(self, url):
        js = None
        logger.info("Getting URL: %s" % url)
        r = requests.get(url, headers=self.get_headers)
        if r.status_code == 200:
            js = json.loads(r.content)
        else:
            logger.info("%i - %s" % (r.status_code, r.text))
        return js

    def get_json(self, path):
        path = path.lstrip("/")
        qurl = "%s/%s" % (self.url, path)
        logger.debug("Getting %s" % qurl)
        return self._get_json(qurl)

    def _get_ld_export(self, frequency):
        qurl = "%s/api/crawl/feed/ld/%s" % (self.url, frequency)
        logger.info("Getting %s" % qurl)
        return self._get_json(qurl)

    def get_ld_export(self, frequency):
        return self.ld_cache.get(frequency, self._get_ld_export, 60 * 60)

    def get_by_export(self, frequency):
        return self._get_json("%s/api/crawl/feed/by/%s" % (self.url, frequency))

    def get_oa_export(self, frequency):
        return self._get_json("%s/api/crawl/feed/oa/%s" % (self.url, frequency))

    def get_target_ids(self):
        return self._get_json("%s/api/targets/ids" % self.url)

    def get_targets(self, page=0, page_length=1000):
        return self._get_json("%s/api/targets?p=%i&l=%i" % (self.url, page, page_length))

    def get_ld_target_ids(self, frequency):
        return self._get_json("%s/api/crawl/ids/ld/%s" % (self.url, frequency))

    def get_ld_target_list(self, frequency):
        return self._get_json("%s/targets/export/ld/%s" % (self.url, frequency))

    def get_by_target_list(self, frequency):
        return self._get_json("%s/targets/export/by/%s" % (self.url, frequency))

    def get_subjects(self):
        return self._get_json("%s/api/subjects" % (self.url))

    def get_collection_tree(self):
        return self._get_json("%s/api/collections" % (self.url))

    def get_collection(self, cid):
        return self._get_json("%s/api/collections/%i" % (self.url, cid))

    def post_document(self, doc):
        ''' See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint '''
        r = requests.post("%s/documents" % self.url, headers=self.up_headers, data=json.dumps([doc]))
        return r

    def post_target(self, url, title):
        target = {}
        target['field_urls'] = [url]
        target['title'] = title
        target['selector'] = 1
        target['field_scope'] = "root"
        target['field_depth'] = "CAPPED"
        target['field_ignore_robots_txt'] = False
        logger.info("POST %s" % (json.dumps(target)))
        r = requests.post("%s/api/targets" % self.url, headers=self.up_headers, data=json.dumps(target))
        return r

    def get_target(self, tid):
        return self.get_json("/api/targets/%d" % tid)

    def update_target_schedule(self, tid, frequency, start_date, end_date=None):
        target = {}
        target['field_crawl_frequency'] = frequency.upper()
        sd = dateutil.parser.parse(start_date)
        target['field_crawl_start_date'] = int(time.mktime(sd.timetuple()))
        if end_date:
            ed = dateutil.parser.parse(start_date)
            target['field_crawl_end_date'] = int(time.mktime(ed.timetuple()))
        else:
            target['field_crawl_end_date'] = 0
        logger.info("PUT %d %s" % (tid, json.dumps(target)))
        r = requests.put("%s/api/targets/%d" % (self.url, tid), headers=self.up_headers, data=json.dumps(target))
        return r

    def update_target_selector(self, tid, uid):
        target = {}
        target['selector'] = uid
        logger.info("PUT %d %s" % (tid, json.dumps(target)))
        r = requests.put("%s/api/targets/%d" % (self.url, tid), headers=self.up_headers, data=json.dumps(target))
        return r

    def watch_target(self, tid):
        target = {}
        target['watchedTarget'] = {}
        target['watchedTarget']['documentUrlScheme'] = ""
        logger.info("PUT %d %s" % (tid, json.dumps(target)))
        r = requests.put("%s/api/targets/%d" % (self.url, tid), headers=self.up_headers, data=json.dumps(target))
        return r

    def unwatch_target(self, tid):
        target = {}
        target['watchedTarget'] = None
        logger.info("PUT %d %s" % (tid, json.dumps(target)))
        r = requests.put("%s/api/targets/%d" % (self.url, tid), headers=self.up_headers, data=json.dumps(target))
        return r
