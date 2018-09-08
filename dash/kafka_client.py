import os
import json
import logging
from urlparse import urlsplit
import time
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from flask import Flask
from flask import render_template, redirect, url_for, flash, jsonify
from werkzeug.contrib.cache import FileSystemCache
from kafka import KafkaConsumer
from threading import Thread
from collections import OrderedDict, defaultdict

logger = logging.getLogger(__name__)


class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
    self.size_limit = kwds.pop("size_limit", None)
    OrderedDict.__init__(self, *args, **kwds)
    self._check_size_limit()

  def __setitem__(self, key, value):
    OrderedDict.__setitem__(self, key, value)
    self._check_size_limit()

  def _check_size_limit(self):
    if self.size_limit is not None:
      while len(self) > self.size_limit:
        self.popitem(last=False)


class Consumer(Thread):
    '''
    {
      "mimetype": "image/vnd.microsoft.icon",
      "content_length": 4150,
      "via": "http://dublincore.org/robots.txt",
      "warc_offset": 1128045,
      "thread": 2,
      "url": "http://dublincore.org/favicon.ico",
      "status_code": 200,
      "start_time_plus_duration": "20180907204730677+225",
      "host": "dublincore.org",
      "seed": "",
      "content_digest": "sha1:HYZOZGARLB7ATJSLRIGZN2UEORPIZXJH",
      "hop_path": "LLEPI",
      "warc_filename": "BL-NPLD-TEST-20180907204622099-00000-11601~h3w~8443.warc.gz",
      "crawl_name": "npld-fc",
      "size": 4415,
      "timestamp": "2018-09-07T20:47:30.913Z",
      "annotations": "ip:139.162.252.71",
      "extra_info": {
        "scopeDecision": "ACCEPT by rule #2 MatchesRegexDecideRule",
        "warcFileRecordLength": 3069
      }
    }
    '''

    def __init__(self, kafka_topic, kafka_brokers, group_id):
        Thread.__init__(self)
        # Set up a consumer:
        self.consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_brokers, group_id=group_id)
        # If requested, start at the start:
        self.consumer.poll()
        self.consumer.seek_to_beginning()
        # The last event timestamp we saw
        self.last_timestamp = None
        # Details of the most recent screenshots:
        self.screenshots = LimitedSizeDict(size_limit=25)
        # Information on the most recent hosts:
        self.hosts = LimitedSizeDict(size_limit=100)
        # This is used to hold the last 1000 messages, for tail analysis
        self.recent = LimitedSizeDict(size_limit=1000)

    def process_message(self, message):
        try:
            m = json.loads(message.value)

            # Record time event and latest timestamp
            url = m['url']
            self.recent[url] = m
            self.last_timestamp = m['timestamp']

            # Recent screenshots:
            if url.startswith('screenshot'):
                self.screenshots[url] = m

            # Host info:
            host = self.get_host(url)
            if host:
                hs = self.hosts.get(host, defaultdict(lambda: defaultdict(int)))
                self.hosts[host] = hs

                # Basics
                hs['stats']['total'] += 1
                hs['stats']['last_timestamp']  = m['timestamp']

                # Mime types:
                mimetype = m.get('mimetype', None)
                if not mimetype:
                    mimetype = m.get('content_type', 'unknown-content-type')
                hs['content_types'][mimetype] += 1

                # Status Codes:
                sc = str(m.get('status_code'))
                if not sc:
                    print(json.dumps(m, indent=2))
                    sc = "-"
                hs['status_codes'][sc] += 1

                # Via
                via_host = self.get_host(m.get('via', None))
                if via_host and host != via_host:
                    hs['via'][via_host] += 1

        except Exception as e:
            logger.exception("Ho gead", e)

    def get_host(self, url):
        if url is None:
            return None
        parts =  urlsplit(url)
        return parts[1]

    def get_status_codes(self):
        status_codes = defaultdict(int)
        for k in self.recent.keys():
            print(k)
            m = self.recent[k]
            sc = str(m.get('status_code'))
            if sc:
                status_codes[sc] += 1
        return status_codes

    def get_stats(self):
        return {
            'last_timestamp': self.last_timestamp,
            'status_codes': self.get_status_codes(),
            'screenshots': self.screenshots,
            'hosts': self.hosts
        }

    def run(self):
        for message in self.consumer:
            self.process_message(message)
