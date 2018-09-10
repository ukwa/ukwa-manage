import os
import json
import logging
import threading
from urlparse import urlsplit
import time
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from flask import Flask
from flask import render_template, redirect, url_for, flash, jsonify
from werkzeug.contrib.cache import FileSystemCache
from kafka import KafkaConsumer
from threading import Thread
from collections import OrderedDict, defaultdict, deque

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


class CrawlLogConsumer(Thread):
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

    def __init__(self, kafka_topic, kafka_brokers, group_id, from_beginning='false'):
        Thread.__init__(self)
        # The last event timestamp we saw
        self.last_timestamp = None
        # Details of the most recent screenshots:
        self.screenshots = deque(maxlen=100)
        self.screenshotsLock = threading.Lock()
        # This is used to hold the last 1000 messages, for tail analysis
        self.recent = deque(maxlen=10000)
        self.recentLock = threading.Lock()
        # Information on the most recent hosts:
        self.hosts = LimitedSizeDict(size_limit=100)
        # Set up a consumer:
        up = False
        while not up:
            try:
                self.consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_brokers, group_id=group_id)
                # If requested, start at the start:
                if from_beginning and from_beginning.lower() == 'true':
                    logger.info("Seeking to the beginning of %s" % kafka_topic)
                    self.consumer.poll(timeout_ms=5000)
                    self.consumer.seek_to_beginning()
                up = True
            except Exception as e:
                logger.exception("Failed to start CrawlLogConsumer!")
                time.sleep(5)

    def process_message(self, message):
        try:
            m = json.loads(message.value)

            # Record time event and latest timestamp
            url = m['url']
            with self.recentLock:
                self.recent.append(m)
            self.last_timestamp = m['timestamp']

            # Recent screenshots:
            if url.startswith('screenshot:'):
                with self.screenshotsLock:
                    original_url = url[11:]
                    logger.info("Found screenshot %s" % m)
                    self.screenshots.append((original_url, m['timestamp']))

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
            logger.exception("Could not process message %s" % message)

    def get_host(self, url):
        if url is None:
            return None
        parts =  urlsplit(url)
        return parts[1]

    def get_status_codes(self):
        status_codes = defaultdict(int)
        with self.recentLock:
            for m in self.recent:
                sc = str(m.get('status_code'))
                if sc:
                    status_codes[sc] += 10
        # Sort by count:
        status_codes = sorted(status_codes.items(), key=lambda x: x[1], reverse=True)
        return status_codes

    def get_stats(self):
        # Get screenshots sorted by timestamp
        with self.screenshotsLock:
            shots = list(self.screenshots)
            shots.sort(key=lambda shot: shot[1], reverse=True)
        return {
            'last_timestamp': self.last_timestamp,
            'status_codes': self.get_status_codes(),
            'screenshots': shots,
            'hosts': self.hosts
        }

    def run(self):
        for message in self.consumer:
            self.process_message(message)
