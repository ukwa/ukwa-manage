import os
import re
import enum
import json
import luigi
import datetime
import glob
import gzip
import time
from dateutil.parser import parse
import zipfile
import logging
from crawl.w3act.w3act import w3act
import crawl.h3.hapyx as hapyx
from crawl.w3act.job import W3actJob
from crawl.w3act.job import remove_action_files

class CrawlFeed(luigi.Task):
    task_namespace = 'w3act'
    frequency = luigi.Parameter()
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/crawl-feed.%s.%s' % (
        state().state_folder, datetime_string[0:7], datetime_string, self.frequency))

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(act().url, act().username, act().password)
        # Grab those targets:
        targets = w.get_ld_export(self.frequency)
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(targets, indent=4)))
