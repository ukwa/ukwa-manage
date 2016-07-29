from __future__ import absolute_import

import os
import re
import json
import glob
import gzip
import time
import shutil
import subprocess
from dateutil.parser import parse
import hdfs
import zipfile
import logging

# Quieted down the hdfs code:
logging.getLogger('hdfs').setLevel(logging.WARNING)

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg
from crawl.celery import HERITRIX_ROOT
from crawl.celery import HERITRIX_HDFS_ROOT

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

"""
Verify that we have a complete crawl on HDFS ready for packaging.
"""


class CrawlJobOutput():
    def __init__(self, job_id, launch_id):
        """Takes the checkpoint info and sets up data needed to build the SIP."""
        self.hdfs =  hdfs.InsecureClient(cfg.get('hdfs','url'), user=cfg.get('hdfs','user'))
        # Set up paths:
        self.WARC_ROOT = "%s/output/warcs" % HERITRIX_HDFS_ROOT
        self.VIRAL_ROOT =  "%s/output/viral" % HERITRIX_HDFS_ROOT
        self.IMAGE_ROOT = "%s/output/images" % HERITRIX_HDFS_ROOT
        self.LOG_ROOT =  "%s/output/logs" % HERITRIX_HDFS_ROOT
        self.LOCAL_LOG_ROOT = "%s/output/logs" % HERITRIX_ROOT
        self.LOCAL_JOBS_ROOT = "%s/jobs" % HERITRIX_ROOT

        #
        self.job_id = job_id
        self.launch_id = launch_id
        self.job_launch_id = "%s/%s" % (job_id, launch_id)
        self.verify_job_launch_id()
        self.crawl_log = self.get_crawl_log()
        self.start_date = CrawlJobOutput.file_start_date([self.crawl_log])
        # Find the WARCs referenced from the crawl log:
        self.parse_crawl_log()
        # TODO Get sha512 and ARK identifiers for WARCs now, and store in launch folder and thus the zip?
        # Bundle logs and configuration data into a zip and upload it to HDFS
        self.upload_logs_as_zip()

    def get_crawl_log(self):
        # First, parse the crawl log(s) and determine the WARC file names:
        logger.info("Looking for crawl logs...")
        logfilepath = "%s/%s/crawl.log" % (self.LOCAL_LOG_ROOT, self.job_launch_id)
        if os.path.exists(logfilepath):
            logger.info("Found %s..." % os.path.basename(logfilepath))
            return logfilepath

    @staticmethod
    def file_start_date(logs):
        """Finds the earliest timestamp in a series of log files."""
        timestamps = []
        for log in logs:
            if log.endswith(".gz"):
                with gzip.open(log, "rb") as l:
                    fields = l.readline().split()
                    if len(fields) > 0:
                        timestamps.append(parse(fields[0]))
            else:
                with open(log, "rb") as l:
                    fields = l.readline().split()
                    if len(fields) > 0:
                        timestamps.append(parse(fields[0]))
        timestamps.sort()
        if len(timestamps) == 0:
            return None
        return timestamps[0].strftime("%Y-%m-%dT%H:%M:%SZ")

    def warc_file_path(self, warcfile):
        return "%s/%s/%s" % (self.WARC_ROOT, self.job_launch_id, warcfile)

    def viral_file_path(self, warcfile):
        return "%s/%s/%s" % (self.VIRAL_ROOT, self.job_launch_id, warcfile)

    def parse_crawl_log(self):
        # Get the WARC filenames
        warcfiles = set()
        with open(self.crawl_log, 'r') as f:
            for line in f:
                parts = re.split(" +", line, maxsplit=13)
                jmd = json.loads(parts[12])
                if 'warcFilename' in jmd:
                    warcfiles.add(jmd['warcFilename'])
                else:
                    logger.error("No WARC file entry found for line: %s" % line)
        # Search for the actual files and return absolute paths:
        self.warc_lookup = {}
        self.warcs = []
        self.viral = []
        for warcfile in warcfiles:
            if self._file_exists(self.warc_file_path(warcfile)):
                logger.info("Found WARC %s" % self.warc_file_path(warcfile))
                self.warc_lookup[warcfile] = self.warc_file_path(warcfile)
                self.warcs.append(self.warc_file_path(warcfile))
            elif self._file_exists(self.viral_file_path(warcfile)):
                logger.info("Found Viral WARC %s" % self.viral_file_path(warcfile))
                self.viral.append(self.viral_file_path(warcfile))
            else:
                raise Exception("Cannot file warc file %s" % warcfile)

    def _file_exists(self, hdfs_path):
        status = self.hdfs.status(hdfs_path, strict=False)
        logger.debug("Status for %s" % hdfs_path)
        if status and status['type'] == 'FILE' and status['length'] > 0:
            return True
        else:
            return False

    def upload_logs_as_zip(self):
        """Zips up all log/config. files and copies said archive to HDFS; finds the
        earliest timestamp in the logs."""
        # Set up paths
        zip_path = "%s/%s/%s.zip" % (self.LOCAL_LOG_ROOT, self.job_launch_id, os.path.basename(self.job_launch_id))
        hdfs_zip_path = "%s/%s/%s.zip" % (self.LOG_ROOT, self.job_launch_id, os.path.basename(self.job_launch_id))
        # Get the logs together:
        self.logs = []
        try:
            if os.path.exists(zip_path):
                logger.info("Deleting %s..." % zip_path)
                try:
                    os.remove(zip_path)
                except Exception as e:
                    logger.error("Cannot delete %s..." % zip_path)
                    raise Exception("Cannot delete %s..." % zip_path)
            logger.info("Zipping logs to %s" % zip_path)
            with zipfile.ZipFile(zip_path, 'w', allowZip64=True) as zipout:
                for crawl_log in glob.glob("%s/%s/crawl.log" % (self.LOCAL_LOG_ROOT, self.job_launch_id)):
                    logger.info("Found %s..." % os.path.basename(crawl_log))
                    zipout.write(crawl_log)

                for log in glob.glob("%s/%s/*-errors.log" % (self.LOCAL_LOG_ROOT, self.job_launch_id)):
                    logger.info("Found %s..." % os.path.basename(log))
                    zipout.write(log)

                for txt in glob.glob("%s/%s/*.txt" % (self.LOCAL_JOBS_ROOT, self.job_launch_id)):
                    logger.info("Found %s..." % os.path.basename(txt))
                    zipout.write(txt)

                for txt in glob.glob("%s/%s/*.json" % (self.LOCAL_JOBS_ROOT, self.job_launch_id)):
                    logger.info("Found %s..." % os.path.basename(txt))
                    zipout.write(txt)

                if os.path.exists("%s/%s/crawler-beans.cxml" % (self.LOCAL_JOBS_ROOT, self.job_launch_id)):
                    logger.info("Found config...")
                    zipout.write("%s/%s/crawler-beans.cxml" % (self.LOCAL_JOBS_ROOT, self.job_launch_id))
                else:
                    logger.error("Cannot find config.")
                    raise Exception("Cannot find config.")
        except Exception as e:
            logger.error("Problem building ZIP file: %s" % str(e))
            logger.exception(e)
            raise Exception("Problem building ZIP file: %s" % str(e))
        # Push to HDFS
        if self._file_exists(hdfs_zip_path):
            raise Exception('Log path %s already exists on HDFS' % hdfs_zip_path)
        logger.info("Copying %s to HDFS." % hdfs_zip_path)
        with open(zip_path,'r') as f:
            self.hdfs.write(data=f,hdfs_path=hdfs_zip_path,overwrite=False)
        logger.info("Copied %s to HDFS." % hdfs_zip_path)
        time.sleep(1)
        logger.info("Checking %s on HDFS." % hdfs_zip_path)
        status = self.hdfs.status(hdfs_zip_path, strict=False)
        logger.info(status)
        self.logs.append(hdfs_zip_path)

    def get_crawl_job_output(self):
        """Return a description of this crawl job output."""
        desc = {
            'job_id': self.job_id,
            'launch_id': self.launch_id,
            'job_launch_id': self.job_launch_id,
            'start_date': self.start_date,
            'warcs': self.warcs,
            'viral': self.viral,
            'logs': self.logs
        }
        return desc

    def verify_job_launch_id(self):
        """Verifies that a message is valid. i.e. it's similar to: 'frequent/20140207041736'"""
        r = re.compile("^[a-z]+/[0-9]+$")
        if not r.match(self.job_launch_id):
            raise Exception("Could not verify job_lanuch_id: %s" % self.job_launch_id)

    @staticmethod
    def assemble(job_id, launch_id):
        """
        Passed a job_id, ensures we have a complete job result.
        """
        cjo = CrawlJobOutput(job_id,launch_id)
        return cjo.get_crawl_job_output()
