from __future__ import absolute_import

import os
import re
import sys
import json
import pika
import glob
import gzip
import bagit
import shutil
import logging
import argparse
import subprocess
from dateutil.parser import parse
import hdfs
from crawl.sip.creator import SipCreator

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


class CheckpointSipPreparer():
    def __init__(self, job_id, args):
        """Takes the checkpoint info and sets up data needed to build the SIP."""
        self.hdfs = hdfs.InsecureClient(host=args.webhdfs_host, port=args.webhdfs_port, user_name=args.webhdfs_user,
                                    timeout=100)
        self.WARC_ROOT = "/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/warcs"
        self.IMAGE_ROOT = "/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/viral"
        self.VIRAL_ROOT = "/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/images"
        self.LOG_ROOT = "/Users/andy/Documents/workspace/wren/compose-dev-crawler/output/logs"
        self.JOBS_ROOT = "/Users/andy/Documents/workspace/wren/compose-dev-crawler/jobs"
        # Need zip 3.0 and unzip 6.0:
        self.zip = "/usr/bin/zip -jg"
        self.unzip = "/usr/bin/unzip"
        self.verifyBinaries()
        self.verifyWebhdfs()
        #
        self.upload_to_hdfs = True
        #
        self.job_id = job_id
        self.sip_dir = "%s/%s" % (args.sip_root, job_id)
        if os.path.exists(self.sip_dir):
            if self.overwrite:
                logger.info("Removing SIP directory %s" % self.sip_dir)
                shutil.rmtree(self.sip_dir)
            else:
                raise Exception("Directory already exists: %s." % self.sip_dir)
        self.crawl_log = self.get_crawl_log()
        self.start_date = self.file_start_date([self.crawl_log])
        # This should bundle up the files into a ZIP and use that instead. i.e. as getLogs below but done right.
        self.get_logs_as_zip()
        # Find the WARCFilename to AboluteFilepath mapping:
        self.parse_crawl_log(self.crawl_log)

    def get_crawl_log(self):
        # First, parse the crawl log(s) and determine the WARC file names:
        logger.info("Looking for crawl logs...")
        logfilepath = "%s/%s/crawl.log" % (self.LOG_ROOT, self.job_id)
        if os.path.exists(logfilepath):
            logger.info("Found %s..." % os.path.basename(logfilepath))
            return logfilepath

    def file_start_date(self, logs):
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
        return timestamps[0].strftime("%Y-%m-%dT%H:%M:%SZ")

    def warc_file_path(self, warcfile):
        return "%s/%s/%s" % (self.WARC_ROOT, self.job_id, warcfile)

    def viral_file_path(self, warcfile):
        return "%s/%s/%s" % (self.VIRAL_ROOT, self.job_id, warcfile)

    def parse_crawl_log(self, crawl_log):
        # Get the WARC filenames
        warcfiles = set()
        with open(crawl_log, 'r') as f:
            for line in f:
                parts = re.split(" +", line, maxsplit=13)
                warcfiles.add(json.loads(parts[12])['warcFilename'])
        # Search for the actual files and return absolute paths:
        self.warc_lookup = {}
        self.warcs = []
        self.viral = []
        for warcfile in warcfiles:
            if os.path.exists(self.warc_file_path(warcfile)):
                logger.info("Found WARC %s" % self.warc_file_path(warcfile))
                self.warc_lookup[warcfile] = self.warc_file_path(warcfile)
                self.warcs.append(self.warc_file_path(warcfile))
            elif os.path.exists(self.viral_file_path(warcfile)):
                logger.info("Found Viral WARC %s" % self.viral_file_path(warcfile))
                self.viral.append(self.viral_file_path(warcfile))
            else:
                raise Exception("Cannot file warc file %s" % warcfile)

    def get_logs_as_zip(self):
        """Zips up all log/config. files and copies said archive to HDFS; finds the
        earliest timestamp in the logs."""
        self.logs = []
        try:
            zip_path = "%s/%s/%s-%s.zip" % (
            self.LOG_ROOT, self.jobname, os.path.basename(self.jobname), self.checkpoint)
            if os.path.exists(zip_path):
                logger.info("Deleting %s..." % zip_path)
                try:
                    os.remove(zip_path)
                except Exception as e:
                    logger.error("Cannot delete %s..." % zip_path)
                    raise Exception("Cannot delete %s..." % zip_path)
            logger.info("Zipping logs to %s" % zip_path)
            for crawl_log in glob.glob("%s/%s/crawl.log.%s" % (self.LOG_ROOT, self.jobname, self.checkpoint)):
                logger.info("Found %s..." % os.path.basename(crawl_log))
                logger.info(self.zip.split() + [zip_path, crawl_log])
                subprocess.check_output(self.zip.split() + [zip_path, crawl_log])

            for log in glob.glob("%s/%s/*-errors.log.%s" % (self.LOG_ROOT, self.jobname, self.checkpoint)):
                logger.info("Found %s..." % os.path.basename(log))
                subprocess.check_output(self.zip.split() + [zip_path, log])

            for txt in glob.glob("%s/%s/*.txt" % (self.JOBS_ROOT, self.jobname)):
                logger.info("Found %s..." % os.path.basename(txt))
                subprocess.check_output(self.zip.split() + [zip_path, txt])

            if os.path.exists("%s/%s/crawler-beans.cxml" % (self.JOBS_ROOT, self.jobname)):
                logger.info("Found config...")
                subprocess.check_output(
                    self.zip.split() + [zip_path, "%s/%s/crawler-beans.cxml" % (self.JOBS_ROOT, self.jobname)])
            else:
                logger.error("Cannot find config.")
                raise Exception("Cannot find config.")
        except Exception as e:
            logger.error("Problem building ZIP file: %s" % str(e))
            logger.exception(e)
            raise Exception("Problem building ZIP file: %s" % str(e))
        if self.upload_to_hdfs:
            if self.hdfs.exists_file_dir(zip_path):
                logger.info("Deleting hdfs://%s..." % zip_path)
                self.hdfs.delete_file_dir(zip_path)
            logger.info("Copying %s to HDFS." % os.path.basename(zip_path))
            with open(zip_path) as file_data:
                self.hdfs.create_file(zip_path, file_data, overwrite=self.overwrite)
        self.logs.append(zip_path)

    def verifyBinaries(self):
        """Verifies that the Zip binaries are present and executable. Need zip >= 3.0 unzip >= 6.0 other zip binaries that supports ZIP64."""
        logger.info("Verifying Zip binaries...")
        if not os.access(self.zip.split()[0], os.X_OK) or not os.access(self.unzip, os.X_OK):
            logger.error("Cannot find Zip binaries.")
            raise Exception("Cannot find Zip binaries.")
        return True

    def verifyWebhdfs(self):
        """Verifies that the WebHDFS services is available."""
        logger.info("Verifying WebHDFS service...")
        return self.hdfs.exists_file_dir("/")

    def create_sip(self, jobname, cpp):
        """Creates a SIP and returns the path to the folder containing the METS."""
        sip_dir = cpp.sip_dir
        if self.hdfs.exists_file_dir("%s.tar.gz" % sip_dir) and not self.overwrite:
            raise Exception("SIP already exists in HDFS: %s.tar.gz" % sip_dir)

        s = SipCreator([jobname], jobname, warcs=cpp.warcs, viral=cpp.viral, logs=cpp.logs,
                           start_date=cpp.start_date)
        if s.verifySetup():
            s.processJobs()
            s.createMets()
            if not os.path.exists(sip_dir):
                os.makedirs(sip_dir)
            with open("%s/%s-%s.xml" % (sip_dir, jobname, cpp.checkpoint), "wb") as o:
                s.writeMets(o)
            s.bagit(sip_dir)
        else:
            raise Exception("Could not verify SIP for %s" % sip_dir)
        return sip_dir

    def copy_sip_to_hdfs(self, sip_dir):
        """Creates a tarball of a SIP and copies to HDFS."""
        gztar = shutil.make_archive(base_name=sip_dir, format="gztar", root_dir=os.path.dirname(sip_dir),
                                    base_dir=os.path.basename(sip_dir))
        logger.info("Copying %s to HDFS..." % gztar)
        with open(gztar) as file_data:
            self.hdfs.create_file(gztar, file_data, overwrite=self.overwrite)
        logger.info("Done.")
        return gztar

    @staticmethod
    def verify_message(path_id):
        """Verifies that a message is valid. i.e. it's similar to: 'frequent/cp00001-20140207041736'"""
        r = re.compile("^[a-z]+/cp[0-9]+-[0-9]+$")
        return r.match(path_id)

    @staticmethod
    def package(job_id):
        """
        Passed a job_id, creates a SIP.
        """
        if CheckpointSipPreparer.verify_message(job_id):
            cpp = CheckpointSipPreparer(job_id)
            sip_dir = cpp.create_sip()
            logger.debug("Created SIP: %s" % sip_dir)
            # Create our Bagit.
            bag = bagit.Bag(sip_dir)
            if bag.validate():
                logger.debug("Copying %s to HDFS..." % job_id)
                gztar = cpp.copy_sip_to_hdfs(sip_dir)
                logger.debug("SIP tarball at hdfs://%s" % gztar)
                # Clean up temp files now were done (not required?)
                shutil.rmtree(sip_dir)
                os.remove(gztar)
                # And post on
                siptosub = {
                    'sip_id': job_id,
                    'gztar_path': gztar
                }
                return siptosub
            else:
                raise Exception("Invalid Bagit: %s" % sip_dir)
        else:
            raise Exception("Could not verify job_id: %s" % job_id)


