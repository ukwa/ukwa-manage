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

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

#
class sipstodls(amqp.QueueConsumer):
    def send_submit_message(self, message):
        """Sends a message to the 'index' queue."""
        parameters = pika.URLParameters(self.ampq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=args.out_queue, durable=True)
        channel.tx_select()
        channel.basic_publish(exchange="",
                              routing_key=args.out_queue,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,
                              ),
                              body=message)
        channel.tx_commit()
        connection.close()

    def verify_message(self, path_id):
        """Verifies that a message is valid. i.e. it's similar to: 'frequent/cp00001-20140207041736'"""
        r = re.compile("^[a-z]+/cp[0-9]+-[0-9]+$")
        return r.match(path_id)

    def create_sip(self, jobname, cpp):
        """Creates a SIP and returns the path to the folder containing the METS."""
        sip_dir = cpp.sip_dir
        if self.hdfs.exists_file_dir("%s.tar.gz" % sip_dir) and not args.overwrite:
            raise Exception("SIP already exists in HDFS: %s.tar.gz" % sip_dir)

        s = sip.SipCreator([jobname], jobname, warcs=cpp.warcs, viral=cpp.viral, logs=cpp.logs,
                           start_date=cpp.start_date, args=args)
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
            self.hdfs.create_file(gztar, file_data, overwrite=args.overwrite)
        logger.info("Done.")
        return gztar

    def setup_hdfs(self, args):
        self.hdfs = PyWebHdfsClient(host=args.webhdfs_host, port=args.webhdfs_port, user_name=args.webhdfs_user,
                                    timeout=100)


