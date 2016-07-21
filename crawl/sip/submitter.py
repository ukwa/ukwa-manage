from __future__ import absolute_import

import os
import bagit
import tarfile
import hdfs
import shutil

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg

# Set up drop/watched folder configuration
DLS_DROP=cfg.get('dls','drop_folder')
DLS_WATCH=cfg.get('dls','watch_folder')

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


#
class SubmitSip():
    def __init__(self, job_id, launch_id, sip_tgz):
        self.hdfs =  hdfs.InsecureClient(cfg.get('hdfs','url'), user=cfg.get('hdfs','user'))
        self.submit_sip(launch_id,sip_tgz)

    def submit_sip(self, job_id, sip_tgz):
        """
        Download, unpack, check and submit the specified SIP tar.gz file (from HDFS)

        :param sip_path_tgz:
        :return:
        """

        # Set up target folder names:
        dls_drop_bag = "%s/%s" % (DLS_DROP, os.path.basename(job_id))
        dls_drop_tgz = "%s/%s.tar.gz" % (DLS_DROP, os.path.basename(job_id))
        dls_watch_bag = "%s/%s" % (DLS_WATCH, os.path.basename(job_id))

        # Download and unpack the BagIt bag into the DLS_DROP folder
        logger.debug("Downloading %s to %s." % (sip_tgz, DLS_DROP))
        client = self.hdfs.download(sip_tgz,dls_drop_tgz)
        # Check if there's already an unpacked version there:
        if os.path.exists(dls_drop_bag):
            raise Exception("Target DLS Drop folder already exists! %s" % dls_drop_bag)
        # Unpack:
        logger.debug("Unpacking %s..." % (dls_drop_tgz))
        tar = tarfile.open(dls_drop_tgz, "r:gz")
        tar.extractall(DLS_DROP)
        tar.close()

        # Validate bag and submit if all is well:
        logger.debug("Checking bag %s..." % (dls_drop_bag))
        bag = bagit.Bag(dls_drop_bag)
        if bag.validate():
            # Check if there is already something in the target location:
            if os.path.exists(dls_watch_bag):
                raise Exception("Target DLS Watch folder already exists! %s" % dls_watch_bag)
            # Move
            logger.info("Moving %s to %s." % (dls_drop_bag, dls_watch_bag))
            shutil.move(dls_drop_bag, dls_watch_bag)
        else:
            raise Exception("Invalid Bag-It bag: %s" % dls_drop_bag)

        # Clean up:
        os.remove(dls_drop_tgz)
