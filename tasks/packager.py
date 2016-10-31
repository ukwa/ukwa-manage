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
from slackclient import SlackClient
from crawl.w3act.w3act import w3act
import crawl.h3.hapyx as hapyx
from crawl.w3act.job import W3actJob
from crawl.w3act.job import remove_action_files
from common import *
from crawl_job_tasks import CheckJobStopped
from assembler import AssembleOutput




class ScanForPackages(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'output'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])

    def requires(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*/*" % State().state_folder):
                job = Jobs[os.path.basename(job_item)]
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            logger.info("ScanForPackages - looking at %s %s" % (job, launch_item))
                            yield ProcessPackages(job, launch, launch_item)



if __name__ == '__main__':
    luigi.run(['output.ScanForPackages', '--date-interval', '2016-10-22-2016-10-26'])  # , '--local-scheduler'])
# luigi.run(['crawl.job.StartJob', '--job', 'daily', '--local-scheduler'])



