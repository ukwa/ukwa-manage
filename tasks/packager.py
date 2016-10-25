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



class AggregateOutputs(luigi.Task):
    """
    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    stage = luigi.Parameter()
    outputs = luigi.ListParameter()

    def requires(self):
        return AssembleOutput(self.job, self.launch_id, self.stage)

    def output(self):
        return ptarget(self.job, self.launch_id, len(self.outputs))

    def run(self):
        # TODO Sort inputs by checkpoint timestamp and merge into versioned lists?
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(self.outputs, indent=4)))


class ProcessPackages(luigi.Task):
    """
    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()

    def requires(self):
        # Look for checkpoints to package:
        outputs = {}
        is_final = False
        for item in glob.glob("%s/%s/%s/crawl.log*" % (LOG_ROOT, self.job.name, self.launch_id)):
            logger.info("ITEM %s" % item)
            if item.endswith("/crawl.log"):
                key = "last"
                is_final = True
                outputs["last"] = item
            elif item.endswith(".lck"):
                pass
            else:
                outputs[item[-14:]] = item

        output_list = sorted(outputs.keys())
        logger.info("INPUT list: %s" % outputs)
        logger.info("INPUT list: %s" % outputs.keys())
        logger.info("INPUT list: %s" % output_list)
        full = list()
        for key in output_list:
            full.append(outputs[key])

        if is_final:
            state = 'final'
        else:
            state = output_list[-1:]

        return AggregateOutputs(self.job, self.launch_id, state, full)

    def output(self):
        return ptarget(self.job, self.launch_id, "final")

    def run(self):
        logger.info(self.launch_id)
        logger.info("OUTPUT: %s" % self.output().path)
        is_final = False
        for input in self.input():
            if input.path.endswith(".final"):
                key = "final"
                is_final = True

        if is_final:
            # only report success if...
            with self.output().open('w') as f:
                f.write('{}'.format(json.dumps(self.input(), indent=4)))
        else:
            yield CheckJobStopped(self.job, self.launch_id)


class ScanForPackages(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'output'
    date_interval = luigi.DateIntervalParameter(default=[datetime.date.today(), datetime.date.today()])

    def requires(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*" % H3().local_job_folder):
                job = Jobs[os.path.basename(job_item)]
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            yield ProcessOutputs(job, launch)



if __name__ == '__main__':
    luigi.run(['output.ScanForPackages', '--date-interval', '2016-10-23-2016-10-26'])  # , '--local-scheduler'])
# luigi.run(['crawl.job.StartJob', '--job', 'daily', '--local-scheduler'])



