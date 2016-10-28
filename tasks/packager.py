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


class AggregateOutputs(luigi.Task):
    """
    """
    task_namespace = 'package'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    state = luigi.Parameter()
    outputs = luigi.ListParameter()

    def requires(self):
        for stage in self.outputs:
            return AssembleOutput(self.job, self.launch_id, os.path.splitext(stage)[1].lstrip("."))

    def output(self):
        return ptarget(self.job, self.launch_id, "%s.%s" %(len(self.outputs), self.state))

    def run(self):
        # Sort inputs by checkpoint timestamp and merge into versioned lists:
        aggregate = {}
        if isinstance(self.input(), list):
            inputs = self.input()
        else:
            inputs = [ self.input() ]
        # Build up the aggregate:
        for input in inputs:
            logger.info("Reading %s" % input.path)
            item = json.load(input.open())
            for key in item.keys():
                current = aggregate.get(key,[])
                if isinstance(item[key],list):
                    current.extend(item[key])
                elif item[key]:
                    current = item[key]
                aggregate[key] = current

        logger.info("Aggregate: %s" % aggregate)

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(aggregate, indent=4)))


class ProcessPackages(luigi.Task):
    """
    This looks at the checkpoints and final crawl chunk and sorts them by date, so an aggregate package for
    each new chunk can be composed. i.e. if there are two checkpoints and a final chunk, we should get three
    incremental packages containing (cp1), (cp1 cp2), (cp1 cp2 final).
    """
    task_namespace = 'package'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    def requires(self):
        # Look for checkpoints to package, and package them in the correct order:
        outputs = {}
        is_final = False
        glob_path = otarget(self.job, self.launch_id, '*').path
        logger.info("glob path is: %s" % glob_path)
        for item in glob.glob(glob_path):
            logger.info("ITEM %s" % item)
            if item.endswith(".final"):
                is_final = True
                outputs["final"] = item
            elif item.endswith(".lck"):
                pass
            else:
                outputs[item[-14:]] = item

        output_list = sorted(outputs.keys())
        logger.info("Ordered by date: %s" % output_list)

        aggregate = list()
        for key in output_list:
            aggregate.append(outputs[key])
            yield AggregateOutputs(self.job, self.launch_id, key, aggregate)

    def output(self):
        return ptarget(self.job, self.launch_id, "list")

    def run(self):
        logger.info(self.launch_id)
        is_final = False
        outputs = []
        for input in self.input():
            if input.path.endswith(".final"):
                is_final = True
            outputs.append(input.path)

        # only report complete success if...
        if is_final:
            with self.output().open('w') as f:
                f.write('{}'.format(json.dumps(outputs, indent=4)))
        else:
            yield CheckJobStopped(self.job, self.launch_id)


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



