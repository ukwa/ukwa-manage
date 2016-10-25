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
from common import *


def mark_job_as(job, launch_id, mark):
    record = jtarget(job, launch_id, mark)
    with record.open('w') as f:
        f.write('{} {}\n'.format(job.name, launch_id))


class StopJobExternalTask(luigi.ExternalTask):
    """
    This task is used to mark jobs as stopped, but this is not something that can be forced automatically, see StopJob.
    """
    task_namespace = 'pulse'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')


class CheckJobStopped(luigi.Task):
    """
    Checks if given job/launch is currently running. Will not force the crawl to stop.
    """
    task_namespace = 'pulse'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')

    def run(self):
        # Set up connection to H3:
        h = hapyx.HapyX("https://%s:%s" % (H3().host, H3().port), username=H3().username, password=H3().password)

        # Is that job running?
        status = h.status(self.job.name)
        if status != "":
            # Check the launch ID is not current:
            launch_id = h.get_launch_id(self.job.name)
            if launch_id == self.launch_id:
                # Declare that we are awaiting an external process to stop this job:
                yield StopJobExternalTask(self.job, self.launch_id)

        # Not running, so mark as stopped:
        with self.output().open('w') as f:
            f.write('{} {}\n'.format(self.job.name, self.launch_id))


class StopJob(luigi.Task):
    task_namespace = 'pulse'
    job = luigi.EnumParameter(enum=Jobs)
    date = luigi.DateParameter(default=datetime.date.today())

    def complete(self):
        # Set up connection to H3:
        h = hapyx.HapyX("https://%s:%s" % (H3().host, H3().port), username=H3().username, password=H3().password)

        status = h.status(self.job.name)
        if status == "":
            return True

        return False

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(Act().url, Act().username, Act().password)
        # Set up connection to H3:
        h = hapyx.HapyX("https://%s:%s" % (H3().host, H3().port), username=H3().username, password=H3().password)

        logger.info("I'm stopping %s" % (self.job.name))

        # Stop job if currently running:
        if self.job.name in h.list_jobs() and h.status(self.job.name) != "":
            """Stops a running job, cleans up the directory, initiates job assembly."""
            launch_id = h.get_launch_id(self.job.name)
            job = W3actJob.from_directory(w, "%s/%s" % (H3().local_job_folder, self.job.name), heritrix=h)
            job.stop()
            remove_action_files(self.job.name, HERITRIX_JOBS=H3().local_job_folder)

            # Record an output file that can be use as a Target by a different task:
            mark_job_as(job, launch_id, 'stopped')
        else:
            logger.warning("No {} job to be stopped!".format(self.job.name))


class StartJob(luigi.Task):
    task_namespace = 'pulse'
    job = luigi.EnumParameter(enum=Jobs)
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return StopJob(self.job)

    # Do no output anything, as we don't want anything to prevent restarts, or initiate downstream actions.
    #def output(self):
    #    return luigi.LocalTarget('state/jobs/{}.started.{}'.format(self.job.name, self.date.isoformat()))

    # Always allow re-starting:
    def complete(self):
        return False

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(Act().url, Act().username, Act().password)
        # Set up connection to H3:
        h = hapyx.HapyX("https://%s:%s" % (H3().host, H3().port), username=H3().username, password=H3().password)

        logger.info("Starting %s" % (self.job.name))
        targets = w.get_ld_export(self.job.name)
        logger.debug("Found %s Targets in date range." % len(targets))
        job = W3actJob(w, targets, self.job.name, heritrix=h, heritrix_job_dir=H3().local_job_folder)
        status = h.status(self.job.name)
        logger.info("Got current job status: %s" % status)

        logger.info("Starting job %s..." % job.name)
        job.start()
        launch_id = h.get_launch_id(self.job.name)

        logger.info("Launched job %s/%s with %s seeds." % (job.name, launch_id, len(job.seeds)))
        #with self.output().open('w') as f:
        #    f.write('{}\n'.format(launch_id))

        # Record an output file that can be use as a Target by a different task.:
        mark_job_as(job, launch_id, 'started')

        return


if __name__ == '__main__':
    luigi.run(['pulse.StartJob', '--job', 'daily'])#, '--local-scheduler'])

