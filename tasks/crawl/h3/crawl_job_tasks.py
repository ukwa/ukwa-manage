from __future__ import absolute_import

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
from tasks.w3act.feeds import CrawlFeed

logger = logging.getLogger('luigi-interface')

LOCAL_JOB_FOLDER = '/heritrix/jobs'


def target_name(state_class, job, launch_id, status):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job, launch_id, state_class, job, launch_id, status)


def jtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(os.environ['LUIGI_STATE_FOLDER'], target_name('01.jobs', job, launch_id, status)))


def get_hapy_for_job(job, host, port=8443, username="admin", password=os.environ['HERITRIX_PASSWORD']):
    if host == 'localhost':
        url = "https://%s:%s" % (host, port)
    else:
        url = "https://%s-%s:%s" % (host, job, port)
    return hapyx.HapyX(url, username=username, password=password)


def mark_job_as(job, launch_id, mark):
    record = jtarget(job, launch_id, mark)
    with record.open('w') as f:
        f.write('{} {}\n'.format(job, launch_id))


class StopJobExternalTask(luigi.ExternalTask):
    """
    This task is used to mark jobs as stopped, but this is not something that can be forced automatically, see StopJob.
    """
    task_namespace = 'pulse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    host = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')


class CheckJobStopped(luigi.Task):
    """
    Checks if given job/launch is currently running. Will not force the crawl to stop.
    """
    task_namespace = 'pulse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    host = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')

    def run(self):
        # Set up connection to H3:
        h = get_hapy_for_job(self.job, self.host)

        # Is that job running?
        status = h.status(self.job)
        if status != "":
            # Check the launch ID is not current:
            launch_id = h.get_launch_id(self.job)
            if launch_id == self.launch_id:
                # Declare that we are awaiting an external process to stop this job:
                yield StopJobExternalTask(self.job, self.launch_id)

        # Not running, so mark as stopped:
        with self.output().open('w') as f:
            f.write('{} {}\n'.format(self.job, self.launch_id))


class StopJob(luigi.Task):
    task_namespace = 'pulse'
    job = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    host = luigi.Parameter()

    def complete(self):
        # Set up connection to H3:
        h = get_hapy_for_job(self.job, self.host)

        # Is this job known?
        if self.job in h.list_jobs():
            status = h.status(self.job)
            if status == "":
                return True
            else:
                return False
        else:
            return True

    def run(self):
        # Set up connection to H3:
        h = get_hapy_for_job(self.job, self.host)

        logger.info("I'm stopping %s" % (self.job))

        # Stop job if currently running:
        if self.job in h.list_jobs() and h.status(self.job) != "":
            """Stops a running job, cleans up the directory, initiates job assembly."""
            launch_id = h.get_launch_id(self.job)
            job = W3actJob.from_directory("%s/%s" % (LOCAL_JOB_FOLDER, self.job), heritrix=h)
            job.stop()
            remove_action_files(self.job, HERITRIX_JOBS=LOCAL_JOB_FOLDER)

            # Record an output file that can be use as a Target by a different task:
            mark_job_as(job, launch_id, 'stopped')
        else:
            logger.warning("No {} job to be stopped!".format(self.job))


class StartJob(luigi.Task):
    task_namespace = 'pulse'
    job = luigi.Parameter()
    date = luigi.DateMinuteParameter(default=datetime.datetime.today())
    from_latest_checkpoint = luigi.BoolParameter(default=False)
    host = luigi.Parameter()

    def requires(self):
        return [ StopJob(self.job), CrawlFeed(frequency=self.job), CrawlFeed(frequency='nevercrawl') ]

    # Do no output anything, as we don't want anything to prevent restarts, or initiate downstream actions.
    def output(self):
        return luigi.LocalTarget('state/jobs/{}.started.{}'.format(self.job, self.date.isoformat()))

    # Always allow re-starting:
    #def complete(self):
    #    return False

    def run(self):
        # Set up connection to H3:
        h = get_hapy_for_job(self.job, self.host)

        logger.info("Starting %s" % (self.job))
        targets = json.load(self.input()[1].open('r'))
        nevercrawl = json.load(self.input()[2].open('r'))
        logger.debug("Found %s Targets in date range." % len(targets))
        job = W3actJob(targets, self.job, heritrix=h, heritrix_job_dir=LOCAL_JOB_FOLDER, nevercrawl=nevercrawl)
        status = h.status(self.job)
        logger.info("Got current job status: %s" % status)

        logger.info("Starting job %s (from checkpoint = %s)..." % (job, self.from_latest_checkpoint))
        job.start(from_latest_checkpoint=self.from_latest_checkpoint)
        launch_id = h.get_launch_id(self.job)

        logger.info("Launched job %s/%s with %s seeds." % (job, launch_id, len(job.seeds)))
        with self.output().open('w') as f:
            f.write('{}\n'.format(launch_id))

        # Record an output file that can be use as a Target by a different task.:
        mark_job_as(job, launch_id, 'started')

        return


#@StartJob.event_handler(luigi.Event.SUCCESS)
#def run_task_success(task):
#    celebrate_success(task)


if __name__ == '__main__':
    #luigi.run(['pulse.StopJob', '--job', 'daily', '--local-scheduler'])
    luigi.run(['pulse.StartJob', '--host', 'crawler07.bl.uk', '--job', 'daily', '--local-scheduler'])
    #luigi.run(['pulse.StartJob', '--job', 'daily', '--from-latest-checkpoint'])
