import os
import time
import logging
import datetime
import tempfile
import luigi
import luigi.date_interval
import luigi.contrib.hdfs
import luigi.contrib.hadoop
import luigi.contrib.hadoop_jar
from ukwa.tasks.settings import state, h3
from ukwa.tasks.common import logger

def get_modest_interval():
    return luigi.date_interval.Custom(
        datetime.date.today() - datetime.timedelta(days=14),
        datetime.date.today() + datetime.timedelta(days=1))


class ScanForOutputs(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    date_interval = luigi.DateIntervalParameter(default=get_modest_interval())
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def requires(self):
        # Enumerate the jobs:
        for (job, launch) in self.enumerate_launches():
            #logger.debug("Yielding %s/%s" % ( job, launch ))
            yield self.process_output(job, launch)

    def enumerate_launches(self):
        # Get HDFS client:
        client = luigi.contrib.hdfs.WebHdfsClient()
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            logger.info("Scanning date %s..." % date)
            for job_item in client.listdir("%s/heritrix/output/warcs" % h3().hdfs_prefix):
                job = os.path.basename(job_item)
                launch_glob = date.strftime('%Y%m%d')
                #logger.debug("Looking for job launch folders matching %s" % launch_glob)
                for launch_item in client.listdir("%s/heritrix/output/warcs/%s" % (h3.hdfs_prefix, job)):
                    if launch_item.startswith(launch_glob):
                        launch = os.path.basename(launch_item)
                        yield (job, launch)


class GenerateWarcList(luigi.Task):
    job = luigi.Parameter()
    launch = luigi.Parameter()

    def output(self):
        target = luigi.contrib.hdfs.HdfsTarget(os.path.join(state().hdfs_folder,
                                                            "%s-%s-warclist.txt" % (self.job, self.launch)))
        return target

    def run(self):
        # Get HDFS client:
        client = luigi.contrib.hdfs.WebHdfsClient()
        data = ""
        for warc in client.listdir("%s/warcs/%s/%s" % (h3.hdfs_prefix, self.job, self.launch)):
            logger.info("Listing %s" % warc)
            data += "%s\n" % warc
        temp_path = '%s.temp-%s' % (self.output().path, int(time.time()))
        logger.info("Uploading to %s" % (temp_path) )
        client.client.write(temp_path, data)
        logger.info("Moving %s to %s" % (temp_path, self.output().path) )
        client.move(temp_path, self.output().path)
