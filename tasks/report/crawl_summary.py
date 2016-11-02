import os
import logging
import datetime
import luigi
import luigi.date_interval
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar

logger = logging.getLogger('luigi-interface')

class ScanForOutputs(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    date_interval = luigi.DateIntervalParameter(
        default=luigi.date_interval.Custom(datetime.date.today() - datetime.timedelta(days=5), datetime.date.today()))
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    hdfs_prefix = "/1_data/pulse/crawler07/heritrix/output"

    def requires(self):
        # Enumerate the jobs:
        for (job, launch) in self.enumerate_launches():
            logger.info("Yielding %s/%s" % ( job, launch ))
            yield self.process_output(job, launch)

    def enumerate_launches(self):
        # Get HDFS client:
        client = luigi.contrib.hdfs.get_autoconfig_client()
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in client.listdir("%s/warcs" % self.hdfs_prefix):
                job = os.path.basename(job_item)
                launch_glob = date.strftime('%Y%m%d')
                logger.info("Looking for job launch folders matching %s" % launch_glob)
                for launch_item in client.listdir("%s/warcs/%s" % (self.hdfs_prefix, job)):
                    if launch_item.startswith(launch_glob):
                        launch = os.path.basename(launch_item)
                        yield (job, launch)

class GenerateCrawlReport(ScanForOutputs):

    def complete(self):
        return False

    def process_output(self,job,launch):
        logger.info("Processing %s/%s" % (job, launch))


if __name__ == '__main__':
    luigi.run(['GenerateCrawlReport', '--local-scheduler'])
    #luigi.run(['GenerateCrawlReport', '--date-interval', "2017-01-13-2017-01-18", '--local-scheduler'])
