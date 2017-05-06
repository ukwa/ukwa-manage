import os
import logging
import luigi
import luigi.date_interval
import luigi.contrib.hdfs
import luigi.contrib.hadoop
import luigi.contrib.hadoop_jar
from tasks.process.scan_hdfs import ScanForOutputs, GenerateWarcList
from tasks.common import logger


class GenerateWarcStats(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Generates the Warc stats by reading in each file and splitting the stream into entries.
    As this uses the stream directly and so data-locality is preserved.

    Parameters:
        job: job
        launch: launch
    """
    job = luigi.Parameter()
    launch = luigi.Parameter()

    def output(self):
        out_name = "%s-stats.tsv" % os.path.splitext(self.input().path)[0]
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainDir)

    def requires(self):
        return GenerateWarcList(self.job, self.launch)

    def jar(self):
        return "../jars/warc-hadoop-recordreaders-2.2.0-BETA-7-SNAPSHOT-job.jar"

    def main(self):
        return "uk.bl.wa.hadoop.mapreduce.warcstats.WARCStatsTool"

    def args(self):
        return [self.input(), self.output()]

    def ssh(self):
        return { "host": "hadoop.ddb.wa.bl.uk", "username": "root", "key_file" : "~/.ssh/id_rsa" }


class GenerateCrawlReport(ScanForOutputs):

    def complete(self):
        return False

    def process_output(self,job,launch):
        logger.info("Processing %s/%s" % (job, launch))
        yield GenerateWarcStats(job,launch)


if __name__ == '__main__':
    luigi.run(['scan.GenerateCrawlReport', '--local-scheduler'])
    #luigi.run(['GenerateCrawlReport', '--date-interval', "2017-01-13-2017-01-18", '--local-scheduler'])
