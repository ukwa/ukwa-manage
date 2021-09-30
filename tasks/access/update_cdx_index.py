import os
import re
import json
import shutil
import logging
import datetime
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import random
import warcio
from urllib.request import urlopen
from urllib.parse import quote_plus, urlparse
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from tasks.access.hdfs_list_warcs import ListWarcsForDateRange
from tasks.common import state_file, CopyToTableInDB
from lib.webhdfs import WebHdfsPlainFormat, webhdfs
from lib.targets import AccessTaskDBTarget, TrackingDBStatusField
from prometheus_client import CollectorRegistry, Gauge

logger = logging.getLogger('luigi-interface')


class CopyToHDFS(luigi.Task):
    """
    This task takes a list of files and copies it up to HDFS to be an input for a task.
    """
    input_file = luigi.Parameter()
    prefix = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "access.hdfs"

    def run(self):
        # Read the file in and write it to HDFS
        input = luigi.LocalTarget(path=self.input_file)
        with input.open('r') as reader:
            with self.output().open('w') as writer:
                logger.warning("Copying %s to HDFS %s" % (input.path, self.output().path))
                for line in reader.readlines():
                    writer.write(line)

    def output(self):
        full_path = os.path.join(self.prefix, os.path.basename(self.input_file))
        return luigi.contrib.hdfs.HdfsTarget(full_path, format=WebHdfsPlainFormat(use_gzip=False))


class CdxIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    input_file = luigi.Parameter()
    cdx_service = luigi.Parameter()
    # This is used to add a timestamp to the output file, so this task can always be re-run:
    timestamp = luigi.DateSecondParameter(default=datetime.datetime.now())
    meta_flag = ''

    task_namespace = "access.index"

    num_reducers = 5

    def requires(self):
        return CopyToHDFS(input_file = self.input_file, prefix="/9_processing/warcs2cdx/")

    def ssh(self):
        return { 'host': 'mapred',
                 'key_file': '~/.ssh/id_rsa',
                 'username': 'access' }

    def jar(self):
#        dir_path = os.path.dirname(os.path.realpath(__file__))
#        return os.path.join(dir_path, "../jars/warc-hadoop-recordreaders-3.0.0-SNAPSHOT-job.jar")
# Note that when using ssh to submit jobs, this needs to be a JAR on the remote server:
        return "/home/access/github/ukwa-manage/tasks/jars/warc-hadoop-recordreaders-3.0.0-SNAPSHOT-job.jar"

    def main(self):
        return "uk.bl.wa.hadoop.mapreduce.cdx.ArchiveCDXGenerator"

    def args(self):
        return [
            "-Dmapred.compress.map.output=true",
            "-Dmapred.output.compress=true",
            "-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec",
            "-i", self.input(),
            "-o", self.output(),
            "-r", self.num_reducers,
            "-w",
            "-h",
            "-m", self.meta_flag,
            "-t", self.cdx_service,
            "-c", "CDX N b a m s k r M S V g"
        ]

    def output(self):
        timestamp = self.timestamp.isoformat()
        timestamp = timestamp.replace(':','-')
        file_prefix = os.path.splitext(os.path.basename(self.input_file))[0]
        return state_file(self.timestamp, 'warcs2cdx', '%s-submitted-%s.txt' % (file_prefix, timestamp), on_hdfs=True )


# Special reader to read the input stream and yield WARC records:
class TellingReader():
    def __init__(self, stream):
        self.stream = stream
        self.pos = 0

    def read(self, size=None):
        chunk = self.stream.read(size)
        self.pos += len(bytes(chunk))
        return chunk

    def readline(self, size=None):
        line = self.stream.readline(size)
        self.pos += len(bytes(line))
        return line

    def tell(self):
        return self.pos


class CheckCdxIndexForWARC(luigi.Task):
    input_file = luigi.Parameter()
    cdx_service = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=500)
    max_records_to_check = luigi.IntParameter(default=10)
    task_namespace = "access.index"

    count = 0
    tries = 0
    hits = 0
    records = 0

    def run(self):
        # Record the task complete successfully:
        self.output().touch()

    def get_capture_dates(self, url):
        # Get the hits for this URL:
        capture_dates = []
        # Paging, as we have a LOT of copies of some URLs:
        batch = 25000
        offset = 0
        next_batch = True
        while next_batch:
            try:
                # Get a batch:
                q = "type:urlquery url:" + quote_plus(url) + (" limit:%i offset:%i" % (batch, offset))
                cdx_query_url = "%s?q=%s" % (self.cdx_service, quote_plus(q))
                logger.info("Getting %s" % cdx_query_url)
                f = urlopen(cdx_query_url)
                content = f.read()
                f.close()
                # Grab the capture dates:
                dom = xml.dom.minidom.parseString(content)
                new_records = 0
                for de in dom.getElementsByTagName('capturedate'):
                    capture_dates.append(de.firstChild.nodeValue)
                    new_records += 1
                # Done?
                if new_records == 0:
                    next_batch = False
                else:
                    # Next batch:
                    offset += batch
            except ExpatError as e:
                logger.warning("Exception on lookup: "  + str(e))
                next_batch = False

        return capture_dates

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_task_percentage_urls_cdx_verified',
                  'Percentage of URLs verified as present in the CDX server',
                  registry=registry)
        if self.tries > 0:
            g.set(100.0 * self.hits/self.tries)

    def output(self):
        # Set up the qualified document ID:
        doc_id = "hdfs://hdfs:54310%s" % self.input_file
        # Get the cdx index name our of the server path:
        cdx_index = urlparse(self.cdx_service).path[1:]
        return TrackingDBStatusField(doc_id=doc_id, field='cdx_index_ss', value=cdx_index)


class CheckCdxIndex(luigi.WrapperTask):
    input_file = luigi.Parameter()
    cdx_service = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=500)
    task_namespace = "access.index"

    checked_total = 0

    def requires(self):
        # For each input file, open it up and get some URLs and timestamps.
        with open(str(self.input_file)) as f_in:
            for item in f_in.readlines():
                #logger.info("Found %s" % item)
                yield CheckCdxIndexForWARC(input_file=item.strip(), cdx_service=self.cdx_service)
                self.checked_total += 1

    def run(self):
        # If all the requirements are there, the whole set must be fine.
        self.output().touch()

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_task_warcs_cdx_verified',
                  'Number of WARCS verified as present in the CDX server',
                  registry=registry)
        g.set(self.checked_total)

    def output(self):
        return AccessTaskDBTarget("warc_set_verified","%s INDEXED OK" % self.input_file)


class CdxIndexAndVerify(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='frequent')
    verify_only = luigi.BoolParameter(default=False)
    cdx_service = luigi.Parameter(default=
                                  os.environ.get('CDX_SERVICE_URL'))
    # Specify a Tracking DB to manage state:
    tracking_db_url = luigi.Parameter(default=
                                      os.environ.get('TRACKING_DB_SOLR_URL'))
    # Specify a fine-grained run date so we can get fresh results
    run_date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    task_namespace = "access.index"

    def requires(self):
        # Extract just the name of the index itself:
        cdx_index_name= urlparse(self.cdx_service).path[1:]
        # Use this as a flag in the tracking DB:
        return ListWarcsForDateRange(
            start_date=self.start_date,
            end_date=self.end_date,
            stream=self.stream,
            status_field='cdx_index_ss', # Field used to indicate indexing status
            status_value=cdx_index_name,
            limit=1000,
            tracking_db_url=self.tracking_db_url
        )

    def run(self):
        # Check the file size
        st = os.stat(self.input().path)
        # Some days have no data, so we can skip them:
        if st.st_size == 0:
            logger.info("No unprocessed WARCs found for %s" % self.task_id)
            self.output().touch()
        else:
            # Make performing the actual indexing optional. Set --verify-only to skip the indexing step:
            if not self.verify_only:
                # Yield a Hadoop job to run the indexer:
                index_task = CdxIndexer(input_file=self.input().path, cdx_service=self.cdx_service)
                yield index_task

            # Then run the verification job again to check it worked:
            verify_task = CheckCdxIndex(input_file=self.input().path, cdx_service=self.cdx_service)
            yield verify_task

            # If it worked, record it here:
            if verify_task.complete():
                # Sometimes tasks get re-run...
                if not self.output().exists():
                    self.output().touch()

    def output(self):
        logger.info("Checking is complete: %s" % self.input().path)
        return AccessTaskDBTarget("warc_set_indexed_and_verified","%s OK" % self.input().path)


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)

    luigi.run(['access.index.CdxIndexAndVerify', '--workers', '5'])

#    very = CdxIndexAndVerify(
#        date=datetime.datetime.strptime("2018-02-16","%Y-%m-%d"),
#        target_date = datetime.datetime.strptime("2018-02-10", "%Y-%m-%d")
#    )
#    cdx = CheckCdxIndex(input_file=very.input().path)
#    cdx.run()

    #input = os.path.join(os.getcwd(),'test/input-list.txt')
    #luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
