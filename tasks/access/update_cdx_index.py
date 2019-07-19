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
    tag = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "access.hdfs"

    def output(self):
        full_path = os.path.join(self.tag, os.path.basename(self.input_file))
#        return luigi.contrib.hdfs.HdfsTarget(full_path, format=WebHdfsPlainFormat(use_gzip=False))
        return luigi.contrib.hdfs.HdfsTarget(full_path, format=luigi.contrib.hdfs.hdfs_format.PlainFormat)

    def run(self):
        # Read the file in and write it to HDFS
        input = luigi.LocalTarget(path=self.input_file)
        with input.open('r') as reader:
            with self.output().open('w') as writer:
                logger.warning("Copying %s to HDFS %s" % (input.path, self.output().path))
                for line in reader.readlines():
                    writer.write(line)
                    writer.write('\n')


class CdxIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    input_file = luigi.Parameter()
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    # This is used to add a timestamp to the output file, so this task can always be re-run:
    timestamp = luigi.DateSecondParameter(default=datetime.datetime.now())
    meta_flag = ''

    task_namespace = "access.index"

    num_reducers = 5

    def output(self):
        timestamp = self.timestamp.isoformat()
        timestamp = timestamp.replace(':','-')
        file_prefix = os.path.splitext(os.path.basename(self.input_file))[0]
        return state_file(self.timestamp, 'warcs2cdx', '%s-submitted-%s.txt' % (file_prefix, timestamp), on_hdfs=True )

    def requires(self):
        return CopyToHDFS(input_file = self.input_file, tag="warcs2cdx")

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
            "-t", self.cdx_server,
            "-c", "CDX N b a m s k r M S V g"
        ]


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
    sampling_rate = luigi.IntParameter(default=500)
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    max_records_to_check = luigi.IntParameter(default=10)
    task_namespace = "access.index"

    count = 0
    tries = 0
    hits = 0
    records = 0

    def output(self):
        # Set up the qualified document ID:
        doc_id = "hdfs://hdfs:54310%s" % self.input_file
        # Get the cdx index name our of the server path:
        cdx_index = urlparse(self.cdx_server).path[1:]
        return TrackingDBStatusField(doc_id=doc_id, field='cdx_index_ss', value=cdx_index)

    def run(self):
        hdfs_file = luigi.contrib.hdfs.HdfsTarget(path=self.input_file, format=WebHdfsPlainFormat())
        logger.info("Opening " + hdfs_file.path)
        #fin = hdfs_file.open('r')
        client = webhdfs()
        with client.read(hdfs_file.path) as fin:
            reader = warcio.ArchiveIterator(TellingReader(fin))
            for record in reader:
                #logger.warning("Got record format and headers: %s %s %s" % (
                #record.format, record.rec_headers, record.http_headers))
                # content = record.content_stream().read()
                # logger.warning("Record content: %s" % content[:128])
                # logger.warning("Record content as hex: %s" % binascii.hexlify(content[:128]))
                #logger.warning("Got record offset + length: %i %i" % (reader.get_record_offset(), reader.get_record_length() ))
                self.records += 1

                # Only look at valid response records:
                if record.rec_type == 'response' and 'application/http' in record.content_type:
                    record_url = record.rec_headers.get_header('WARC-Target-URI')
                    # Skip ridiculously long URIs
                    if len(record_url) > 2000:
                        logger.warning("Skipping very long URL: %s" % record_url)
                        continue
                    # Timestamp, stripped down to Wayback form:
                    timestamp = record.rec_headers.get_header('WARC-Date')
                    timestamp = re.sub('[^0-9]', '', timestamp)
                    #logger.info("Found a record: %s @ %s" % (record_url, timestamp))
                    # Check a random subset of the records, always emitting the first record:
                    if self.count == 0 or random.randint(1, self.sampling_rate) == 1:
                        logger.info("Checking a record: %s @ %s" % (record_url, timestamp))
                        capture_dates = self.get_capture_dates(record_url)
                        if timestamp in capture_dates:
                            self.hits += 1
                        else:
                            logger.warning("Record not found in index: %s @ %s" % (record_url, timestamp))
                        # Keep track of checked records:
                        self.tries += 1
                        # If we've tried enough records, exit:
                        if self.tries >= self.max_records_to_check:
                            break
                    # Keep track of total records:
                    self.count += 1

            # Ensure the input stream is closed (despite not reading all the data):
            #reader.read_to_end()

        # If there were not records at all, something went wrong!
        if self.records == 0:
            raise Exception("For %s, found %i records at all!" % (self.input_file, self.records))

        # Otherwise, the hits and tries should match (n.b. can be zero if there are no indexable records in this WARC):
        if self.hits == self.tries:
            # Record the task complete successfully:
            self.output().touch()
        else:
            raise Exception("For %s, only %i of %i records checked are in the CDX index!"%(self.input_file, self.hits, self.tries))

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
                cdx_query_url = "%s?q=%s" % (self.cdx_server, quote_plus(q))
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


class CheckCdxIndex(luigi.WrapperTask):
    input_file = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=500)
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    task_namespace = "access.index"

    checked_total = 0

    def requires(self):
        # For each input file, open it up and get some URLs and timestamps.
        with open(str(self.input_file)) as f_in:
            for item in f_in.readlines():
                #logger.info("Found %s" % item)
                yield CheckCdxIndexForWARC(item)
                self.checked_total += 1

    def output(self):
        return AccessTaskDBTarget("warc_set_verified","%s INDEXED OK" % self.input_file)

    def run(self):
        # If all the requirements are there, the whole set must be fine.
        self.output().touch()

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_task_warcs_cdx_verified',
                  'Number of WARCS verified as present in the CDX server',
                  registry=registry)
        g.set(self.checked_total)


class CdxIndexAndVerify(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='frequent')
    verify_only = luigi.BoolParameter(default=False)
    task_namespace = "access.index"

    def requires(self):
        return ListWarcsForDateRange(
            start_date=self.start_date,
            end_date=self.end_date,
            stream=self.stream,
            status_field='cdx_index_ss',
            status_value='data-heritrix',
            limit=1000
        )

    def output(self):
        logger.info("Checking is complete: %s" % self.input().path)
        return AccessTaskDBTarget("warc_set_indexed_and_verified","%s OK" % self.input().path)

    def run(self):
        # Check the file size
        st = os.stat(self.input().path)
        # Some days have no data, so we can skip them:
        if st.st_size == 0:
            logger.info("No WARCs found for %s" % self.task_id)
            self.output().touch()
        else:
            # Make performing the actual indexing optional. Set --verify-only to skip the indexing step:
            if not self.verify_only:
                # Yield a Hadoop job to run the indexer:
                index_task = CdxIndexer(self.input().path)
                yield index_task

            # Then run the verification job again to check it worked:
            verify_task = CheckCdxIndex(input_file=self.input().path)
            yield verify_task

            # If it worked, record it here:
            if verify_task.complete():
                # Sometimes tasks get re-run...
                if not self.output().exists():
                    self.output().touch()


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
