import os
import re
import json
import logging
import datetime
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import random
import warcio
import urllib
from urllib import quote_plus  # python 2
# from urllib.parse import quote_plus # python 3
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from tasks.access.listings import ListWarcsForDate
from tasks.common import state_file, report_file, CopyToTableInDB, taskdb_target
from lib.webhdfs import WebHdfsPlainFormat

logger = logging.getLogger('luigi-interface')


class CopyToHDFS(luigi.Task):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a JSON item for each, outputting the result in
    [JSON Lines format](http://jsonlines.org/).

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    input_file = luigi.Parameter()
    tag = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def output(self):
        full_path = os.path.join(self.tag, os.path.basename(self.input_file))
        return luigi.contrib.hdfs.HdfsTarget(full_path, format=WebHdfsPlainFormat(use_gzip=False))

    def run(self):
        # Read the file in and write it to HDFS
        input = luigi.LocalTarget(path=self.input_file)
        with input.open('r') as reader:
            with self.output().open('w') as writer:
                items = json.load(reader)
                for item in items:
                    logger.info("Found %s" % item)
                    writer.write(item['filename'])


class CdxIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    input_file = luigi.Parameter()
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')

    meta_flag = ''
    task_namespace = 'index'

    num_reducers = 50

    def output(self):
        out_name = os.path.join("warcs2cdx", "%s-submitted.txt" % os.path.splitext(os.path.basename(self.input_file))[0])
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.Plain)

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
        #logger.warning("read()ing from current position: %i, size=%s" % (self.pos, size))
        chunk = self.stream.read(size)
        #if len(bytes(chunk)) == 0:
        #    logger.warning("read() 0 bytes, current position: %i" % self.pos)
        #else:
        #    logger.warning("read() %s" % binascii.hexlify(chunk[:64]))
        self.pos += len(bytes(chunk))
        #logger.warning("read()ing current position now: %i" % self.pos)
        return chunk

    def readline(self, size=None):
        #logger.warning("readline()ing from current position: %i" % self.pos)
        line = self.stream.readline(size)
        #logger.warning("readline() %s" % line)
        self.pos += len(bytes(line))
        #logger.warning("readline()ing current position now: %i" % self.pos)
        return line

    def tell(self):
        #logger.debug("tell()ing current position: %i" % self.pos)
        return self.pos


class CheckCdxIndexForWARC(CopyToTableInDB):
    input_file = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=500)
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    max_records_to_check = luigi.IntParameter(default=10)
    task_namespace = "index"

    table = 'index_result_table'
    columns = (('warc_path', 'text'),
               ('records_checked', 'int'),
               ('records_found', 'float'))

    count = 0
    tries = 0
    hits = 0

    def rows(self):
        hdfs_file = luigi.contrib.hdfs.HdfsTarget(path=self.input_file, format=WebHdfsPlainFormat())
        logger.info("Opening " + hdfs_file.path)
        fin = hdfs_file.open('r')
        reader = warcio.ArchiveIterator(TellingReader(fin))
        for record in reader:
            #logger.warning("Got record format and headers: %s %s %s" % (
            #record.format, record.rec_headers, record.http_headers))
            # content = record.content_stream().read()
            # logger.warning("Record content: %s" % content[:128])
            # logger.warning("Record content as hex: %s" % binascii.hexlify(content[:128]))
            #logger.warning("Got record offset + length: %i %i" % (reader.get_record_offset(), reader.get_record_length() ))

            # Only look at valid response records:
            if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):
                record_url = record.rec_headers.get_header('WARC-Target-URI')
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

        # Close the input stream and catch any exception due to closing it early:
        fin._abort()

        if self.hits == self.tries:
            yield self.input_file, self.tries, self.hits
        else:
            raise Exception("For %s, only %i of %i records checked are in the CDX index!"%(self.input_file, self.hits, self.tries))

    def get_capture_dates(self, url):
        # Get the hits for this URL (go deep, as we have a LOT of copies of some URLs):
        q = "type:urlquery url:" + quote_plus(url) + " limit:1000000"
        cdx_query_url = "%s?q=%s" % (self.cdx_server, quote_plus(q))
        capture_dates = []
        try:
            proxies = { 'http': 'http://explorer:3127'}
            f = urllib.urlopen(cdx_query_url, proxies=proxies)
            dom = xml.dom.minidom.parseString(f.read())
            for de in dom.getElementsByTagName('capturedate'):
                capture_dates.append(de.firstChild.nodeValue)
            f.close()
        except ExpatError, e:
            logger.warning("Exception on lookup: "  + str(e))

        return capture_dates


class CheckCdxIndex(luigi.WrapperTask):
    input_file = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=500)
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    task_namespace = "index"

    def requires(self):
        # For each input file, open it up and get some URLs and timestamps.
        with open(str(self.input_file)) as f_in:
            items = json.load(f_in)
            for item in items:
                logger.info("Found %s" % item)
                yield CheckCdxIndexForWARC(item['filename'])

    def output(self):
        return taskdb_target("warc_set_verified","%s INDEXED OK" % self.input_file)

    def run(self):
        # If all the requirements are there, the whole set must be fine.
        self.output().touch()


class CdxIndexAndVerify(luigi.Task):
    target_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='npld')

    def requires(self):
        return ListWarcsForDate(target_date=self.target_date, stream=self.stream)

    def output(self):
        logger.info("Checking is complete: %s" % self.input().path)
        return taskdb_target("warc_set_indexed_and_verified","%s OK" % self.input().path)

    def run(self):
        # Yield a Hadoop job to run the indexer:
        index_task = CdxIndexer(self.input().path)
        yield index_task

        # Then run the verification job again to check it worked:
        verify_task = CheckCdxIndex(input_file=self.input().path)
        yield verify_task

        # If it worked, record it here:
        if verify_task.complete():
            self.output().touch()


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.interface.setup_interface_logging()

    luigi.run(['CdxIndexAndVerify', '--workers', '10'])

#    very = CdxIndexAndVerify(
#        date=datetime.datetime.strptime("2018-02-16","%Y-%m-%d"),
#        target_date = datetime.datetime.strptime("2018-02-10", "%Y-%m-%d")
#    )
#    cdx = CheckCdxIndex(input_file=very.input().path)
#    cdx.run()

    #input = os.path.join(os.getcwd(),'test/input-list.txt')
    #luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
