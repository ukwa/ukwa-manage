from tasks.hadoop.warc.warctasks import HadoopWarcReaderJob
import os
import re
import sys
import random
import logging
import luigi
import luigi.contrib
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import urllib
from urllib import quote_plus  # python 2
# from urllib.parse import quote_plus # python 3

logger = logging.getLogger(__name__)


class CheckCdxIndex(HadoopWarcReaderJob):
    """
    Picks a sample of URLs from some WARCs and checks they are in the CDX index.

    Parameters:
        input_file: The path for the file that contains the list of WARC files to process
        from_local: Whether the paths refer to files on HDFS or local files
        read_for_offset: Whether the WARC parser should read the whole record so it can populate the
                         record.raw_offset and record.raw_length fields (good for CDX indexing). Enabling this will
                         mean the reader has consumed the content body so your job will not have access to it.

    """

    sampling_rate = luigi.IntParameter(default=100)
    cdx_server = luigi.Parameter(default="http://bigcdx:8080/data-heritrix")

    n_reduce_tasks = 1

    count = 0
    tries = 0
    hits = 0

    def __init__(self, **kwargs):
        """Ensure arguments are set up correctly."""
        super(CheckCdxIndex, self).__init__(**kwargs)

    def output(self):
        """ Specify the output file name, which is based on the input file name."""
        out_name = "%s-cdx-verification-sampling-rate-%i.txt" % \
                   (os.path.splitext(os.path.basename(self.input_file))[0], self.sampling_rate)
        if self.from_local:
            return luigi.LocalTarget(out_name)
        else:
            return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainFormat)

    def mapper(self, record):
        # type: (ArcWarcRecord) -> [(str, str)]
        """ Takes the parsed WARC record and extracts some basic stats."""

        # Extract the URI and status code:
        record_url = record.rec_headers.get_header('WARC-Target-URI')
        timestamp = record.rec_headers.get_header('WARC-Date')
        logger.warning("Found a record: %s @ %s" % (record_url, timestamp))

        # Only look at valid response records:
        if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):
            # Strip down to Wayback form:
            timestamp = re.sub('[^0-9]','', timestamp)
            # Check a random subset of the records, always emitting the first record:
            if self.count == 0 or random.randint(1, self.sampling_rate) == 1:
                logger.warn("Checking a record: %s" % record_url)
                capture_dates = self.get_capture_dates(record_url)
                if timestamp in capture_dates:
                    self.hits += 1
                    yield "HITS", 1
                else:
                    yield "MISS", record_url
                # Keep track of checked records:
                self.tries += 1
                yield "SAMPLE_SIZE", 1
            # Keep track of total records:
            self.count += 1
            yield "TOTAL", 1

        # Occasionally report status:
        # (see https://hadoop.apache.org/docs/r1.2.1/streaming.html#How+do+I+update+counters+in+streaming+applications%3F)
        if self.count % 100 == 0:
            sys.stderr.write("reporter:counter:CDX,TRIES,%i\n" % self.tries)
            sys.stderr.write("reporter:counter:CDX,HITS,%i\n" % self.hits)
            sys.stderr.write("reporter:counter:CDX,TOTAL,%i\n" % self.count)
            sys.stderr.write("reporter:status:CDX %i/%i checks performed.\n" % (self.tries, self.count))

    def get_capture_dates(self, url):
        # Get the hits for this URL:
        q = "type:urlquery url:" + quote_plus(url)
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

    def reducer(self, key, values):
        """
        This counts the output conditions:

        :param key:
        :param values:
        :return:
        """
        if key == "MISS":
            count = 0
            for value in values:
                logger.warning("%s %s" % (key, value))
                yield key, value
                count += 1
            logger.warning("MISSES %i" % count)
            yield "MISSES", count
        else:
            logger.warning("REDUCER KEY: %s" % key)
            count = 0
            for value in values:
                logger.warning("REDUCER VALUE: %s" % value)
                count += value
            logger.warning("%s %i" % (key, count))
            yield key, count


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.interface.setup_interface_logging()

    input = os.path.join(os.getcwd(),'test/input-list.txt')
    luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
