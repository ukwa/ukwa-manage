import os
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from ukwa_luigi.warc_tasks import ExternalListFile

class GenerateWarcStatsIndirect(luigi.contrib.hadoop.JobTask):
    """
    Generates the WARC stats by reading each file in turn. Data is therefore no-local.

    Parameters:
        input_file: The file (on HDFS) that contains the list of WARC files to process
    """
    input_file = luigi.Parameter()

    def output(self):
        out_name = "%s-stats.tsv" % os.path.splitext(self.input_file)[0]
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainDir)

    def requires(self):
        return ExternalListFile(self.input_file)

    def extra_modules(self):
        return []

    def extra_files(self):
        return ["luigi.cfg"]

    def mapper(self, line):
        """
        Each line should be a path to a WARC file on HDFS

        We open each one in turn, and scan the contents.

        The pywb record parser gives access to the following properties (at least):

        entry['urlkey']
        entry['timestamp']
        entry['url']
        entry['mime']
        entry['status']
        entry['digest']
        entry['length']
        entry['offset']

        :param line:
        :return:
        """

        # Ignore blank lines:
        if line == '':
            return

        warc = luigi.contrib.hdfs.HdfsTarget(line)
        #entry_iter = DefaultRecordParser(sort=False,
        #                                 surt_ordered=True,
       ##                                  include_all=False,
        #                                 verify_http=False,
        #                                 cdx09=False,
        #                                 cdxj=False,
        #                                 minimal=False)(warc.open('rb'))

        #for entry in entry_iter:
        #    hostname = urlparse.urlparse(entry['url']).hostname
        #    yield hostname, entry['status']

    def reducer(self, key, values):
        """

        :param key:
        :param values:
        :return:
        """
        for value in values:
            yield key, sum(values)


