from tasks.hadoop.warc.warctasks import HadoopWarcReaderJob
from six.moves.urllib.parse import urlparse
import os
import luigi
import luigi.contrib

luigi.interface.setup_interface_logging()


class GenerateWarcStats(HadoopWarcReaderJob):
    """
    Generates some WARC stats from a stream of ARC/WARC Records. The :py:class:`HadoopWarcReaderJob` superclass
    handles the parsing of input files and uses warcio to break it into

    Parameters:
        input_file: The path for the file that contains the list of WARC files to process
        from_local: Whether the paths refer to files on HDFS or local files
        read_for_offset: Whether the WARC parser should read the whole record so it can populate the
                         record.raw_offset and record.raw_length fields (good for CDX indexing). Enabling this will
                         mean the reader has consumed the content body so your job will not have access to it.

    """

    n_reduce_tasks = 10

    def __init__(self, **kwargs):
        """Ensure arguments are set up correctly."""
        super(GenerateWarcStats, self).__init__(**kwargs)

    def output(self):
        """ Specify the output file name, which is based on the input file name."""
        out_name = "%s-stats.tsv" % os.path.splitext(self.input_file)[0]
        if self.from_local:
            return luigi.LocalTarget(out_name)
        else:
            return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainFormat)

    def mapper(self, record):
        # type: (ArcWarcRecord) -> [(str, str)]
        """ Takes the parsed WARC record and extracts some basic stats."""

        # Only look at valid response records:
        if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):

            # Extract the URI and status code:
            record_url = record.rec_headers.get_header('WARC-Target-URI')
            status_code = record.http_headers.get_statuscode()
            # Extract the hostname:
            hostname = urlparse(record_url).hostname

            #if self.read_for_offset:
            #    print(record.raw_offset, record.raw_length, record.length, record.content_stream().read())
            #else:
            #    print(record.length, record.content_stream().read())

            hostname = urlparse(record_url).hostname
            yield "%s\t%s" % (hostname, status_code), 1


            # Yield the hostname+status code as the key, and the count to be summed.
            yield "%s\t%s" % (hostname, status_code), 1

    def reducer(self, key, values):
        """
        This simple reducer just sums the values for each key

        :param key:
        :param values:
        :return:
        """
        yield key, sum(values)


if __name__ == '__main__':
    # Just run it directly, rather than via the Luigi Scheduler:
    job = GenerateWarcStats(input_file='../test/input-list.txt', from_local=True)
    #job.n_reduce_tasks = 20
    job.run()
    out = job.output()

    # Example running from Python, but via Luigi:
    #luigi.run(['GenerateWarcStats', '--input-file', '/Users/andy/Documents/workspace/python-shepherd/input-files.txt', '--local-scheduler'])
