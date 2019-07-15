import os
import re
import csv
import enum
import json
import gzip
import glob
import shutil
import logging
import datetime
import luigi
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from tasks.analyse.hdfs_path_parser import HdfsPathParser, CrawlStream
from tasks.analyse.hdfs_analysis import CopyFileListToHDFS, ListAllFilesOnHDFSToLocalFile
from tasks.common import state_file
from lib.webhdfs import webhdfs
from lib.targets import AccessTaskDBTarget

logger = logging.getLogger('luigi-interface')

"""
Tasks relating to using the list of HDFS content to update access systems.
"""


class ListWarcFileSets(luigi.Task):
    """
    Lists the WARCS and arranges them by date:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    stream = luigi.EnumParameter(enum=CrawlStream, default=CrawlStream.frequent)
    task_namespace = 'access.report'

    def requires(self):
        return DownloadHDFSFileList(self.date, self.stream)

    #def complete(self):
    #    return False

    def output(self):
        return state_file(self.date, 'warc', '%s-warc-filesets.txt' % self.stream.name)

    def run(self):
        # Go through the data and assemble the resources for each crawl:
        filenames = []
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            first_line = reader.next()
            for item in reader:
                # Parse file paths and names:
                p = HdfsPathParser(item)
                # Look at WARCS in this stream:
                if p.stream == self.stream and p.kind == 'warcs' and p.file_name.endswith(".warc.gz"):
                    filenames.append(p.file_path)

        # Sanity check:
        if len(filenames) == 0:
            raise Exception("No filenames generated! Something went wrong!")

        # Finally, emit the list of output files as the task output:
        filenames = sorted(filenames)
        counter = 0
        with self.output().open('w') as f:
            for output_path in filenames:
                if counter > 0:
                    if counter % 10000 == 0:
                        f.write('\n')
                    else:
                        f.write(' ')
                f.write('%s' % output_path)
                counter += 1


class ListWarcsByDate(luigi.Task):
    """
    Lists the WARCS with datestamps corresponding to a particular day. Defaults to yesterday.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    stream = luigi.EnumParameter(enum=CrawlStream, default=CrawlStream.frequent)

    task_namespace = 'access.report'

    file_count = 0

    def requires(self):
        # Get todays list:
        return DownloadHDFSFileList(self.date)

    def output(self):
        return state_file(self.date, 'warcs', '%s-warc-files-by-date.txt' % self.stream.name )

    def run(self):
        # Build up a list of all WARCS, by day:
        by_day = {}
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            first_line = reader.next()
            for item in reader:
                # Parse file paths and names:
                p = HdfsPathParser(item)
                # Look at WARCS in this stream:
                if p.stream == self.stream and p.kind == 'warcs':
                    # Take the first ten characters of the timestamp - YYYY-MM-DD:
                    file_datestamp = p.timestamp[0:10]

                    if file_datestamp not in by_day:
                        by_day[file_datestamp] = []

                    by_day[file_datestamp].append(item)
        # Write them out:
        filenames = []
        for datestamp in by_day:
            datestamp_output = state_file(None, 'warcs-by-day', '%s-%s-%s-warcs-for-date.txt' % (self.stream.name,datestamp,len(by_day[datestamp])))
            with datestamp_output.open('w') as f:
                f.write(json.dumps(by_day[datestamp], indent=2))

        # Emit the list of output files as the task output:
        self.file_count = len(filenames)
        with self.output().open('w') as f:
            for output_path in filenames:
                f.write('%s\n' % output_path)


class ListWarcsForDate(luigi.Task):
    """
    Lists the WARCS with datestamps corresponding to a particular day. Defaults to yesterday.
    """
    target_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.EnumParameter(enum=CrawlStream, default=CrawlStream.frequent)
    date = luigi.DateParameter(default=datetime.date.today())

    task_namespace = 'access.report'

    def requires(self):
        # Get current list (use self.date not the target_date):
        return ListWarcsByDate(self.date, self.stream)

    def find_best_path(self):
        # List all the warcs-by-date files and select the one with the highest count.
        datestamp = self.target_date.strftime("%Y-%m-%d")
        target_path = state_file(None, 'warcs-by-day', '%s-%s-*-warcs-for-date.txt' % (self.stream.name, datestamp)).path
        max_count = 0
        best_path = None
        for path in glob.glob(target_path):
            count = int(re.search('-([0-9]+)-warcs-for-date.txt$', path).group(1))
            # If this has a higher file count, use it:
            if count > max_count:
                max_count = count
                best_path = path

        return best_path

    def complete(self):
        # Ensure ListWarcsByDate has been run:
        if not self.requires().complete():
            return False
        # If the pre-requisite has definately run, proceed as normal:
        return super(ListWarcsForDate, self).complete()

    def output(self):
        best_path = self.find_best_path()
        if best_path:
            # List of WARCs for the given day stored here:
            return luigi.LocalTarget(path=best_path)
        else:
            # Return special Target that says no WARCs were to be found
            return NoWARCsToday()

    def run(self):
        # The output does all the work here.
        pass


class NoWARCsToday(luigi.Target):
    """
    Special Target that exists only to inform downstream tasks that there are no WARCs for a given day.
    """
    def exists(self):
        return True


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.interface.setup_interface_logging()

    class Color(enum.Enum):
        RED = 1
        GREEN = 2
        BLUE = 3

    print(Color.RED)

    v = CrawlStream.frequent
    print(v)
    print(v.name)
