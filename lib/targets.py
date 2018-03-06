import os
import luigi
from lib.pathparsers import CrawlStream

"""
These classes define our standad concepts and Luigi Targets for events and outputs.
"""


class CrawlPackageTarget(luigi.LocalTarget):
    """
    This Local Target defines the contents of a crawl package,
    with the total number of files used as a version number.

    TBA: Stages, e.g.

    - raw file list
    - file list with hashes and ARKs
    - open package pre-ZIP
    - ZIPPED package
    - Package on HDFS

    ???

    """
    package_folder = os.environ.get('LOCAL_PACKAGE_FOLDER', '/data/packages')

    def __init__(self, stream, job, launch, file_count):
        # type: (CrawlStream, str, str, int) -> None
        self.stream = stream
        self.job = job
        self.launch = launch
        self.file_count = file_count

        # Set up the path:
        self.sub_path = '%s/%s/%s/%08d.json' % (self.stream.name, self.job, self.launch, self.file_count)
        path = os.path.join(self.package_folder, self.sub_path)
        super(CrawlPackageTarget, self).__init__(path)


class CrawlReportTarget(luigi.LocalTarget):
    """
    This Local Target defines the Crawl Report output files used to build the reporting site.
    """
    report_folder = os.environ.get('LOCAL_REPORT_FOLDER', '/data/ukwa-reports')

    def __init__(self, stream, job, launch):
        # type: (CrawlStream, str, str) -> None
        self.stream = stream
        self.job = job
        self.launch = launch

        # Set up path:
        self.sub_path = os.path.join('content/crawls', '%s/%s/%s/index.md' % (self.stream.name, self.job, self.launch))
        path = os.path.join(self.report_folder, self.sub_path)
        super(CrawlReportTarget, self).__init__(path)


class ReportTarget(luigi.LocalTarget):
    """
    This Local Target defines the general Report output files used to build the reporting site.
    """
    report_folder = os.environ.get('LOCAL_REPORT_FOLDER', '/data/ukwa-reports')

    def __init__(self, tag, suffix, date=None):
        # type: (str, str) -> None
        self.tag = tag
        self.suffix = suffix
        self.date = date

        # Set up path:
        if self.date:
            self.sub_path= os.path.join(
                self.date.strftime("%Y-%m"),
                tag,
                '%s-%s' % (self.date.strftime("%Y-%m-%d"), suffix))
        else:
            self.sub_path = os.path.join(tag, suffix)

        path = os.path.join(self.report_folder, self.sub_path)
        super(ReportTarget, self).__init__(path)

