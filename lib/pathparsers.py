import re
import os
import enum
import logging
import datetime

"""
These classes take file path conventions and parse and validate them.
"""

logger = logging.getLogger(__name__)


class CrawlStream(enum.Enum):
    """
    An enumeration of the different crawl streams.
    """

    selective = 1
    """'selective' is permissions-based collection. e.g. Pre-NPLD collections."""

    frequent = 2
    """ 'frequent' covers NPLD crawls of curated sites."""

    domain = 3
    """ 'domain' refers to NPLD domain crawls."""

    def __str__(self):
        return self.name


class HdfsPathParser(object):
    """
    This class takes a HDFS file path and determines what, if any, crawl it belongs to, etc.
    """

    @staticmethod
    def field_names():
        """This returns the extended set of field names that this class derives from the basic listing."""
        return ['recognised', 'collection', 'stream','job', 'kind', 'permissions', 'number_of_replicas', 'user_id', 'group_id', 'file_size', 'modified_at', 'timestamp', 'file_path', 'file_name', 'file_ext']

    def __init__(self, item):
        """
        Given a string containing the absolute HDFS file path, parse it to work our what kind of thing it is.

        Determines crawl job, launch, kind of file, etc.

        For WCT-era selective content, the job is the Target ID and the launch is the Instance ID.

        :param file_path:
        """

        # Perform basic processing:
        # ------------------------------------------------
        # To be captured later
        self.recognised = False
        self.stream = None
        self.job = None
        self.kind = 'unknown'
        # From the item listing:
        self.permissions = item['permissions']
        self.number_of_replicas = item['number_of_replicas']
        self.user_id = item['userid']
        self.group_id = item['groupid']
        self.file_size = item['filesize']
        self.modified_at = item['modified_at']
        self.file_path = item['filename']
        # Derived:
        self.file_name = os.path.basename(self.file_path)
        first_dot_at = self.file_name.find('.')
        if first_dot_at != -1:
            self.file_ext = self.file_name[first_dot_at:]
        else:
            self.file_ext = None
        self.timestamp_datetime = datetime.datetime.strptime(item['modified_at'], "%Y-%m-%dT%H:%M:%S")
        self.timestamp = self.timestamp_datetime.isoformat()

        # Look for different filename patterns:
        # ------------------------------------------------

        mfc = re.search('^/heritrix/output/(warcs|viral|logs)/([a-z\-0-9]+)[-/]([0-9]{12,14})/([^\/]+)$', self.file_path)
        mdc = re.search('^/heritrix/output/(warcs|viral|logs)/(dc|crawl)[0-3]\-([0-9]{8}|[0-9]{14})/([^\/]+)$', self.file_path)
        mby = re.search('^/data/([0-9])+/([0-9])+/(DLX/|Logs/|WARCS/|)([^\/]+)$', self.file_path)
        if mdc:
            self.recognised = True
            self.stream = CrawlStream.domain
            (self.kind, self.job, self.launch, self.file_name) = mdc.groups()
            self.job = 'domain'  # Overriding old job name.
            # Cope with variation in folder naming - all DC crawlers launched on the same day:
            if len(self.launch) > 8:
                self.launch = self.launch[0:8]
            self.launch_datetime = datetime.datetime.strptime(self.launch, "%Y%m%d")
        elif mfc:
            self.recognised = True
            self.stream = CrawlStream.frequent
            (self.kind, self.job, self.launch, self.file_name) = mfc.groups()
            self.launch_datetime = datetime.datetime.strptime(self.launch, "%Y%m%d%H%M%S")
        elif mby:
            self.recognised = True
            self.stream = CrawlStream.selective
            # In this case the job is the Target ID and the launch is the Instance ID:
            (self.job, self.launch, self.kind, self.file_name) = mby.groups()
            self.kind = self.kind.lower().strip('/')
            if self.kind == '':
                self.kind = 'unknown'
            self.launch_datetime = None
        elif self.file_path.startswith('/_to_be_deleted/'):
            self.recognised = True
            self.kind = 'to-be-deleted'
            self.file_name = os.path.basename(self.file_path)

        # Specify the collection, based on stream:
        if self.stream == CrawlStream.frequent or self.stream == CrawlStream.domain:
            self.collection = 'npld'
        elif self.stream == CrawlStream.selective:
            self.collection = 'selective'

        # Now Add data based on file name...
        # ------------------------------------------------

        # Attempt to parse file timestamp out of filename,
        # Store ISO formatted date in self.timestamp, datetime object in self.timestamp_datetime
        mwarc = re.search('^.*-([12][0-9]{16})-.*\.warc\.gz$', self.file_name)
        if mwarc:
            self.timestamp_datetime = datetime.datetime.strptime(mwarc.group(1), "%Y%m%d%H%M%S%f")
            self.timestamp = self.timestamp_datetime.isoformat()
        else:
            if self.stream and self.launch_datetime:
                # fall back on launch datetime:
                self.timestamp_datetime = self.launch_datetime
                self.timestamp = self.timestamp_datetime.isoformat()

        # TODO Distinguish 'bad' crawl files, e.g. warc.gz.open files that are down as warcs

        # TODO Do the same for crawl logs...

        # TODO distinguish crawl logs from other logs...
        if self.file_path.startswith("crawl.log"):
            self.type = "CRAWL_LOG"

    def to_dict(self):
        d = dict()
        for f in self.field_names():
            d[f] = str(getattr(self,f,""))
        return d


if __name__ == '__main__':
    print(HdfsPathParser({
        "filename": "/data/12312/212312/filename",
        "modified_at": "2011-02-12T13:14:11"}).kind)
    print(HdfsPathParser({
        "filename": "/data/12312/212312/DLX/filename",
        "modified_at": "2011-02-12T13:14:11"}).kind)
    print(HdfsPathParser({
        "filename": "/_to_be_deleted/data/12312/212312/DLX/filename",
        "modified_at": "2011-02-12T13:14:11"}).kind)
