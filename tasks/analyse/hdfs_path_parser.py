import os
import re
import enum
import logging
import datetime

# Headers in the original HDFS file listing (before paths are parsed)
file_list_headers = ['permissions', 'number_of_replicas', 'userid', 'groupid', 'filesize', 'modified_at', 'filename']


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
        self.collection = None
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
        self.launch_datetime = None

        # Look for different filename patterns:
        # ------------------------------------------------
        self.analyse_file_path()

        # Now Add data based on file kind and file name...
        # ------------------------------------------------

        # Distinguish 'bad' crawl files, e.g. warc.gz.open files that are down as warcs
        if self.kind == 'warcs':
            if not self.file_name.endswith(".warc.gz"):
                # The older selective crawls allowed CDX files alongside the WARCs:
                if self.collection == 'selective' and self.file_name.endswith(".warc.cdx"):
                    self.kind = 'cdx'
                else:
                    self.kind = 'warcs-invalid'
            else:
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

        # Distinguish crawl logs from other logs...
        if self.kind == 'logs':
            if self.file_name.startswith("crawl.log"):
                self.kind = 'crawl-logs'

    def analyse_file_path(self):
        """
        This function analyses the file path to classify the item.
        """
        
        #
        # Selective era layout /data/<target-id>/<instance-id>/<kind>
        #
        if re.search('^/data/', self.file_path ):
            self.collection = 'selective'
            self.stream = CrawlStream.selective
            mby  = re.search('^/data/([0-9]+)/([0-9]+)/(DLX/|Logs/|WARCS/|)([^\/]+)$', self.file_path)
            if mby:
                self.recognised = True
                # In this case the job is the Target ID and the launch is the Instance ID:
                (self.job, self.launch, self.kind, self.file_name) = mby.groups()
                self.kind = self.kind.lower().strip('/')
                if self.kind == '':
                    self.kind = 'unknown'
                self.launch_datetime = None
                
        # 
        # First NPLD era file layout /heritrix/output/(warcs|viral|logs)/<job>...
        #
        elif re.search('^/heritrix/output/(warcs|viral|logs)/.*', self.file_path ):
            self.collection = 'npld'
            # Original domain-crawl layout: kind/job (need to look for this first)
            mdc  = re.search('^/heritrix/output/(warcs|viral|logs)/(dc|crawl)[0-3]\-([0-9]{8}|[0-9]{14})/([^\/]+)$', self.file_path)
            # original frequent crawl layout: kind/job/launch-id
            mfc  = re.search('^/heritrix/output/(warcs|viral|logs)/([a-z\-0-9]+)[-/]([0-9]{12,14})/([^\/]+)$', self.file_path)
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

        # 
        # Second NPLD era file layout /heritrix/output/<job>/<launch>(warcs|viral|logs)/...
        #
        elif re.search('^/heritrix/output/(dc2.+|frequent.*)/.*', self.file_path ):
            self.collection = 'npld2'
            # 2019 frequent-crawl layout: job/launch-id/kind (same as DC now?
            mfc2 = re.search('^/heritrix/output/([a-z\-0-9]+)/([0-9]{12,14})/(warcs|viral|logs)/([^\/]+)$', self.file_path)
            if mfc2:
                self.recognised = True
                (self.job, self.launch, self.kind, self.file_name) = mfc2.groups()
                # Recognise domain crawls:
                if self.job.startswith('dc2'):
                    self.stream = CrawlStream.domain
                else:
                    self.stream = CrawlStream.frequent
                # Cope with variation in folder naming - all DC crawlers launched on the same day:
                if len(self.launch) > 8:
                    self.launch = self.launch[0:8]
                self.launch_datetime = datetime.datetime.strptime(self.launch, "%Y%m%d")
                
        # 
        # Files stored but intended for deletion.
        #
        elif self.file_path.startswith('/_to_be_deleted/'):
            self.recognised = True
            self.kind = 'to-be-deleted'
            self.file_name = os.path.basename(self.file_path)
            
        #
        # If un-matched, default to classifying by top-level folder.
        #
        else:
            self.collection = self.file_path.split(os.path.sep)[1]
            self.file_name = os.path.basename(self.file_path)
        

    def to_dict(self):
        d = dict()
        for f in self.field_names():
            d[f] = str(getattr(self,f,""))
        return d


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

