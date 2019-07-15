import os
import logging
import posixpath
import luigi
import luigi.contrib.hdfs
from lib.webhdfs import WebHdfsPlainFormat
from luigi.contrib.postgres import PostgresTarget
from tasks.common import state_file

logger = logging.getLogger('luigi-interface')

"""
These classes define our standard concepts and Luigi Targets for events and outputs.
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
        print("GOT PATH " + path)
        super(ReportTarget, self).__init__(path)


def _make_dated_path(pather, state_folder, tag, suffix, date):
    if date is None:
        full_path = pather.join(
            str(state_folder),
            tag,
            "%s-%s" % (tag, suffix))
    elif isinstance(date, str):
        full_path = pather.join(
            str(state_folder),
            tag,
            date,
            "%s-%s-%s" % (date, tag, suffix))
    else:
        full_path = pather.join(
            str(state_folder),
            tag,
            date.strftime("%Y-%m"),
            '%s-%s-%s' % (date.strftime("%Y-%m-%d"), tag, suffix))

    return full_path


class TaskTarget(luigi.LocalTarget):
    """
    This Local Target defines the general task output targets.
    Aims to supercede the `status_file` approach from tasks.common.
    """
    local_state_folder = os.environ.get('LOCAL_STATE_FOLDER', '/var/task-state')

    def __init__(self, tag, suffix, date=None):
        # type: (str, str) -> None
        self.tag = tag
        self.suffix = suffix
        self.date = date

        # build the full path:
        full_path = _make_dated_path(os.path, self.local_state_folder, self.tag, self.suffix, self.date)

        super(TaskTarget, self).__init__(full_path)


class HdfsTaskTarget(luigi.contrib.hdfs.HdfsTarget):
    """
    This HDFS Target defines the general task output targets.
    Aims to supercede the `status_file` approach from tasks.common.
    """
    hdfs_state_folder = os.environ.get('HDFS_STATE_FOLDER', '/9_processing/task-state/')

    def __init__(self, tag, suffix, date=None, use_webhdfs=False, use_gzip=False ):
        # type: (str, str) -> None
        self.tag = tag
        self.suffix = suffix
        self.date = date
        self.use_gzip = use_gzip
        self.use_webhdfs = use_webhdfs

        # build the full path:
        full_path = _make_dated_path(posixpath, self.hdfs_state_folder, self.date, self.tag, self.suffix)

        if use_webhdfs:
            target_format = WebHdfsPlainFormat(use_gzip=use_gzip)
        else:
            target_format = luigi.contrib.hdfs.PlainFormat()

        super(HdfsTaskTarget, self).__init__(path=full_path, format=target_format)


class IngestTaskDBTarget(PostgresTarget):
    """
    A helper for storing task-complete flags in a dedicated database.
    """
    def __init__(self,task_group, task_result):
        # Initialise:
        super(IngestTaskDBTarget, self).__init__(
            host='ingest',
            database='ingest_task_state',
            user='ingest',
            password='ingest',
            table=task_group,
            update_id=task_result
        )
        # Set the actual DB table to use:
        self.marker_table = "ingest_task_state"


class AccessTaskDBTarget(PostgresTarget):
    """
    A helper for storing task-complete flags in a dedicated database.
    """
    def __init__(self, task_group, task_result):
        # Initialise:
        super(AccessTaskDBTarget, self).__init__(
            host=os.environ.get('ACCESS_TASKDB_HOST','access'),
            database='access_task_state',
            user='access',
            password='access',
            table=task_group,
            update_id=task_result
        )
        # Set the actual DB table to use:
        self.marker_table = "access_task_state"


class DatedStateFileTask(luigi.Task):
    """
    This specialisation of a general luigi Task support having two separate files - a small 'dated' overall task
    status file that manages a much larger 'current' file that contains detailed data.
    """

    on_hdfs = False
    output_ext = 'csv'
    dated_ext = 'json'

    def output(self):
        return self._state_file('current', ext=self.output_ext)

    def _state_file(self, state_date, ext):
        return state_file(state_date,self.tag,'%s.%s' % (self.name, ext), on_hdfs=self.on_hdfs)

    def dated_state_file(self):
        return self._state_file(self.date, ext=self.dated_ext)

    def complete(self):
        # Check the dated file exists
        dated_target = self.dated_state_file()
        logger.info("Checking %s exists..." % dated_target.path)
        exists = dated_target.exists()
        if not exists:
            return False
        return True


