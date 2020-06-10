import os
import logging
import posixpath
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from luigi.contrib.postgres import PostgresTarget, CopyToTable
from lib.webhdfs import WebHdfsPlainFormat

LOCAL_STATE_FOLDER = os.environ.get('LOCAL_STATE_FOLDER', '/var/task-state')
HDFS_STATE_FOLDER = os.environ.get('HDFS_STATE_FOLDER','/9_processing/task-state/')

logger = logging.getLogger('luigi-interface')


def state_file(date, tag, suffix, on_hdfs=False, use_gzip=False, use_webhdfs=False):
    # Set up the state folder:
    state_folder = LOCAL_STATE_FOLDER
    pather = os.path
    if on_hdfs:
        pather = posixpath
        state_folder = HDFS_STATE_FOLDER

    # build the full path:
    if date is None:
        full_path = pather.join(
            str(state_folder),
            tag,
            "%s-%s" % (tag,suffix))
    elif isinstance(date, str):
        full_path = pather.join(
            str(state_folder),
            tag,
            date,
            "%s-%s-%s" % (date,tag,suffix))
    else:
        full_path = pather.join(
            str(state_folder),
            tag,
            date.strftime("%Y-%m"),
            '%s-%s-%s' % (date.strftime("%Y-%m-%d"), tag, suffix))

    # Replace any awkward characters
    full_path = full_path.replace(":","_")

    if on_hdfs:
        if use_webhdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=WebHdfsPlainFormat(use_gzip=use_gzip))
        else:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=luigi.contrib.hdfs.PlainFormat())
    else:
        return luigi.LocalTarget(path=full_path)


# --------------------------------------------------------------------------
# These helpers help set up database targets for fine-grained task outputs
# --------------------------------------------------------------------------

class CopyToTableInDB(CopyToTable):
    """
    Abstract class that fixes which tables are used
    """
    host = 'access'
    database = 'access_task_state'
    user = 'access'
    password = 'access'

    def output(self):
        """
        Returns a PostgresTarget representing the inserted dataset.
        """
        return taskdb_target(self.table,self.update_id)


class CopyToTableInIngestDB(CopyToTable):
    """
    Abstract class that fixes which tables are used
    """
    host = 'ingest'
    database = 'ingest_task_state'
    user = 'ingest'
    password = 'ingest'

    def output(self):
        """
        Returns a PostgresTarget representing the inserted dataset.
        """
        return taskdb_target(self.table,self.update_id, kind='ingest')

