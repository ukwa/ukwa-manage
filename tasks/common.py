import os
import logging
import posixpath
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from luigi.contrib.postgres import PostgresTarget, CopyToTable
from tasks.hdfs.webhdfs import WebHdfsPlainFormat
from prometheus_client import CollectorRegistry, push_to_gateway
from metrics import record_task_outcome

luigi.interface.setup_interface_logging()

LOCAL_STATE_FOLDER = os.environ.get('LOCAL_STATE_FOLDER', '/var/task-state')
LOCAL_REPORT_FOLDER = os.environ.get('LOCAL_REPORT_FOLDER', '/data/ukwa-reports')
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

    if on_hdfs:
        if use_webhdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=WebHdfsPlainFormat(use_gzip=use_gzip))
        else:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=luigi.contrib.hdfs.PlainFormat())
    else:
        return luigi.LocalTarget(path=full_path)


def report_file(date, tag, suffix):
    report_folder = LOCAL_REPORT_FOLDER
    # build the full path:
    pather = os.path
    if date:
        full_path = pather.join( str(report_folder),
                         date.strftime("%Y-%m"),
                         tag,
                         '%s-%s' % (date.strftime("%Y-%m-%d"), suffix))
    else:
        full_path = pather.join( str(report_folder), tag, suffix)

    return luigi.LocalTarget(path=full_path)


# --------------------------------------------------------------------------
# These helpers help set up database targets for fine-grained task outputs
# --------------------------------------------------------------------------

def taskdb_target(task_group, task_result):
    # Set the task group and ID:
    target = PostgresTarget(
            host='access',
            database='access_task_state',
            user='access',
            password='access',
            table=task_group,
            update_id=task_result
        )
    # Set the actual DB table to use:
    target.marker_table = "ingest_task_state"

    return target


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



# --------------------------------------------------------------------------
# This general handler reports task failure and success, for each task
# family (class name) and namespace.
#
# For some specific classes, additional metrics are computed.
# --------------------------------------------------------------------------


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_any_failure(task, exception):
    # type: (luigi.Task) -> None
    """
       Will be called directly after a successful execution
       and is used to update any relevant metrics
    """

    # Where to store the metrics:
    registry = CollectorRegistry()

    # Generate metrics:
    record_task_outcome(registry, task, 0)

    # POST to Prometheus Push Gateway:
    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)
    else:
        logger.error("No metrics gateway configured!")


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_any_success(task):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """

    # Where to store the metrics:
    registry = CollectorRegistry()

    # Generate metrics:
    record_task_outcome(registry, task, 1)

    # POST to Prometheus Push Gateway:
    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)
    else:
        logger.error("No metrics gateway configured!")
