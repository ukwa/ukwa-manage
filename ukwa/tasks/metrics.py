import luigi
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from ukwa.tasks.backup.postgresql import BackupProductionW3ACTPostgres
from ukwa.tasks.settings import systems

# --------------------------------------------------------------------------
# This file contains event handlers for Luigi that should get automatically
# picked up and record the outcome of any task.
#
# Metrics definitions:
# --------------------------------------------------------------------------


def record_task_outcome(registry, task, value):
    # type: (CollectorRegistry, luigi.Task, int) -> None
    g = Gauge('ukwa_task_event_timestamp', 'Timestamp of this task event.',
              labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set_to_current_time()
    g2 = Gauge('ukwa_task_status', 'Record a 1 if a task ran, 0 if a task failed.',
               labelnames=['task_namespace'], registry=registry)
    g2.labels(task_namespace=task.task_namespace).set(value)


def record_db_backup_metrics(registry, task):
    # type: (CollectorRegistry, BackupProductionW3ACTPostgres) -> None
    g = Gauge('ukwa_task_database_backup_size_bytes', 'Size of a database backup.',
              labelnames=['db'], registry=registry)
    g.labels(db='w3act').set(task.get_backup_size())


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
    registry = CollectorRegistry()
    record_task_outcome(registry, task, 0)
    push_to_gateway(systems().prometheus_push_gateway, job=task.get_task_family(), registry=registry)


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_any_success(task):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """

    # Where to store the metrics:
    registry = CollectorRegistry()

    # Generic metrics:
    record_task_outcome(registry, task, 1)

    # Task-specific metrics:
    if isinstance(task, BackupProductionW3ACTPostgres):
        record_db_backup_metrics(registry,task)

    # POST to prometheus:
    push_to_gateway(systems().prometheus_push_gateway, job=task.get_task_family(), registry=registry)


if __name__ == '__main__':
    # An example task to test with:
    task = BackupProductionW3ACTPostgres()
    celebrate_any_success(task)
