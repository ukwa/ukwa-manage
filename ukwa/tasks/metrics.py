import luigi
from prometheus_client import CollectorRegistry, Gauge
from ukwa.tasks.backup.postgresql import BackupProductionW3ACTPostgres

# --------------------------------------------------------------------------
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


if __name__ == '__main__':
    # An example task to test with:
    task = BackupProductionW3ACTPostgres()
    #celebrate_any_success(task)
