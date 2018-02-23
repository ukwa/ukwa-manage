import luigi
import logging
from prometheus_client import CollectorRegistry, Gauge

# --------------------------------------------------------------------------
# Metrics collection
# --------------------------------------------------------------------------

logger = logging.getLogger('luigi-interface')


def record_task_outcome(registry, task, value):
    # type: (CollectorRegistry, luigi.Task, int) -> None

    g = Gauge('ukwa_task_event_timestamp',
              'Timestamp of this task event.',
              labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set_to_current_time()

    g = Gauge('ukwa_task_status',
              'Record a 1 if a task ran, 0 if a task failed.',
               labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set(value)

    # Task-specific metrics:
    try:
        # Any task wishing to store metrics should implement this method and update the registry:
        task.get_metrics(registry)
    except AttributeError:
        logger.info("No get_metrics method found on %s" % task)

