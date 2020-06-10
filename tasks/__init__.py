"""
Luigi task definitions for UK Web Archive processes.

Here we declare our task handlers to make sure they are always used.

"""

import os
import luigi
from prometheus_client import CollectorRegistry, push_to_gateway, Gauge
from tasks.metrics import record_task_outcome


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
    record_task_outcome(registry, task, 0, luigi.Event.FAILURE)

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
    record_task_outcome(registry, task, 1, luigi.Event.SUCCESS)

    # POST to Prometheus Push Gateway:
    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)
    else:
        logger.error("No metrics gateway configured!")


@luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
def record_processing_time(task, processing_time):
    """Record the processing time of every task."""
    logger.info("Got %s processing time %s" % (task.task_namespace, str(processing_time)))

    # Where to store the metrics:
    registry = CollectorRegistry()

    # Disable pylint warnings due to it not picking up decorators:
    # pylint: disable=E1101,E1120,E1123,E1124
    
    # Generate metrics:
    g = Gauge('ukwa_task_processing_time',
              'Processing time of a task, in seconds.',
               labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set(processing_time)

    # POST to Prometheus Push Gateway:
    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)
    else:
        logger.error("No metrics gateway configured!")
