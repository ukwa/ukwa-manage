import luigi
import logging
from prometheus_client import CollectorRegistry, Gauge
from prometheus_client.core import _floatToGoString

# --------------------------------------------------------------------------
# Metrics collection
# --------------------------------------------------------------------------

logger = logging.getLogger('luigi-interface')


def record_task_outcome(registry, task, value, event_status):
    # type: (CollectorRegistry, luigi.Task, int) -> None

    g = Gauge('ukwa_task_event_timestamp',
              'Timestamp of this task event, labelled by task status.',
              labelnames=['task_namespace','status'], registry=registry)
    g.labels(task_namespace=task.task_namespace, status=event_status).set_to_current_time()

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


def generate_latest_with_timestamps(registry, timestamp):
    '''Returns the metrics from the registry in latest text format as a string. Based on the original
    prometheus_client generate_latest but adding support for setting the timestamp.'''
    output = []
    for metric in registry.collect():
        output.append('# HELP {0} {1}'.format(
            metric.name, metric.documentation.replace('\\', r'\\').replace('\n', r'\n')))
        output.append('\n# TYPE {0} {1}\n'.format(metric.name, metric.type))
        for name, labels, value in metric.samples:
            if labels:
                labelstr = '{{{0}}}'.format(','.join(
                    ['{0}="{1}"'.format(
                     k, v.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"'))
                     for k, v in sorted(labels.items())]))
            else:
                labelstr = ''
            output.append('{0}{1} {2} {3}\n'.format(name, labelstr, _floatToGoString(value), timestamp))
    return ''.join(output).encode('utf-8')