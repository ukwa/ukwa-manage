#
# Helper class for managing task events in the TrackDB
#
import os
import json
import logging
import datetime
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = logging.getLogger(__name__)

class Task():

    def __init__(self, event_name, event_date=datetime.date.today(), params={}):
        self.event_name = event_name
        self.started_at = None
        self.finished_at = None
        param_list = ':'.join(map('='.join, params.items()))
        self.event = {
            'id': 'task:%s:%s:%s'% (event_name, event_date.isoformat(), param_list),
            'kind_s': 'tasks'
        }
        self.status = 'new'

    def start(self):
        self.started_at = datetime.datetime.now()
        self.event['started_at_dt'] = "%sZ" % self.started_at.isoformat()
        self.status = 'started'

    def finish(self, status="success"):
        self.finished_at = datetime.datetime.now()    
        self.event['finished_at_dt'] = "%sZ" % self.finished_at.isoformat()
        self.status = status
        self.event['task_status_s'] = status
        if self.started_at:
            self.event['runtime_secs_i'] = (self.finished_at-self.started_at).total_seconds()

    def add(self, dict_to_add):
        for key in dict_to_add:
            self.event[key] = dict_to_add[key]

    def as_dict(self):
        return self.event

    def to_jsonline(self):
        return json.dumps(self.as_dict())

    def push_metrics(self, stat_keys=[]):
        registry = CollectorRegistry()

        # Record the task completion timestamp, good for checking all is well:
        g = Gauge('ukwa_task_event_timestamp',
                    'Timestamp of this task event, labelled by task status.',
                    labelnames=['task_namespace','status'], registry=registry)
        g.labels(task_namespace=self.event_name, status=self.status).set_to_current_time()

        # For every key passed in above, pick it out of the event stats and add it:
        for key in stat_keys:
            if key in self.event:
                g = Gauge(f"ukwa_task_{key}",
                            'Outcome of this task, labelled by task status.',
                            labelnames=['task_namespace','status'], registry=registry)
                g.labels(task_namespace=self.event_name, status=self.status).set(self.event[key])
            else:
                logger.warning(f"Could not find metrics key '{key}'")

        if os.environ.get("PUSH_GATEWAY"):
            push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=self.event_name, registry=registry)
        else:
            logger.error("No metrics PUSH_GATEWAY configured!")
        