#
# Helper class for managing task events in the TrackDB
#
import datetime

class Task():

    def __init__(self, event_name, event_date=datetime.date.today(), params={}):
        self.started_at = None
        self.finished_at = None
        param_list = ':'.join(map('='.join, params.items()))
        self.event = {
            'id': 'task:%s:%s:%s'% (event_name, event_date.isoformat(), param_list),
            'kind_s': 'tasks'
        }

    def start(self):
        self.started_at = datetime.datetime.now()
        self.event['started_at_dt'] = self.started_at.isoformat()

    def finish(self, status="success"):
        self.finished_at = datetime.datetime.now()    
        self.event['finished_at_dt'] = self.finished_at.isoformat()
        self.event['task_status_s'] = status
        if self.started_at:
            self.event['runtime_secs_i'] = (self.finished_at-self.started_at).total_seconds()

    def add(self, dict_to_add):
        for key in dict_to_add:
            self.event[key] = dict_to_add[key]

    def as_dict(self):
        return self.event
        