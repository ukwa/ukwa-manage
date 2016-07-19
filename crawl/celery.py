from __future__ import absolute_import

import os
import ConfigParser
from celery import Celery

# App configuration, defaults first:
default_cfg = os.path.join(os.path.realpath(os.path.dirname(__file__)), "default.cfg")
cfg = ConfigParser.ConfigParser()
cfg.readfp(open(default_cfg))
# Optional overrides from a file specificed by an environment variable:
if "SHEPHERD_CONFIG" in os.environ:
    cfg.read([os.environ["SHEPHERD_CONFIG"]])

#HERITRIX_ROOT="/opt/heritrix"
#HERITRIX_JOBS="%s/jobs" % HERITRIX_ROOT
HERITRIX_ROOT="/Users/andy/Documents/workspace/wren/compose-pulse-crawler"
HERITRIX_JOBS="/Users/andy/Documents/workspace/wren/compose-pulse-crawler/jobs"
HERITRIX_HDFS_ROOT="/heritrix"

# Basic configuration:
app = Celery('crawl',
             broker='amqp://%s' % cfg.get('amqp', 'host'),
# This is only needed if we consume the results of the calls
#             backend='rpc://',
             include=['crawl.tasks', 'crawl.status'])

# Optional configuration, see the application user guide.
app.conf.update(
    # Ignore results and errors so no backend is needed:
    CELERY_IGNORE_RESULT=True,
    CELERY_STORE_ERRORS_EVEN_IF_IGNORED=False,

    # Expiry date for results (but we're not keeping them at all right now):
    CELERY_TASK_RESULT_EXPIRES=3600,

    # ACK only if the task succeeds:
    CELERY_ACKS_LATE=True,

    # Enable publisher confirms so we know messages got set:
    BROKER_TRANSPORT_OPTIONS = {
        'block_for_ack': True
    },

    # Custom queue configuration (so queues can be inspected manually):
    CELERY_QUEUES = {
        "default": {
            "exchange": "default",
            "binding_key": "default"},
        # "crawl.task.add": {
        #     "exchange": "default",
        #     "binding_key": "crawl.task.add",
        # },
        # "crawl.task.mul": {
        #     "exchange": "default",
        #     "binding_key": "crawl.task.mul",
        # }
    },
    # Mapping from tasks to queues:
    CELERY_ROUTES = {
        # 'crawl.task.add': {
        #     'queue' : 'crawl.task.add',
        # },
        # 'crawl.task.mul': {
        #     'queue': 'crawl.task.mul',
        # },
    },
    # Default routing:
    CELERY_DEFAULT_QUEUE="default",
    CELERY_DEFAULT_EXCHANGE_TYPE="direct",
    CELERY_DEFAULT_ROUTING_KEY="default",

    # Use JSON so we can inspect queue contents manually
    CELERY_TASK_SERIALIZER = "json"
)

if __name__ == '__main__':
    app.start()


