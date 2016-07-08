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

print(dict(cfg.items("h3")))

HERITRIX_ROOT="/opt/heritrix"
HERITRIX_JOBS="%s/jobs" % HERITRIX_ROOT

# Basic configuration:
app = Celery('crawl',
             broker='amqp://',
# This is only needed if we consume the results of the calls
#             backend='rpc://',
             include=['crawl.job'])

# Optional configuration, see the application user guide.
app.conf.update(
    # Expiry date for results (but we're not keeping them at all right now):
    CELERY_TASK_RESULT_EXPIRES=3600,

    # ACK only if the task succeeds:
    CELERY_ACKS_LATE=True,

    # Enable publisher confirms so we know messages got set:
    BROKER_TRANSPORT_OPTIONS = {
        'block_for_ack': True
    },

    # Keep trying when sending messages (unnecessary?):
    CELERY_TASK_PUBLISH_RETRY_POLICY = {
        'max_retries': None,
    },

    # Custom queue configuration:
    CELERY_QUEUES = {
        "default": {
            "exchange": "default",
            "binding_key": "default"},
        "crawl.job.add": {
            "exchange": "default",
            "binding_key": "crawl.job.add",
        },
        "crawl.job.mul": {
            "exchange": "default",
            "binding_key": "crawl.job.mul",
        }
    },
    CELERY_DEFAULT_QUEUE = "default",
    CELERY_DEFAULT_EXCHANGE_TYPE = "direct",
    CELERY_DEFAULT_ROUTING_KEY = "default",

    # Mapping from tasks to queues:
    CELERY_ROUTES = {
        'crawl.job.add': {
            'queue' : 'crawl.job.add',
        },
        'crawl.job.mul': {
            'queue': 'crawl.job.mul',
        },
    },
)

if __name__ == '__main__':
    app.start()

