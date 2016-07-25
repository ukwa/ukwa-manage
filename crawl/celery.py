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

# Pick up folder locations from config:
HERITRIX_ROOT=cfg.get('h3','local_root_folder')
HERITRIX_JOBS=cfg.get('h3','local_job_folder')
HERITRIX_HDFS_ROOT=cfg.get('h3','hdfs_root_folder')

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
            "binding_key": "default"
        },
        # "RC1-restart-job": {
        #     "exchange": "default",
        #     "binding_key": "crawl.tasks.restart_job",
        # },
        # "RC2-validate-job": {
        #     "exchange": "default",
        #     "binding_key": "crawl.tasks.validate_job",
        # }
        "RC10-uri-to-index": {
            "exchange": "default",
            "binding_key": "crawl.tasks.uri_to_index",
        },
        "RC11-uri-to-doc": {
            "exchange": "default",
            "binding_key": "crawl.tasks.uri_of_doc",
        }
    },
    # Mapping from tasks to queues:
    CELERY_ROUTES = {
        # 'crawl.tasks.restart_job': {
        #     'queue' : 'RC1-restart-job',
        # },
        # 'crawl.tasks.validate_job': {
        #     'queue': 'RC2-validate-job',
        # },
        'crawl.tasks.uri_to_index': {
            'queue': 'RC10-uri-to-index',
        },
        'crawl.tasks.uri_of_doc': {
            'queue': 'RC11-uri-to-doc',
        },
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


