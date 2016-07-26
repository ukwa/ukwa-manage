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
        'PULSE-01-stop-start-job': {
            "exchange": "default",
            "binding_key": "crawl.tasks.stop_start_job",
        },
        "PULSE-02-assemble-job-output": {
            "exchange": "default",
            "binding_key": "crawl.tasks.assemble_job_output",
        },
        "PULSE-03-build-sip": {
            "exchange": "default",
            "binding_key": "crawl.tasks.build_sip",
        },
        "PULSE-04-submit-sip": {
            "exchange": "default",
            "binding_key": "crawl.tasks.submit_sip",
        },
        "PULSE-05-verify-sip": {
            "exchange": "default",
            "binding_key": "crawl.tasks.verify_sip",
        },
        "PULSE-10-uri-to-index": {
            "exchange": "default",
            "binding_key": "crawl.tasks.uri_to_index",
        },
        "PULSE-11-uri-to-doc": {
            "exchange": "default",
            "binding_key": "crawl.tasks.uri_of_doc",
        },
        "STATUS-01-crawl-state-updates": {
            "exchange": "default",
            "binding_key": "crawl.status.update_job_status",
        }
    },
    # Mapping from tasks to queues:
    CELERY_ROUTES = {
        'crawl.tasks.stop_start_job': {
             'queue' : 'PULSE-01-stop-start-job',
        },
        'crawl.tasks.assemble_job_output': {
             'queue': 'PULSE-02-assemble-job-output',
        },
        'crawl.tasks.build_sip': {
             'queue': 'PULSE-03-build-sip',
        },
        'crawl.tasks.submit_sip': {
             'queue': 'PULSE-04-submit-sip',
        },
        'crawl.tasks.verify_sip': {
             'queue': 'PULSE-05-verify-sip',
        },
        'crawl.tasks.uri_to_index': {
            'queue': 'PULSE-10-uri-to-index',
        },
        'crawl.tasks.uri_of_doc': {
            'queue': 'PULSE-11-uri-to-doc',
        },
        'crawl.status.update_job_status': {
            'queue': 'STATUS-01-crawl-state-updates',
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


