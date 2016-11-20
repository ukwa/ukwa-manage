#!/usr/bin/env python

"""
Daemon intended to monitor a queue for "Archive this now" submissions from
w3act.
"""

import os
import pika
import json
import shutil
import logging
import heritrix
import requests
from lxml import html
from w3act import settings
from daemonize import Daemon
from w3act.job import W3actJob


logger = logging.getLogger("w3act.%s" % __name__)
handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

#Try to set logging output for all modules.
logging.getLogger("").setLevel(logging.WARNING)
logging.getLogger("").addHandler(handler)


def send_message(host, queue, key, message):
    """Sends a message to RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.tx_select()
    channel.basic_publish(exchange="",
        routing_key=key,
        properties=pika.BasicProperties(
            delivery_mode=2,
      ),
        body=message)
    channel.tx_commit()
    connection.close()


def send_error_message(message):
    """Sends a message to the 'w3actor-error' queue."""
    send_message(
        settings.QUEUE_HOST,
        settings.JOB_ERROR_QUEUE_NAME,
        settings.JOB_ERROR_QUEUE_KEY,
        message
    )


def callback(ch, method, properties, body):
    try:
        logger.info("Message received: %s" % body)
        target = json.loads(body)

        api = heritrix.API(host="https://opera.bl.uk:8443/engine", user="admin", passwd="bl_uk", verbose=False, verify=False)
        job = W3actJob([target], heritrix=api)
        job.start()
    except Exception as e:
        logger.error("%s [%s]" % (str(e), body))
        send_error_message("%s|%s" % (body, str(e)))


class JobDaemon(Daemon):
    """Maintains a connection to the queue."""
    def run(self):
        while True:
            try:
                logger.debug("Starting connection %s:%s." % (settings.QUEUE_HOST, settings.JOB_QUEUE_NAME))
                connection = pika.BlockingConnection(pika.ConnectionParameters(settings.QUEUE_HOST))
                channel = connection.channel()
                channel.queue_declare(queue=settings.JOB_QUEUE_NAME, durable=True)
                channel.basic_consume(callback, queue=settings.JOB_QUEUE_NAME, no_ack=True)
                channel.start_consuming()
            except Exception as e:
                logger.error(str(e))

