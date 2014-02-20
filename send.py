#!/usr/bin/env python

import sys
import pika
import logging
from harchiverd import settings

logging.basicConfig()

connection = pika.BlockingConnection( pika.ConnectionParameters( settings.HAR_QUEUE_HOST ) )
channel = connection.channel()
channel.queue_declare( queue=settings.HAR_QUEUE_NAME, durable=True )
channel.basic_publish( exchange="",
	routing_key=settings.HAR_QUEUE_KEY,
	properties=pika.BasicProperties(
		delivery_mode=2,
	),
	body=sys.argv[ 1 ]
)
connection.close()

