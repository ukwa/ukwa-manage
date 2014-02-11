#!/usr/bin/env python

import sys
import pika
import logging
from sipd import settings

logging.basicConfig()

connection = pika.BlockingConnection( pika.ConnectionParameters( settings.SIP_QUEUE_HOST ) )
channel = connection.channel()
channel.queue_declare( queue=settings.SIP_QUEUE_NAME, durable=True )
channel.basic_publish( exchange="",
	routing_key=settings.SIP_QUEUE_KEY,
	properties=pika.BasicProperties(
		delivery_mode=2,
	),
	body=sys.argv[ 1 ]
)
connection.close()

