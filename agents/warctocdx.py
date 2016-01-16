#!/usr/bin/env python

"""Watches the queue of unindexed warcs and indexes them."""

import os
import re
import sys
import json
import shutil
import logging
import requests
from shared import amqp;

AMQP_URL = os.environ['AMQP_URL']
QUEUE_NAME = os.environ['QUEUE_NAME']
DUMMY = os.environ['DUMMY_RUN']

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( "warctocdx" )
logger.setLevel( logging.INFO )


class warctocdx(amqp.QueueConsumer):
	def callback(self, ch, method, properties, body ):
		"""Passed a WARC file path, launch a job to extract the CDX file."""
		try:
			logger.debug( "Message received: %s." % body )
			cl = json.loads(body)
			warc = cl["warc"]

			if False:
				ch.basic_ack(delivery_tag = method.delivery_tag)
			else:
				logger.error("Failed with %s" % "FAIL")	

		except Exception as e:
			logger.error( "%s [%s]" % ( str( e ), body ) )
			logging.exception(e)

if __name__ == "__main__":
	t = warctocdx(AMQP_URL, QUEUE_NAME, "post-crawl","pc010-warcs-to-index")
	t.begin()

