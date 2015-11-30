#!/usr/bin/env python

"""
Various SIPs are stored in gzipped tarfiles in HDFS. This script will pull
a single message from a queue, verify that the corresponding SIP exists in
HDFS, retrieve it, parse the METS file for ARK identifiers and verify that
each ARK is available in DLS. If so, the corresponding SIP is flagged for 
indexing.
"""

import os
import sys
import pika
import logging
import dateutil.parser
from sipverify import *
from datetime import datetime, timedelta

QUEUE_HOST="194.66.232.93"
SIP_QUEUE="sip-submitted"
ERROR_QUEUE="sip-error"
INDEX_QUEUE="index"
DLS_LOOKUP="/heritrix/sips/dls-export/Public Web Archive Access Export.txt"
SELECTIVE_PREFIX="f6ivc2"

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT)
logger = logging.getLogger("verify-sips")
logger.setLevel(logging.INFO)
logging.getLogger("pika").setLevel(logging.ERROR)

def get_available_arks():
	"""Reads relevant ARKs from a text file."""
	with open(DLS_LOOKUP, "rb") as i:
		return [l.split()[0] for l in i if not l.startswith(SELECTIVE_PREFIX)]

def outside_embargo(message):
	"""Checks whether a message is outside the week embargo."""
	timestamp = message.split("/")[1]
	date = dateutil.parser.parse(timestamp)
	return date < (datetime.now() - timedelta(days=7))

if __name__ == "__main__":
	if not os.path.exists(DLS_LOOKUP) and os.access(DLS_LOOKUP, os.R_OK):
		logger.error("Cannot read DLS lookup: %s" % DLS_LOOKUP)
		sys.exit(1)

	dls_arks = get_available_arks()
	if len(dls_arks) == 0:
		logger.error("No DLS ARKs found!")
		sys.exit(1)

	seen = []
	message = get_message(QUEUE_HOST, SIP_QUEUE)
	while message is not None and message not in seen:
		message = message.strip()
		logger.debug("Received message: %s" % message)
		if isvalid(message):
			if outside_embargo(message):
				arks = get_identifiers(message)
				if len(arks) > 0:
					all_arks_available = True
					for ark in arks:
						all_arks_available = (all_arks_available and (ark in dls_arks))
					if all_arks_available:
						put_message(QUEUE_HOST, INDEX_QUEUE, message)
						logger.debug("OK to index: %s" % message)
					else:
						seen.append(message)
						put_message(QUEUE_HOST, SIP_QUEUE, message)
						logger.info("ARKs not available: %s" % message)
				else:
					logger.warning("No ARKs found for %s" % message)
			else:
				seen.append(message)
				put_message(QUEUE_HOST, SIP_QUEUE, message)
				logger.info("Inside embargo period: %s" % message)
		else:
			put_message(QUEUE_HOST, ERROR_QUEUE, "%s|%s" % (message, "Invalid message."))
		message = get_message(QUEUE_HOST, SIP_QUEUE)
	if message is not None:
		put_message(QUEUE_HOST, SIP_QUEUE, message)

