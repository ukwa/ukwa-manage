#!/usr/bin/env python
"""
Watch the uris-to-check queue and report how long it took them to turn up in Wayback
Logging:
	critical - logged when problem occurs that is unexpected/shouldn't happen/crash etc. and script exits
	error	- logged for feasible issues but script continues, in which case the amqp message is rejected
	info	- standard output for data to be tracked in kibana/elk
	debug	- only for the benefit of debugging
"""

#### python libraries
import os
import sys
import logging
import argparse
import pika
import time
import json
import requests
from requests.utils import quote
import xml.dom.minidom
from datetime import datetime

#### wa libraries
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))

# globals ----------------------------------
sleep = 10

# functions --------------------------------
def configure_logging():
	# set output format
	formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s')
	handler = logging.StreamHandler()
	handler.setFormatter(formatter)

	# replace root logger?
	logging.root.handlers = []
	logging.root.addHandler(handler)
	logging.root.setLevel(logging.INFO)

	# set default logging output for imported libraries
	logging.getLogger('requests').setLevel(logging.WARNING)
	logging.getLogger('pika').setLevel(logging.WARNING)

	return logging.getLogger(__name__)

def get_args():
	parser = argparse.ArgumentParser('Get documents from AMQP queue, ')
	parser.add_argument('--amqp-url', '-a',
		dest='amqp_url',
		type=str,
		help='AMQP endpoint to use, eg [amqp://(user):(pwd}@(amqp_hostname):5672/%%2f]')
	parser.add_argument(
		'--routing-key', '-r',
		dest='routing_key',
		type=str, default='FC-1-uris-to-check', 
		help='AMQP routing key for queue [default: %(default)s]')
	parser.add_argument(
		'--num', '-n',
		dest='qos_num',
		type=int,
		default=10, 
		help='Maximum number of messages to handle at once [default: %(default)s]')
	parser.add_argument(
		'--wb-url', '-W',
		dest='wb_url',
		type=str,
		help='Wayback endpoint to check URL availability, eg [http://(wayback_hostname):8080/wayback]' )
	parser.add_argument(
		'exchange',
		help='Name of the exchange to use')
	parser.add_argument(
		'queue',
		help='Name of queue to view messages from')

	args = parser.parse_args()
	logger.debug('args: %s' % args)
	return args

def check_message(ch, method, properties, body):
	# check amqp message includes url and launch_timestamp
	try: 
		amqp_msg = json.loads(body)
		msg_url = amqp_msg['launch_message']['url']
		dt_msg_launchtimestamp = datetime.strptime(amqp_msg['launch_timestamp'][:19], '%Y-%m-%dT%H:%M:%S')
		if (not msg_url or not dt_msg_launchtimestamp):
			logger.error('Missing required message url: %s\tlaunch_timestamp: %s' %
				(msg_url, str(dt_msg_launchtimestamp)))
			ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
			return
	except Exception as e:
		logger.error('Exception whilst consuming %s messages: %s' % (str(args.amqp_url), str(e)))
		ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
		return

	# skip non http(s) records
	try:
		if not msg_url[:4] == 'http':
			logger.error('Skipping non http(s) message %s' % msg_url)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		if not isinstance(dt_msg_launchtimestamp, datetime):
			logger.error('Skipping non datetime values in message %s' % msg_url)
			ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
			return
	except Exception as e:
		logger.error('Exception with message url %s or launch_timestamp %s: %s' % (msg_url, str(e)))
		ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
		return

	# try to get url from wayback
	try:
                wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (args.wb_url, quote(msg_url))
                logger.debug("Checking %s" % wburl);
		wbreq = requests.get(wburl)
		if not isinstance(wbreq.status_code, int):
			logger.error('url %s status code not integer' % wburl)
			ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
			return
	except Exception as e:
		logger.error('Failed to get wayback url %s' % wburl)
		ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
		return

	# search for wayback timestamp > launch_timestamp
	most_recent_instance = None
	if wbreq.status_code == 200:
		wbreq_dom = xml.dom.minidom.parseString(wbreq.text)
		instance_capturedates = []
		for inst_date in wbreq_dom.getElementsByTagName('capturedate'):
			instance_capturedates.append(inst_date.firstChild.nodeValue)
		for inst_date in sorted(instance_capturedates, key=int):
			logger.debug('launch date: %s instance capture date: %s' % (dt_msg_launchtimestamp, inst_date))
			most_recent_instance = inst_date
			dt_inst_date = datetime.strptime(inst_date, '%Y%m%d%H%M%S')
			if dt_inst_date >= dt_msg_launchtimestamp:
				logger.info('instance of %s queued %s captured at %s - duration %s seconds' %
					(msg_url,
					datetime.strftime(dt_msg_launchtimestamp, '%Y-%m-%d %H:%M:%S'),
					datetime.strftime(dt_inst_date, '%Y-%m-%d %H:%M:%S'),
					int((dt_inst_date - dt_msg_launchtimestamp).total_seconds())))
				ch.basic_ack(delivery_tag = method.delivery_tag)
				return
	else:
		# reject amqp message and requeue for future test
		logger.debug('%s status for %s' % (wbreq.status_code, args.amqp_url))
		ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
		return

	logger.info('launch %s of url %s not yet in wayback (%s)' % (dt_msg_launchtimestamp,msg_url, most_recent_instance))
	ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)

# main -------------------------------------
if __name__ == '__main__':
	try:
		# configure logging
		logger = configure_logging()

		# get command arguments
		args = get_args()

		# create AMQP handler
		try:
			# amqp configuration
			parameters = pika.URLParameters(args.amqp_url)
			connection = pika.BlockingConnection(parameters)
			amqp = connection.channel()
			amqp.exchange_declare(exchange=args.exchange, durable=True)
			amqp.queue_declare(queue=args.queue, durable=True)
			amqp.queue_bind(queue=args.queue, exchange=args.exchange, routing_key=args.routing_key)
			amqp.basic_qos(prefetch_count=args.qos_num)
			# proclaim function and settings to consume amqp messages with
			amqp.basic_consume(check_message, queue=args.queue, no_ack=False)
		except Exception as e:
			logger.critical('Failed to create %s AMQP handler: %s' % (args.amqp_url, str(e)))
			logger.exception(e)
			time.sleep(sleep)
			sys.exit(2)
		else:
			logger.debug('Created amqp handle %s' % amqp)

		# watch AMQP messages
		# this starts the consuming of AMQP messages, but as the 'check_message' function 
		# processes each message, exception handling to coded there
		amqp.start_consuming()

	except Exception as e:
		logger.critical('Unknown error: %s' % str(e))
		logger.exception(e)
		time.sleep(sleep)
		sys.exit(1)
