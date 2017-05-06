'''
Created on 28 Jan 2016

For the HAR daemon, we use:

{
    "clientId": "FC-3-uris-to-crawl",
    "metadata": {
        "heritableData": {
            "heritable": [
                "source",
                "heritable"
            ],
            "source": "http://acid.matkelly.com/"
        },
        "pathFromSeed": ""
    },
    "isSeed": true,
    "url": "http://acid.matkelly.com/"
}

@author: andy
'''

import pika
import json
from datetime import datetime
import logging

logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )

class launcher(object):
	'''
	classdocs
	'''


	def __init__(self, args):
		'''
		Constructor
		'''
		self.args = args
		

	def send_message(self, message, queue=None):
		"""
		Sends a message to the given queue.
		"""
		#
		if not queue:
			queue = self.args.queue
		#
		parameters = pika.URLParameters(self.args.amqp_url)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.exchange_declare(exchange=self.args.exchange, durable=True)
		channel.queue_declare( queue=queue, durable=True, auto_delete=False )
		channel.queue_bind(queue=queue, exchange=self.args.exchange)#, routing_key="uris-to-render")
		channel.tx_select()
		channel.basic_publish( exchange=self.args.exchange,
			routing_key=queue,
			properties=pika.BasicProperties(
				delivery_mode=2,
			),
			body=message )
		channel.tx_commit()
		connection.close()


	def launch(self, destination, uri, source, isSeed=False, clientId="FC-3-uris-to-crawl", forceFetch=False, sendCheckMessage=True):
		curim = {}
		if destination == "h3":
			curim['headers'] = {}
			#curim['headers']['User-Agent'] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.120 Chrome/37.0.2062.120 Safari/537.36"
			curim['method']= "GET"
			curim['parentUrl'] = uri
			curim['parentUrlMetadata'] = {}
			curim['parentUrlMetadata']['pathFromSeed'] = ""
			curim['parentUrlMetadata']['heritableData'] = {}
			curim['parentUrlMetadata']['heritableData']['source'] = source
			curim['parentUrlMetadata']['heritableData']['heritable'] = ['source','heritable']
			curim['isSeed'] = isSeed
			if not isSeed:
				curim['forceFetch'] = forceFetch
			curim['url'] = uri
		elif destination == "har":
			curim['clientId']= clientId
			curim['metadata'] = {}
			curim['metadata']['heritableData'] = {}
			curim['metadata']['heritableData']['heritable'] = ['source','heritable']
			curim['metadata']['heritableData']['source'] = source
			curim['metadata']['pathFromSeed'] = ""
			curim['isSeed'] = isSeed
			if not isSeed:
				curim['forceFetch'] = forceFetch
			curim['url'] = uri
		else:
			logger.error("Can't handle destination type '%s'" % destination )
		message = json.dumps(curim)
		logger.debug("Sending message: "+message)

		# Push a 'seed' message onto the rendering queue:
		self.send_message(message)
		# Also push the same message to the FC-1-uris-to-check
		if sendCheckMessage:
			check_message = {}
			check_message['launch_timestamp'] = datetime.utcnow().isoformat()
			check_message['launch_message'] = curim
			check_message['launch_queue'] = self.args.queue
			self.send_message(json.dumps(check_message),'FC-1-uris-to-check')
