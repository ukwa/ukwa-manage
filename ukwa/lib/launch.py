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

import json
from kafka import KafkaProducer
from datetime import datetime
from urlparse import urlparse
import logging

logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )


class KafkaLauncher(object):
    '''
    classdocs
    '''

    def __init__(self, args):
        '''
        Constructor
        '''
        self.args = args
        self.producer = KafkaProducer(
            bootstrap_servers=self.args.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_message(self, key, message, queue=None):
        """
        Sends a message to the given queue.
        """
        #
        if not queue:
            queue = self.args.queue

        logger.info("Sending message: " + json.dumps(message))
        self.producer.send(queue, key=key, value=message)

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
            curim['hop'] = ""
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

        # Determine the key
        key = urlparse(uri).hostname

        # Push a 'seed' message onto the rendering queue:
        self.send_message(key, curim)
        # Also push the same message to the FC-1-uris-to-check
        if sendCheckMessage:
            check_message = {}
            check_message['launch_timestamp'] = datetime.utcnow().isoformat()
            check_message['launch_message'] = curim
            check_message['launch_queue'] = self.args.queue
            self.send_message(check_message,'FC-1-uris-to-check')

    def flush(self):
        self.producer.flush()