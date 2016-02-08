#!/usr/bin/env python

"""Watched the crawled documents log queue and passes entries to w3act

Input:

{
    "annotations": "ip:173.236.225.186,duplicate:digest",
    "content_digest": "sha1:44KA4PQA5TYRAXDIVJIAFD72RN55OQHJ",
    "content_length": 324,
    "extra_info": {},
    "hop_path": "IE",
    "host": "acid.matkelly.com",
    "jobName": "frequent",
    "mimetype": "text/html",
    "seed": "WTID:12321444",
    "size": 511,
    "start_time_plus_duration": "20160127211938966+230",
    "status_code": 404,
    "thread": 189,
    "timestamp": "2016-01-27T21:19:39.200Z",
    "url": "http://acid.matkelly.com/img.png",
    "via": "http://acid.matkelly.com/",
    "warc_filename": "BL-20160127211918391-00001-35~ce37d8d00c1f~8443.warc.gz",
    "warc_offset": 36748
}

Note that 'seed' is actually the source tag, and is set up to contain the original (Watched) Target ID.

Output:

[
{
"id_watched_target":<long>,
"wayback_timestamp":<String>,
"landing_page_url":<String>,
"document_url":<String>,
"filename":<String>,
"size":<long>
},
<further documents>
]

See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint

i.e. 

seed -> id_watched_target
start_time_plus_duration -> wayback_timestamp
via -> landing_page_url
url -> document_url (and filename)
content_length -> size

Note that, if necessary, this process to refer to the 
cdx-server and wayback to get more information about 
the crawled data and improve the landing page and filename data.


"""

import os
import sys
import json
import pika
import time
import logging
import argparse
from urlparse import urlparse
import requests
import xml.dom.minidom

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib.agents.w3act import w3act
from lib.agents.document_mdex import DocumentMDEx

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.DEBUG )


def document_available(url, ts):
	"""
	
	Queries Wayback to see if the content is there yet.
	
	e.g.
	http://192.168.99.100:8080/wayback/xmlquery.jsp?type=urlquery&url=https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
	
	<wayback>
	<request>
		<startdate>19960101000000</startdate>
		<resultstype>resultstypecapture</resultstype>
		<type>urlquery</type>
		<enddate>20160204115837</enddate>
		<firstreturned>0</firstreturned>
		<url>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</url>
		<resultsrequested>10000</resultsrequested>
		<resultstype>resultstypecapture</resultstype>
	</request>
	<results>
		<result>
			<compressedoffset>2563</compressedoffset>
			<mimetype>application/pdf</mimetype>
			<redirecturl>-</redirecturl>
			<file>BL-20160204113809800-00000-33~d39c9051c787~8443.warc.gz
</file>
			<urlkey>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</urlkey>
			<digest>JK2AKXS4YFVNOTPS7Q6H2Q42WQ3PNXZK</digest>
			<httpresponsecode>200</httpresponsecode>
			<robotflags>-</robotflags>
			<url>https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
</url>
			<capturedate>20160204113813</capturedate>
		</result>
	</results>
</wayback>
	
	"""
	try:
		wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (args.wb_url, url)
		logger.debug("Checking %s" % wburl);
		r = requests.get(wburl)
		logger.debug("Response: %d" % r.status_code)
		# Is it known, with a matching timestamp?
		if r.status_code == 200:
			dom = xml.dom.minidom.parseString(r.text)
			for de in dom.getElementsByTagName('capturedate'):
				if de.firstChild.nodeValue == ts:
					# Excellent, it's been found:
					return True
	except Exception as e:
		logger.error( "%s [%s %s]" % ( str( e ), url, ts ) )
		logging.exception(e)
	# Otherwise:
	return False

def callback( ch, method, properties, body ):
	"""Passed a document crawl log entry, POSTs it to W3ACT."""
	try:
		logger.debug( "Message received: %s." % body )
		cl = json.loads(body)
		url = cl["url"]
		# Skip non http(s) records (?)
		if( not url[:4] == "http"):
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		status_code = cl["status_code"]
		# Don't index negative or non 2xx status codes here:
		if( status_code/100 != 2 ):
			logger.info("Ignoring <=0 status_code log entry: %s" % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			return
		# Build document info line:
		doc = {}
		wtid = cl['seed'].replace('WTID:','')
		doc['target_id'] = int(wtid)
		doc['wayback_timestamp'] = cl['start_time_plus_duration'][:14]
		doc['landing_page_url'] = cl['via']
		doc['document_url'] = cl['url']
		doc['filename'] = os.path.basename( urlparse(cl['url']).path )
		doc['size'] = int(cl['content_length'])
		# Check if content appears to be in Wayback:
		if document_available(doc['document_url'], doc['wayback_timestamp']):
			# If so, extract any additional metadata:
			doc = DocumentMDEx(doc).mdex()
			# and then inform W3ACT it's available:
			logger.debug("Sending doc: %s" % doc)
			act = w3act(args.w3act_url,args.w3act_user,args.w3act_pw)
			r = act.post_document(doc)
			if( r.status_code == 200 ):
				logger.debug("Success!")
				ch.basic_ack(delivery_tag = method.delivery_tag)
				return
			else:
				logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

	except Exception as e:
		logger.error( "%s [%s]" % ( str( e ), body ) )
		logging.exception(e)
		
	# All that failed? Then reject and requeue the message to try later:
	ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True)
	# Now sleep briefly to avoid overloading the servers:
	logger.warning("Sleeping for 15 seconds before retrying...")
	time.sleep(15)
	return

if __name__ == "__main__":
	parser = argparse.ArgumentParser('Get documents from the queue and post to W3ACT.')
	parser.add_argument('--amqp-url', dest='amqp_url', type=str, default="amqp://guest:guest@localhost:5672/%2f",
		help="AMQP endpoint to use [default: %(default)s]" )
	parser.add_argument('-w', '--w3act-url', dest='w3act_url', 
					type=str, default="http://localhost:9000/act/", 
					help="W3ACT endpoint to use [default: %(default)s]" )
	parser.add_argument('-u', '--w3act-user', dest='w3act_user', 
					type=str, default="wa-sysadm@bl.uk", 
					help="W3ACT user email to login with [default: %(default)s]" )
	parser.add_argument('-p', '--w3act-pw', dest='w3act_pw', 
					type=str, default="sysAdmin", 
					help="W3ACT user password [default: %(default)s]" )
	parser.add_argument('-W', '--wb-url', dest='wb_url', 
					type=str, default="http://localhost:8080/wayback", 
					help="Wayback endpoint to check URL availability [default: %(default)s]" )
	parser.add_argument('--num', dest='qos_num', 
		type=int, default=1, help="Maximum number of messages to handle at once. [default: %(default)s]")
	parser.add_argument('exchange', metavar='exchange', help="Name of the exchange to use.")
	parser.add_argument('queue', metavar='queue', help="Name of queue to view messages from.")
	
	args = parser.parse_args()
	
	try:
		logger.info( "Starting connection %s:%s." % ( args.amqp_url, args.queue ) )
		parameters = pika.URLParameters(args.amqp_url)
		connection = pika.BlockingConnection( parameters )
		channel = connection.channel()
		channel.exchange_declare(exchange=args.exchange, durable=True)
		channel.queue_declare( queue=args.queue, durable=True )
		channel.queue_bind(queue=args.queue, exchange=args.exchange, routing_key="documents-to-catalogue")
		channel.basic_qos(prefetch_count=args.qos_num)
		channel.basic_consume( callback, queue=args.queue, no_ack=False )
		channel.start_consuming()
	except Exception as e:
		logger.error( str( e ) )
		logging.exception(e)
		logger.info("Sleeping for 10 seconds before a restart is attempted...")
		time.sleep(10)

