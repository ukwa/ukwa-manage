#!/usr/bin/env python
# encoding: utf-8

'''
Created on 1 Feb 2016


Note:
Sending doc: 
{'wayback_timestamp': u'20160202235322', 
'target_id': 1, 
'filename': u'supplementary-guidance-january-2016.pdf', 
'landing_page_url': u'https://www.gov.uk/government/publications/department-for-transport-delivers-more-grant-funding-to-transport-freight-by-rail', 
'document_url': u'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/492092/supplementary-guidance-january-2016.pdf',
 'size': 202581}

@author: andy
'''
import os
import sys
import logging
import argparse
from urlparse import urlparse

from lib.w3act.w3act import w3act


# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.INFO )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )


def main():
	parser = argparse.ArgumentParser('Interrogate the W3ACT API.')
	parser.add_argument('-w', '--w3act-url', dest='w3act_url', 
					type=str, default="http://localhost:9000/act/", 
					help="W3ACT endpoint to use [default: %(default)s]" )
	parser.add_argument('-u', '--w3act-user', dest='w3act_user', 
					type=str, default="wa-sysadm@bl.uk",
					help="W3ACT user email to login with [default: %(default)s]" )
	parser.add_argument('-p', '--w3act-pw', dest='w3act_pw', 
					type=str, default="sysAdmin", 
					help="W3ACT user password [default: %(default)s]" )
	parser.add_argument('action', metavar='action', help="The action to perform (one of 'add-target', 'list-targets', 'get-target').")
	
	args, subargs = parser.parse_known_args()
	
	# Connect
	act = w3act(args.w3act_url,args.w3act_user,args.w3act_pw)
	
	if args.action == "list-targets":
		json = act.get_json("api/targets")
		print json
	elif args.action == 'add-target':
		r = act.post_target(subargs[0], subargs[1])
		print r.status_code
		print r.text		
	elif args.action == 'update-target-schedule':
		r = act.update_target_schedule(int(subargs[0]), subargs[1], subargs[2])
		print r.status_code
		print r.text
	elif args.action == 'set-selector':
		r = act.update_target_selector(int(subargs[0]))
		print r.status_code
		print r.text
	elif args.action == 'watch-target':
		r = act.watch_target(int(subargs[0]))
		print r.status_code
		print r.text
	elif args.action == 'unwatch-target':
		r = act.unwatch_target(int(subargs[0]))
		print r.status_code
		print r.text
	elif args.action == 'add-document':
		doc = {}
		wtid = subargs[0]
		doc['target_id'] = int(wtid)
		doc['wayback_timestamp'] = subargs[1]
		doc['document_url'] = subargs[2]
		doc['landing_page_url'] = subargs[3]
		doc['filename'] = os.path.basename( urlparse(doc['document_url']).path )
		doc['size'] = ""
		logger.debug("Sending doc: %s" % doc)
		r = act.post_document(doc)
		print r.status_code
		print r.text

if __name__ == "__main__":
	main()