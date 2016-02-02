'''
Created on 1 Feb 2016

@author: andy
'''
import os
import sys
import logging
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))
from lib.agents.w3act import w3act


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


if __name__ == "__main__":
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
		r = act.post_target(subargs[0], subargs[1], subargs[2])
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