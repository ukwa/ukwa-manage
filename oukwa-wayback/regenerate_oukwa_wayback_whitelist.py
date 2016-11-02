#!/usr/bin/env python2.7
'''
Description:	Regenerate OUKWA wayback whitelist of SURTs from W3ACT
Author:		Gil
Date:		2017 Jan 11
'''

# libraries ------------
import os
import sys
import logging
import argparse
from crawl.w3act.w3act import w3act
from crawl.h3.utils import url_to_surt

# globals --------------
f_surts = '~/oukwa-wayback-whitelist/surts'

# functions ------------
def setup_logging():
        # set handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
        handler.setFormatter(formatter)

        # attach to root logger
        logging.root.addHandler(handler)

        # set logging levels
        global logger
        logging.basicConfig(level=logging.WARNING)
        logging.root.setLevel(logging.WARNING)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

def script_die(msg):
        logger.error(msg)
        logger.error("Script died")
        sys.exit(1)


def get_args():
	parser = argparse.ArgumentParser('Grab Open Access targets and output to a file in SURT form.')
	parser.add_argument('-w, --wcturlsfile', dest='f_wcturls', default='~/oukwa-wayback-whitelist/wct.urls', 
		help='File containing URLs from WCT to convert')

	global args
	args = parser.parse_args()

	# test
	if not os.path.isfile(args.f_wcturls):
		script_die("Argument of input file of URLs [%s] doesn't exist" % args.f_wcturls)


# main -----------------
def main():
	setup_logging()
	get_args()

if __name__ == "__main__":
	main()






'''
    w = w3act(args.act_url, args.act_username, args.act_password)
    items = w.get_oa_export("all")
    surts = ["http://(%s" % url_to_surt(u) for t in items for u in t["seeds"]]
    with open(args.output_file, "wb") as o:
        o.write("\n".join(surts))
  
'''
