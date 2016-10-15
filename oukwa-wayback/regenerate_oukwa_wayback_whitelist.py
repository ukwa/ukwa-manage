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

# globals --------------

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
	parser = argparse.ArgumentParser('Convert URLs to SURTs')
	parser.add_argument('-u, --urlsfile', dest='urlsFile', default='~/oukwa-wayback/txt.urls', help='File containing URLs to convert')
	parser.add_argument('-s, --surtsfile', dest='surtsFile', default='~/oukwa-wayback/txt.surts', help='Output file of SURTs')

	global args
	args = parser.parse_args()

	# test
	if not os.path.isfile(args.urlsFile):
		script_die("Argument of input file of URLs [%s] doesn't exist" % args.urlsFile)
	if os.path.isfile(args.surtsFile):
		script_die("Argument of output file of SURTs [%s] already exists" % args.surtsFile)


# main -----------------
def main():
	setup_logging()
	get_args()

if __name__ == "__main__":
	main()
