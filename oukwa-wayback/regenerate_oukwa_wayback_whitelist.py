#!/usr/bin/env python2.7
'''
Description:	Regenerate OUKWA wayback whitelist of SURTs from W3ACT
Author:		Gil
Date:		2017 Feb 1
'''

# libraries ------------
import os
import sys
import logging
import argparse
import re
from surt import surt

sys.path.append('/home/tomcat/github/python-shepherd/')
from crawl.w3act.w3act import w3act

# globals --------------
RE_NONCHARS = re.compile(r"""
[^	# search for any characters that aren't those below
\w
:
/
\.
\-
=
?
&
~
%
+
@
]
""", re.VERBOSE)
RE_SCHEME = re.compile('https?://')
allSurtsFile = '/opt/wayback-whitelist.txt'
w3actURLsFile = '/home/tomcat/oukwa-wayback-whitelist/w3act_urls'

# functions ------------
def setup_logging():
        global logger
	logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # set handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)-8s %(filename)s.%(funcName)s:\t %(message)s")
        handler.setFormatter(formatter)

        # attach to root logger
        logging.root.addHandler(handler)

def script_die(msg):
        logger.error(msg)
        logger.error("Script died")
        sys.exit(1)

# --------
def get_args():
	parser = argparse.ArgumentParser('Grab Open Access targets and output to a file in SURT form.')
	parser.add_argument('-w, --wcturlsfile', dest='WCTurls', default='/home/tomcat/oukwa-wayback-whitelist/wct_urls.txt', 
		help='File containing URLs from WCT to convert')
	parser.add_argument('-a, --w3acturl', dest='act_url', default='https://www.webarchive.org.uk/act',
		help='URL of ACT service')
	parser.add_argument('-u, --user', dest='act_username', help="Email username for ACT")
	parser.add_argument('-p, --password', dest='act_password', help="Password for ACT access")

	global args
	args = parser.parse_args()

	# test
	if not os.path.isfile(args.WCTurls):
		script_die("Argument of input file of URLs [%s] doesn't exist" % args.WCTurls)

	logger.info("Using WCT URLs file %s" % args.WCTurls)

# --------
def generate_surt(url):
	if RE_NONCHARS.search(url):
		logger.warn("Questionable characters found in URL [%s]" % url)

	surtVal = surt(url)

	#### WA: ensure SURT has scheme of original URL ------------ 
	# line_scheme = RE_SCHEME.match(line)           # would allow http and https (and any others)
	line_scheme = 'http://'                         # for wayback, all schemes need to be only http
	surt_scheme = RE_SCHEME.match(surtVal)

	if line_scheme and not surt_scheme:
		if re.match(r'\(', surtVal):
			#surtVal = line_scheme.group(0) + surtVal
			surtVal = line_scheme + surtVal
			logger.debug("Added scheme [%s] to surt [%s]" % (line_scheme, surtVal))
		else:
			#surtVal = line_scheme.group(0) + '(' + surtVal
			surtVal = line_scheme + '(' + surtVal
			logger.debug("Added scheme [%s] and ( to surt [%s]" % (line_scheme, surtVal))

	surtVal = re.sub(r'\)/$', ',', surtVal)

	return surtVal

def surts_from_wct(allSurts):
	count = 0
	# process every URL from WCT
	with open(args.WCTurls, 'r') as wcturls:
		# strip any whitespace from beginning or end of line
		lines = wcturls.readlines()
		lines = [l.strip() for l in lines]

		# for all WCT URLs, generate surt. Using a set disallows duplicates
		for line in lines:
			allSurts.add(generate_surt(line))
			count += 1

	logger.info("%s surts from WCT generated" % count)

def surts_from_w3act(allSurts):
	count = 0
	# get licenced urls from w3act
	w = w3act(args.act_url, args.act_username, args.act_password)
	acturls = w.get_oa_export("all")

	# write a copy of w3act urls as a record
	with open(w3actURLsFile, 'w') as outAct:
		# for all w3act urls, generate surt
		for line in acturls:
			for seed in line["seeds"]:
				outAct.write("%s\n" % seed)

				surtVal = generate_surt(seed)
				allSurts.add(surtVal)
				count += 1
				logger.debug("ACT seed [%s] surt [%s]" % (seed, surtVal))

	logger.info("%s surts from ACT generated" % count)


# main -----------------
def main():
	setup_logging()
	get_args()

	# collate surts
	allSurts = set()
	surts_from_wct(allSurts)
	surts_from_w3act(allSurts)

	# write to output file
	count = 0
	with open(allSurtsFile, 'w') as outfile:
		for surt in sorted(allSurts):
			outfile.write("%s\n" % surt)
			count += 1

	logger.info("Wrote %s surts to %s" % (count, allSurtsFile))

if __name__ == "__main__":
	main()
