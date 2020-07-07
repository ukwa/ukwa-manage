#!/usr/bin/env python
"""
Generic methods used for verifying/indexing SIPs.
"""

# libraries ---------------------------
from __future__ import absolute_import
import os
import sys
import logging
import argparse

# globals -----------------------------


# functions ---------------------------
def setup_logging():
    global logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.WARN)
    logger = logging.getLogger(__name__)
    logger.setLevel( logging.INFO)

def script_die(msg):
    logger.error(msg)
    logger.error("Script died")
    sys.exit(1)

def get_args():
    parser = argparse.ArgumentParser('Verify HDFS and DLS ARKs')
    parser.add_argument('--hdfsf', dest='hdfsFile', default='npld_warcs_arks.txt', help='WA HDFS ARKs file')
    parser.add_argument('--dlsf', dest='dlsFile', default='dls_wa_export.txt', help='DLS ARKs file')
    global args
    args = parser.parse_args()
    if not (os.path.isfile(args.dlsFile) and os.path.isfile(args.hdfsFile)):
        script_die("Both input files, '{}' and '{}', are needed".format(args.hdfsFile, args.dlsFile))


def main():
    setup_logging()
    get_args()

if __name__ == "__main__":
    main()
