#!/usr/bin/env python

import os
import re
import sys
import time
import locale
import shutil
import httplib
import rfc3987
import urllib2
import logging
import argparse
import heritrix
import StringIO
import subprocess
import dateutil.parser
from lxml import etree
from datetime import datetime
from retry_decorator import *
from pywb.utils import binsearch
from pywb.warc import cdxindexer
from optparse import OptionParser
from hanzo.warctools import WarcRecord
from requests.exceptions import ConnectionError

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.WARNING )
logger = logging.getLogger( "warcindexer" )


def index_warcs( warcs, cdx, base_cdx=None ):
    """Creates a sorted CDX for a series of WARC files, 
       supplementing revisit records with original records."""
    with open(cdx, "wb") as o:
        with cdxindexer.CDXWriter(o, False) as writer:
            for fullpath, filename in cdxindexer.iter_file_or_dir(warcs):
                with open(fullpath, "rb") as infile:
                    entry_iter = cdxindexer.create_index_iter(infile, include_all=False, surt_ordered=False)
                    for entry in entry_iter:
                        if entry.mime == "warc/revisit":
                            if base_cdx is not None:
                                with open(base_cdx, "rb") as c:
                                    matches = [m for m in binsearch.iter_exact(c, entry.key) if m.split()[3] != "warc/revisit" and m.split()[5] == entry.digest]
                                if len(matches) > 0:
                                    o.write("%s\n" % matches[-1])
                        writer.write(entry, fullpath)

    with open(cdx, "rb") as i:
        lines = [l.strip() for l in i]
    lines.sort(cmp=locale.strcoll)
    with open(cdx, "wb") as o:
        o.write("\n".join(lines))


def generate_path_index(warcs, index):
    lines = []
    for fullpath, filename in cdxindexer.iter_file_or_dir(warcs):
        lines.append("%s\t%s" % (filename, fullpath))
    lines.sort()
    set(lines)
    with open(index, "wb") as path_index:
        path_index.write("\n".join(lines))


if __name__ == "__main__":
    warcs = sys.argv[1:]
    index_warcs(warcs, "test.cdx")

