#!/usr/bin/env python

import argparse
from crawl.w3act.w3act import w3act
from crawl.h3.utils import url_to_surt
import logging

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        'Grab Open Access targets and output to a file in SURT form.')
    parser.add_argument('--act-url', dest='act_url', type=str,
                        default="https://www.webarchive.org.uk/act/",
                        help="ACT endpoint to use. [default: %(default)s]")
    parser.add_argument('--act-username', dest='act_username', type=str,
                        help="ACT username to use. [default: %(default)s]")
    parser.add_argument('--act-password', dest='act_password', type=str,
                        help="ACT password to use. [default: %(default)s]")
    parser.add_argument('output_file', metavar='output file', default="/wayback/ldhosts.txt",
                        help="Output file to create, e.g. '/wayback/ldhosts.txt''.")

    args = parser.parse_args()

    w = w3act(args.act_url, args.act_username, args.act_password)
    items = w.get_oa_export("all")
    surts = ["http://(%s" % url_to_surt(u) for t in items for u in t["seeds"]]
    with open(args.output_file, "wb") as o:
        o.write("\n".join(surts))

