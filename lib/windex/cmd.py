'''
This contains the core TrackDB code for managing queries and updates to the Tracking Database
'''
import os
import json
import logging
import argparse
import urllib.parse
from lib.windex.cdx import CdxIndex

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Default CDX service to work with:
DEFAULT_CDX_SERVER = os.environ.get("CDX_SERVER","http://cdx.api.wa.bl.uk/")
DEFAULT_CDX_COLLECTION = os.environ.get("CDX_COLLECTION","data-heritrix")

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='windex')

    # Common arguments:
    parser.add_argument('-c', '--cdx-service', type=str, 
        help='The CDX Service to talk to (defaults to %s).' % DEFAULT_CDX_SERVER, 
        default=DEFAULT_CDX_SERVER)
    parser.add_argument('-C', '--cdx-collection', type=str, 
        help='The CDX Collection to work with (defaults to %s).' % DEFAULT_CDX_COLLECTION, 
        default=DEFAULT_CDX_COLLECTION)
    parser.add_argument('-i', '--indent', type=int, help='Number of spaces to indent when emitting JSON.')

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")

    # Add a parser for the 'get' subcommand:
    parser_cdx = subparsers.add_parser('cdx-query', help='Look up a URL.')
    #parser_cdx.add_argument('--first', type=int, help='Number of spaces to indent when emitting JSON.')
    parser_cdx.add_argument('url', type=str, help='The URL to look up.')

    # Add a parser for the 'list' subcommand:
    parser_cdx_job = subparsers.add_parser('cdx-hadoop-job', help='Run a Hadoop job to update the CDX service.')
    parser_cdx_job.add_argument('warc-list.txt', type=str, help='A file containing a list of WARCs to be indexed (full HDFS paths).')

    args = parser.parse_args()

    # Ops:
    logger.info("Got args: %s" % args)
    if args.op == 'cdx-query':
        # Set up CDX client:
        cdx_url = urllib.parse.urljoin(args.cdx_service, args.cdx_collection)
        cdxs = CdxIndex(cdx_url)
        # and query:
        for result in cdxs.query(args.url):
            print(result)
    elif args.op == 'cdx-hadoop-job':
        raise Exception("Not implemented!")
    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
