'''
This contains the core TrackDB code for managing queries and updates to the Tracking Database
'''
import os
import json
import logging
import argparse
import urllib.parse
from lib.windex.cdx import CdxIndex
from lib.windex.trace import follow_redirects

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

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

    # Add a parser for the 'query' subcommand:
    parser_cdx = subparsers.add_parser('cdx-query', help='Look up a URL.')
    #parser_cdx.add_argument('--first', type=int, help='Number of spaces to indent when emitting JSON.')
    parser_cdx.add_argument('url', type=str, help='The URL to look up.')

    # Add a parser for the 'trace' subcommand:
    parser_cdx = subparsers.add_parser('trace', help='Look up a URL, and follow redirects.')
    parser_cdx.add_argument('input_file', type=str, help='File containing the list of URLs to look up.')

    # Add a parser for the 'list' subcommand:
    parser_cdx_job = subparsers.add_parser('cdx-index', help='Run a Hadoop job to update the CDX service.')
    parser_cdx_job.add_argument('warc-list.ids', type=str, help='A file containing a list of IDs/HDFS paths of WARC files to be indexed (full HDFS paths).')

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

    elif args.op == 'trace':
        # Set up CDX client:
        cdx_url = urllib.parse.urljoin(args.cdx_service, args.cdx_collection)
        cdxs = CdxIndex(cdx_url)
        with open(args.input_file) as fin:
            for line in fin:
                url = line.strip()
                for result in follow_redirects(cdxs, url):
                    print("%s\t%s" % (result,url))

    elif args.op == 'cdx-index':
        # UNCLEAR HOW BEST TO DO THIS DUE TO AWKWARD DEPENDENCIES
        # It needs to run a Hadoop job, with the heavy JARs.
        # It needs to cope with a list of IDs.
        # It needs to take the list of WARCs on STDIN.
        # Hence, it needs to NOT run from here. I think?!?!?!?
        raise Exception("Not implemented!")
    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
