'''
This contains the core TrackDB code for managing queries and updates to the Tracking Database
'''
import os
import json
import logging
import subprocess
import argparse
import tempfile
import urllib.parse
from lib.windex.cdx import CdxIndex
from lib.windex.trace import follow_redirects
from lib.windex.mr_cdx_job import MRCdxIndexerJarJob

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Default CDX service to work with:
DEFAULT_CDX_SERVER = os.environ.get("CDX_SERVER","http://cdx.api.wa.bl.uk/")
DEFAULT_CDX_COLLECTION = os.environ.get("CDX_COLLECTION","data-heritrix")

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='windex')

    # Common arguments:
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging.')
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
    parser_cdx_job.add_argument('input_file', type=str, help='A file containing a list of TrackDB IDs of WARC files to be indexed.')

    # And PARSE:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)    

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
        with tempfile.NamedTemporaryFile('w+') as fpaths:
            # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
            with open(args.input_file) as fin:
                for line in fin.readlines():
                    path = line.strip()
                    if path.startswith('hdfs:'):
                        path = urllib.parse.urlparse(path).path
                    fpaths.write("%s\n" % path)

            # Make sure temp file is up to date:
            fpaths.flush()
                    
            #hdfs_in = '/9_processing/warcs2cdx/warcs-list.txt'
            hdfs_out = '/9_processing/warcs2cdx/output'

            # Remove old input/output:
            #subprocess.check_call("hadoop fs -rm %s" % hdfs_in)
            subprocess.call(["hadoop", "fs", "-rmr", hdfs_out])

            # Set up the CDX indexer map-reduce job:
            mr_job = MRCdxIndexerJarJob(args=[
                '-r', 'hadoop',
                '--cdx-endpoint', 'http://cdx.dapi.wa.bl.uk/data-heritrix',
                fpaths.name, # < local input file, mrjob will upload it
                ])

            # Run and gather output:
            stats = {}
            with mr_job.make_runner() as runner:
                runner.run()
                for key, value in mr_job.parse_output(runner.cat_output()):
                    i = stats.get(key, 0)
                    stats[key] = i + int(value)
                    print(key,value)

            # And print the totals:
            print(json.dumps(stats))

    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
