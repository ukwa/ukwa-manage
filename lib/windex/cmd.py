'''
This file defines the command-line interface for performing web archive indexing tasks.
'''
import os
import json
import logging
import subprocess
import argparse
import tempfile
import datetime
import urllib.parse

# For querying TrackDB status:
from lib.trackdb.solr import SolrTrackDB
from lib.trackdb.cmd import DEFAULT_TRACKDB
from lib.trackdb.tasks import Task

# Specific code relating to index work
from lib.windex.cdx import CdxIndex
from lib.windex.trace import follow_redirects
from lib.windex.mr_cdx_job import run_cdx_index_job
from lib.windex.mr_solr_job import run_solr_index_job

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Default CDX service to work with:
DEFAULT_CDX_SERVER = os.environ.get("CDX_SERVER","http://cdx.dapi.wa.bl.uk/")
DEFAULT_CDX_COLLECTION = os.environ.get("CDX_COLLECTION","test-collection")

# Default SOLR
DEFAULT_SOLR_ZOOKEEPERS = os.environ.get("SOLR_ZOOKEEPERS", "dev-zk1:2182,dev-zk2:2182,dev-zk3:2182")
DEFAULT_SOLR_COLLECTION = os.environ.get("SOLR_COLLECTION", "test-collection")

# Other defaults
DEFAULT_BATCH_SIZE = 100

# MAIN
def main():
    # Set up a parser:
    root_parser = argparse.ArgumentParser(prog='windex')

    # Common arguments, by group:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')

    # TrackDB args:
    trackdb_parser = argparse.ArgumentParser(add_help=False)
    trackdb_parser.add_argument('-t', '--trackdb-url', type=str, help='The TrackDB URL to talk to.', 
        default=DEFAULT_TRACKDB)
    trackdb_parser.add_argument('-S', '--stream', 
        choices= ['frequent', 'domain', 'webrecorder'], 
        default='frequent',
        help='Which content stream to look for.')
    trackdb_parser.add_argument('-Y', '--year', 
        default=datetime.date.today().year,
        type=int, help="Which year to query for.")

    # CDX Server args:
    cdx_parser = argparse.ArgumentParser(add_help=False)
    cdx_parser.add_argument('-c', '--cdx-service', type=str, 
        help='The CDX Service to talk to.',
        default=DEFAULT_CDX_SERVER)
    cdx_parser.add_argument('-C', '--cdx-collection', type=str, 
        help='The CDX Collection to work with.', 
        default=DEFAULT_CDX_COLLECTION)

    # Use sub-parsers for different operations:
    subparsers = root_parser.add_subparsers(dest="op")

    # Add a parser for the 'query' subcommand:
    parser_cdx = subparsers.add_parser('cdx-query', 
        help='Look up a URL.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, cdx_parser])
    parser_cdx.add_argument('-i', '--indent', type=int, help='Number of spaces to indent when emitting JSON.')
    parser_cdx.add_argument('url', type=str, help='The URL to look up.')

    # Add a parser for the 'trace' subcommand:
    parser_trace = subparsers.add_parser('trace', 
        help='Look up a URL, and follow redirects.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, cdx_parser])
    parser_trace.add_argument('input_file', type=str, help='File containing the list of URLs to look up.')

    # Add a parser for the 'list' subcommand:
    parser_index_cdx = subparsers.add_parser('cdx-index', 
        help="Index WARCs into a CDX service.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, trackdb_parser, cdx_parser])
    parser_index_cdx.add_argument('-B', '--batch-size', type=int, help='Number files to process in each run.', default=DEFAULT_BATCH_SIZE)

    parser_index_solr = subparsers.add_parser('solr-index', 
        help="Index WARCs into a Solr service.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, 
        parents=[common_parser, trackdb_parser])
    parser_index_solr.add_argument('-B', '--batch-size', type=int, help='Number files to process in each run.', default=DEFAULT_BATCH_SIZE)
    parser_index_solr.add_argument('-Z', '--zks', help="Zookeepers to talk to, as comma-separated lost of HOST:PORT", default=DEFAULT_SOLR_ZOOKEEPERS)
    parser_index_solr.add_argument('-C', '--solr-collection', help="The SolrCloud collection to index into.", default=DEFAULT_SOLR_COLLECTION)
    parser_index_solr.add_argument('config', help="The indexer configuration file to use.")
    parser_index_solr.add_argument('annotations', help="The annotations file to use with the indexer.")
    parser_index_solr.add_argument('oasurts', help="The Open Access SURTS file to use with the indexer.")

    # And PARSE:
    args = root_parser.parse_args()

    # Set up full CDX endpoint URL:
    if "cdx_service" in args:
        cdx_url = urllib.parse.urljoin(args.cdx_service, args.cdx_collection)

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)    

    # Ops:
    logger.info("Got args: %s" % args)
    if args.op == 'cdx-query':
        # Set up CDX client:
        cdxs = CdxIndex(cdx_url)
        # and query:
        for result in cdxs.query(args.url):
            print(result)

    elif args.op == 'trace':
        # Set up CDX client:
        cdxs = CdxIndex(cdx_url)
        with open(args.input_file) as fin:
            for line in fin:
                url = line.strip()
                for result in follow_redirects(cdxs, url):
                    print("%s\t%s" % (result,url))

    elif args.op == 'cdx-index' or args.op == 'solr-index':
        # TODO Add option to just index from a list of file (no TrackDB at all)
        # Setup TrackDB
        tdb = SolrTrackDB(args.trackdb_url, kind='warcs',)
        # Setup an event record:
        t = Task(args.op)
        t.start()
        # Perform indexing job:
        ids = []
        stats = {}
        if args.op == 'cdx-index':
            # Get a list of items to process:
            cdx_field = "cdx_index_ss"
            field_value = ["-%s" % cdx_field, "%s*" % args.cdx_collection]
            items = tdb.list(args.stream, args.year, field_value, limit=args.batch_size)
            if len(items) > 0:
                # Run a job to index those items:
                stats = run_cdx_index_job(items, cdx_url)
                # If that worked (no exception thrown), update the tracking database accordingly:
                ids = []
                for item in items:
                    ids.append(item['id'])
                # Mark as indexed, but also as unverified:
                tdb.update(ids, cdx_field, "%s" % args.cdx_collection)
                tdb.update(ids, cdx_field, "%s|unverified" % args.cdx_collection)
                # Add fields to store:
                stats['cdx_endpoint_s'] = cdx_url
            else:
                logger.warn("No WARCs found to process!")
                return
        elif args.op == 'solr-index':
            # Get a list of items to process:
            solr_field = "solr_index_ss"
            field_value = ["-%s" % solr_field, "%s*" % args.solr_collection]
            items = tdb.list(args.stream, args.year, field_value, limit=args.batch_size)
            if len(items) > 0:
                # Run a job to index those items:
                stats = run_solr_index_job(items, args.zks, args.solr_collection, args.config, args.annotations, args.oasurts)
                # If that worked (no exception thrown), update the tracking database accordingly:
                for item in items:
                    ids.append(item['id'])
                # Mark as indexed, but also as to-be-verified:
                tdb.update(ids, solr_field, "%s" % args.solr_collection)
                tdb.update(ids, solr_field, "%s|unverified" % args.solr_collection)
                # Add fields to store:
                stats['solr_collection_s'] = args.solr_collection
            else:
                logger.warn("No WARCs found to process!")

        # Update event item in TrackDB
        t.finish()
        # Add properties:
        props = {
            'batch_size_i': len(ids),
            'ids_ss' : ids,
            'stream_s': args.stream,
            'year_i': args.year
        }
        t.add(props)
        # Add stats:
        t.add(stats)
        # Send to TrackDB:
        tdb.import_items([t.as_dict()])

    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
