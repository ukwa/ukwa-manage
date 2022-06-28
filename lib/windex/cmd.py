'''
This file defines the command-line interface for performing web archive indexing tasks.
'''
import os
import json
import logging
import subprocess
import argparse
import tempfile
import urllib.parse
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# For querying TrackDB status:
from lib.trackdb.solr import SolrTrackDB
from lib.trackdb.tasks import Task

# Specific code relating to index work
from lib.windex.cdx import CdxIndex
from lib.windex.trace import follow_redirects
from lib.windex.mr_cdx_pywb_job import run_cdx_index_job, run_cdx_index_job_with_file
from lib.windex.mr_hash_job import run_hash_index_job_with_file
from lib.windex.mr_solr_job import run_solr_index_job
from lib.windex.mr_log_job import run_log_job, run_log_job_with_file
from lib.windex.index_ops import remove_solr_records

# Document Harvester config:
from lib.docharvester.find import DEFAULT_DB_URI


logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Default CDX service to work with:
DEFAULT_CDX_SERVER = os.environ.get("CDX_SERVER","http://cdx.dapi.wa.bl.uk/")
DEFAULT_CDX_COLLECTION = os.environ.get("CDX_COLLECTION","test-collection")

# Default SOLR
#DEFAULT_SOLR_ZOOKEEPERS = os.environ.get("SOLR_ZOOKEEPERS", "dev-zk1:2182,dev-zk2:2182,dev-zk3:2182")
#DEFAULT_SOLR_COLLECTION = os.environ.get("SOLR_COLLECTION", "test-collection")
DEFAULT_SOLR_ENDPOINT   = os.environ.get("SOLR_ENDPOINT", "http://solr8.bapi.wa.bl.uk/solr/fc-2020-test")

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
    trackdb_parser.add_argument('-t', '--trackdb-url', type=str, help='The TrackDB URL to talk to.', required=True)
    trackdb_parser.add_argument('-H', '--hadoop-service', choices=['h020', 'h3'], dest='service', help='Which Hadoop service to talk to (required).', required=True)
    trackdb_parser.add_argument('-S', '--stream', 
        choices= ['frequent', 'domain', 'webrecorder'], 
        default='frequent',
        help='Which content stream to look for.')
    trackdb_parser.add_argument('-Y', '--years-back', 
        default=1,
        type=int, help="How many years back to go looking for files to process.")
    trackdb_parser.add_argument('-N', '--no-update', action='store_true', help='Set this flag and TrackDB will not be updated or modified by this task.')


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

    # Add a parser for the 'cdx-index' subcommand:
    parser_index_cdx = subparsers.add_parser('cdx-index', 
        help="Use TrackDB to index WARCs into a CDX service.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, trackdb_parser, cdx_parser])
    parser_index_cdx.add_argument('-B', '--batch-size', type=int, help='Number files to process in each run.', default=DEFAULT_BATCH_SIZE)

    # Add a parser for the 'cdx-index-job' subcommand:
    parser_index_cdxjob = subparsers.add_parser('cdx-index-job', 
        help="Index WARCs listed in a file into a CDX service (for one-off indexing).", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, cdx_parser])
    parser_index_cdxjob.add_argument('input_file', help="A file containing a list of WARCs to index.")

    # Add a parser for the 'solr-index' subcommand:
    parser_index_solr = subparsers.add_parser('solr-index', 
        help="Use TrackDB to index WARCs into a Solr service.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, 
        parents=[common_parser, trackdb_parser])
    parser_index_solr.add_argument('-B', '--batch-size', type=int, help='Number files to process in each run.', default=DEFAULT_BATCH_SIZE)
    parser_index_solr.add_argument('-s', '--solr-url', help="The Solr collection endpoint to index into.", default=DEFAULT_SOLR_ENDPOINT)
    parser_index_solr.add_argument('-c', '--config', help="The indexer configuration file to use.")
    parser_index_solr.add_argument('-A', '--annotations', help="The annotations file to use with the indexer.")
    parser_index_solr.add_argument('-a', '--oasurts', help="The Open Access SURTS file to use with the indexer.")

    # Add a parser for the 'hash-job' subcommand:
    parser_index_hashjob = subparsers.add_parser('hash-job', 
        help="Generate the digest/hash of a set of files.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_index_hashjob.add_argument('input_file', help="A file containing a list of files to hash.")

    # Add a parser for the 'log-analyse' subcommand:
    parser_logs = subparsers.add_parser('log-analyse',
        help="Use TrackDB and index logs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, trackdb_parser])
    parser_logs.add_argument('-d', '--log-date', help='The last-modified date of log files to be processed, e.g. 2021-12-01. Default is today.',  type=date.fromisoformat, default=datetime.now())
    parser_logs.add_argument('-D', '--docs-found-db', default=DEFAULT_DB_URI, help="DB URI to use for the database of all documents found [default: %(default)s]")
    parser_logs.add_argument('-T', '--targets', help="The W3ACT Targets file to use to determine Watched Targets.", required=False)

    # Add a parser for the 'log-analyse-job' subcommand:
    parser_logsjob = subparsers.add_parser('log-analyse-job',
        help="Analyse a set of log files on HFDS.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_logsjob.add_argument('-T', '--targets', help="The W3ACT Targets file to use to determine Watched Targets.")
    parser_logsjob.add_argument('-D', '--docs-found-db', default=DEFAULT_DB_URI, help="DB URI to use for the database of all documents found [default: %(default)s]")
    parser_logsjob.add_argument('input_file', help="A file containing a list of log files to analyse.")

    # Add parse to clip records from indexes:
    parser_index_del = subparsers.add_parser('index-delete',
        help="Delete items from indexes.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser,cdx_parser])
    parser_index_del.add_argument('-s', '--solr-url', help='URL of Solr service to operate on.', default=DEFAULT_SOLR_ENDPOINT)
    parser_index_del.add_argument('urls_to_delete', help='Location of files listing URLs to be deleted from indexes.')

    # And PARSE:
    args = root_parser.parse_args()
    if args.op == None:
        raise Exception("No operation specified.")

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)    

    # Work out the date we're looking back to
    modified_since = datetime.now() - relativedelta(years=args.years_back)
    logger.info(f"Looking for TrackDB entries since {modified_since}.")

    # Set up full CDX endpoint URL:
    if "cdx_service" in args:
        cdx_url = urllib.parse.urljoin(args.cdx_service, args.cdx_collection)

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
        # Setup TrackDB
        tdb = SolrTrackDB(args.trackdb_url, hadoop=args.service, kind='warcs')
        # Record task start time:
        started_at = datetime.now()
        # Setup an event record:
        t = Task(f"{args.op}-{args.service}-{args.stream}")
        t.start()
        metrics = {}
        # Perform indexing job:
        item_updates = []
        status_value = None
        if args.op == 'cdx-index':
            # Get a list of items to process:
            status_field = "cdx_index_ss"
            field_value = ["-%s" % status_field, "%s*" % args.cdx_collection]
            items = tdb.list(args.stream, modified_since, field_value, limit=args.batch_size)
            if len(items) > 0:
                # Run a job to index those items:
                results = run_cdx_index_job(items, cdx_url)
                file_metrics = results['files']
                metrics = results['metrics']
                status_value = args.cdx_collection
            else:
                logger.warning("No WARCs found to process!")

        elif args.op == 'solr-index':
            # Extract collection name:
            solr_collection = args.solr_url.split("/")[-1]
            # Get a list of items to process:
            status_field = "solr_index_ss"
            field_value = ["-%s" % status_field, "%s*" % solr_collection]
            items = tdb.list(args.stream, modified_since, field_value, limit=args.batch_size)
            if len(items) > 0:
                # Run a job to index those items:
                results = run_solr_index_job(items, args.solr_url, args.config, args.annotations, args.oasurts)
                # No file-level metrics for this task:s
                file_metrics = {}
                metrics = results
                status_value = solr_collection
            else:
                logger.warning("No WARCs found to process!")

        # Perform item updates:
        ids = []
        for item in items:
            ids.append(item['id'])
            warc_path = urllib.parse.urlparse(item['id']).path
            item_update = file_metrics.get(warc_path, {})
            # Mark as indexed, but also as unverified:
            item_update['id'] = item['id']
            item_update[status_field] = "%s" % status_value
            item_updates.append(item_update)
        if args.no_update:
            logger.info("Skipping updates to TrackDB...")
        else:
            logger.info("Updating items in TrackDB...")
            tdb.import_items(item_updates)
        # Update event item in TrackDB
        t.finish()
        # Add properties:
        t.add({
            'batch_size': len(ids),
            'ids_ss' : ids,
            'stream_s': args.stream,
        })
        # Add job metrics:
        t.add(metrics)
        t.push_metrics( ['batch_size', 'total_record_count', 'total_sent_record_count', 'warc_file_count'] )
        # Print out the task summary:
        print(t.to_jsonline())
        
    elif args.op == 'cdx-index-job':
        # Run a one-off job to index some WARCs based on a list from a file:
        results = run_cdx_index_job_with_file(args.input_file, cdx_url)
        print(json.dumps(results, sort_keys=True))
    elif args.op == 'hash-job':
        # Run a one-off job to index some WARCs based on a list from a file:
        results = run_hash_index_job_with_file(args.input_file)
        print(json.dumps(results, sort_keys=True))
    elif args.op == 'log-analyse':
        # Setup TrackDB for log files
        tdb = SolrTrackDB(args.trackdb_url, hadoop=args.service, kind='crawl-logs')
        # Setup an event record:
        t = Task(f"{args.op}-{args.service}-{args.stream}")
        t.start()
        # Perform indexing job:
        ids = []
        stats = {}
        # Get a list of items to process:
        status_field = "log_analysis_ss"
        status_field_value = "done"
        field_value = ["-%s" % status_field, status_field_value]
        logger.info(f"Looking for log files with modified dates of {args.log_date}")
        items = tdb.list(args.stream, args.log_date, field_value, limit=10000, modified_that_day=True)
        logger.info(f"Found {len(items)} log file(s) for that day.")
        if len(items) > 0:
            # Run a job to index those items:
            stats = run_log_job(items, args.targets, args.docs_found_db)
            # If that worked (no exception thrown), update the tracking database accordingly:
            ids = []
            for item in items:
                ids.append(item['id'])
            # Mark as processed:
            if args.no_update:
                logger.info("Skipping updates to TrackDB...")
            else:
                logger.info("Updating items in TrackDB...")
                tdb.update(ids, status_field, status_field_value)
        else:
            logger.warning("No files found to process!")

        # Update event item in TrackDB
        t.finish()
        # Add properties:
        props = {
            'batch_size': len(ids),
            'ids_ss' : ids,
            'stream_s': args.stream,
        }
        t.add(props)
        # Add stats:
        t.add(stats)
        # TODO? Send to TrackDB:
        #tdb.import_items([t.as_dict()])
        print(t.to_jsonline())
        t.push_metrics(['batch_size'])
    elif args.op == 'log-analyse-job':
        # Run a one-off job to analyse some logs based on a list from a file:
        results = run_log_job_with_file(args.input_file, args.targets, args.docs_found_db)
        print(json.dumps(results, sort_keys=True))


    elif args.op == 'index-delete':
        with open(args.urls_to_delete) as f:
            urls = f.readlines()
        urls = [url.strip() for url in urls]
        remove_solr_records(
            args.solr_url,
            "url",
            urls
        )
    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
