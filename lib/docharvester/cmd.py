'''
Document Harvester command line tools.

The overall workflow is:

- import candidate documents into a 'documents found' database
- check they are available in wayback
- determine which W3ACT target they are associated with (if any)
- collect some additional metadata for them
- push the accepted document to W3ACT or reject them if they don't match a W3ACT target

'''
import os
import csv
import sys
import json
import logging
import argparse
from lib.docharvester.kafka import KafkaDocumentFinder
from lib.docharvester.from_luigi_files import LuigiStateScanner
from lib.docharvester.find import DocumentsFoundDB
from lib.docharvester.to_w3act import DocToW3ACT

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='docharv')

    # Common arguments:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    common_parser.add_argument('-T', '--targets', required=True, help="Location of crawl feed/targets file used to determine watched targets.")

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")
    subparsers.required = True

    # Add a parser for the 'import-kafka' subcommand:
    parser_kafka = subparsers.add_parser('import-kafka', 
        help='Connect to a Kafka service "crawled urls" log and scan for documents.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_kafka.add_argument('-k', '--kafka-bootstrap-server', dest='bootstrap_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser_kafka.add_argument("-L", "--latest", dest="latest", action="store_true", default=False, required=False,
                        help="Start with the latest messages rather than from the earliest. [default: %(default)s]")
    parser_kafka.add_argument("-M", "--max-messages", dest="max_messages", default=None, required=False, type=int,
                        help="Maximum number of messages to process. [default: %(default)s]")
    parser_kafka.add_argument("-t", "--timeout", dest="timeout", default=10, required=False, type=int,
                        help="Seconds to wait for more messages before timing-out. '-1' for 'never'. [default: %(default)s]")
    parser_kafka.add_argument("-S", "--stream", dest="topic", default="fc.crawled", required=False,
                        help="Name of the stream to inspect. [default: %(default)s]")
    parser_kafka.add_argument("-G", "--group_id", dest="group_id", default=None, required=False,
                        help="Group ID to use. Setting this enables offset tracking. [default: %(default)s]")
    parser_kafka.add_argument("-C", "--client_id", dest="client_id", default="CrawlStreamsReport", required=False,
                        help="Client ID to use. [default: %(default)s]")

    # Add a parser for the 'import-luigi' subcommand:
    parser_luigi = subparsers.add_parser('import-luigi', 
        help='Scan a Luigi state folder for documents.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_luigi.add_argument('-p', '--path', dest='path', type=str, default='/mnt/gluster/ingest/task-state/documents/',
                        help="The path to scan [default: %(default)s]")

    # Add a parser for the 'process' subcommand:
    parser_p = subparsers.add_parser('process', 
        help='Process found/NEW documents, assign targets and send to ACT.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_p.add_argument('-C', '--cdx', dest='cdx_server', type=str, default='http://cdx.api.wa.bl.uk/data-heritrix',
                        help="The CDX service to use to check items are available [default: %(default)s]")
    parser_p.add_argument('--act-url', dest='act_url', default='http://act.dapi.wa.bl.uk/act',
                        help="The ACT service to send documents to. [default: %(default)s]")
    parser_p.add_argument('--act-user', dest='act_user', default='w3act-access@bl.uk',
                        help="The user to log into ACT as. [default: %(default)s]")
    parser_p.add_argument('--act-pw', dest='act_password', required=True,
                        help="The password to log into ACT with.")

    # And PARSE it:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)    

    # Ops:
    logger.debug("Got args: %s" % args)
    if args.op == 'import-kafka':
        # Process documents out of a crawl log stream:
        with KafkaDocumentFinder(args) as finder:
            finder.find()
    elif args.op == 'import-luigi':
        # Scan Luigi state files:
        with LuigiStateScanner() as scanner:
            scanner.scan(args.path)
    elif args.op == 'process':
        # Find 'NEW' docs and attempt to push them to W3ACT:
        df = DocumentsFoundDB()
        dw = DocToW3ACT(args.cdx_server, args.targets, args.act_url, args.act_user, args.act_password)
        df.update_new_documents(dw, apply_updates=True)
    else:
        raise Exception("Not implemented!")

if __name__ == "__main__":
    main()