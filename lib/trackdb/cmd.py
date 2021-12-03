'''
This contains the core TrackDB code for managing queries and updates to the Tracking Database
'''
import os
import sys
import json
import logging
import argparse
from lib.trackdb.solr import SolrTrackDB

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='trackdb')

    # Common arguments:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')

    common_parser.add_argument('--dry-run', action='store_true', help='Do not modify the TrackDB.')
    common_parser.add_argument('-i', '--indent', type=int, help='Number of spaces to indent when emitting JSON.')
    common_parser.add_argument('-t', '--trackdb-url', type=str, help='The TrackDB URL to talk to.', required=True)
    common_parser.add_argument('kind', 
        choices= ['files', 'warcs', 'logs', 'tasks', 'launches', 'documents'], 
        help='The kind of entities to operate on. The \'files\' type is used to import records from HDFS listings.')

    # Filter arguments:
    filter_parser = argparse.ArgumentParser(add_help=False)
    filter_parser.add_argument('--year', 
        type=int,
        help='Filter down by date.')
    filter_parser.add_argument('--stream', 
        choices= ['frequent', 'domain', 'webrecorder'], 
        help='Filter the results by stream.', default=None)
    # Not implemented yet:
    #parser.add_argument('--collection', 
    #    choices= ['npld', 'bypm'], 
    #    help='Filter the results by the collection, NPLD or by-permission.', default='[* TO *]')
    filter_parser.add_argument('--field', 
        type=str,
        metavar=('FIELD', 'VALUE'),
        nargs=2,
        help='Filter by any additional field and value. Use the value \'_NONE_\' to look for unset values.')

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")
    subparsers.required = True

    # Add a parser for the 'get' subcommand:
    parser_get = subparsers.add_parser(
        'get', 
        help='Get a single record from the TrackDB.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_get.add_argument('id', type=str, help='The record ID to look up, or "-" to read a list of IDs from STDIN.')

    # Add a parser for the 'import' subcommand:
    parser_import = subparsers.add_parser(
        'import', 
        help='Import JSONL documents into TrackDB.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_import.add_argument('input_file', type=str, help='The file to read, use "-" for STDIN.')

    # Add a parser for the 'list' subcommand:
    parser_list = subparsers.add_parser(
        'list', 
        help='Get a list of records from the TrackDB, output as JSONL by default.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, filter_parser])
    parser_list.add_argument('--ids-only', action='store_true', help='Just output record IDs as plain text.')
    #parser_list.add_argument('-j', '--jsonl', action='store_true', help='Detailed output in JSONL format.')
    parser_list.add_argument('-l', '--limit', type=int, default=100, help='The maximum number of records to return.')

    # Add a parser for the 'update' subcommand:
    parser_up = subparsers.add_parser(
        'update', 
        help='Create or update on a record in the TrackDB.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_up.add_argument('--set', metavar=('field','value'), help='Set a field to a given value.', nargs=2)
    parser_up.add_argument('--add', metavar=('field','value'), help='Add the given value to a field. Always uses add-distinct', nargs=2)
    parser_up.add_argument('--remove', metavar=('field','value'), help='Remove the specified value from the field.', nargs=2)
    parser_up.add_argument('--inc', metavar=('field','increment'), help='Increment the specified field, e.g. "--inc counter 1".', nargs=2)
    parser_up.add_argument('id', type=str, help='The record ID to update, or "-" to read a list of IDs from STDIN.')

    # TODO Add a 'stats' subcommand that summarises the record stats:
    # e.g. count of records by kind.

    # TODO Add 'task' record support, and add record existance checks.

    # And PARSE it:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    # Set up Solr client:
    tdb = SolrTrackDB(args.trackdb_url, kind=args.kind)

    # Ops:
    logger.debug("Got args: %s" % args)
    if args.op == 'list':
        for doc in tdb.list(args.stream, args.year, args.field, limit=args.limit):
            if args.ids_only:
                print(doc['id'])
            else:
                print(json.dumps(doc, indent=args.indent))
    elif args.op == 'import':
        if args.input_file == '-':
            count = tdb.import_jsonl_reader(sys.stdin.buffer)
        else:
            with open(args.input_file) as f:
                count = tdb.import_jsonl_reader(f)
        if count == 0:
            raise Exception("No items were imported!")
    elif args.op == 'get':
        doc = tdb.get(args.id)
        if doc:
            print(json.dumps(doc, indent=args.indent))
    elif args.op == 'update':
        ids = []
        if args.id == '-':
            for line in sys.stdin:
                ids.append(line.strip())
        else:
            ids.append(args.id)
        # And run the updates:
        if args.set:
            tdb.update(ids, args.set[0], args.set[1], action='set')
        if args.add:
            tdb.update(ids, args.add[0], args.add[1], action='add-distinct')
        if args.remove:
            tdb.update(ids, args.remove[0], args.remove[1], action='remove')
        if args.inc:
            tdb.update(ids, args.inc[0], args.inc[1], action='remove')
    else:
        raise Exception("Operaton %s is not implemented!" % args.op )


if __name__ == "__main__":
    main()
