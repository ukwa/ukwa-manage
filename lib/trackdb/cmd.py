'''
This contains the core TrackDB code for managing queries and updates to the Tracking Database
'''
import os
import argparse
from solr import SolrTrackDB

# Defaults to using the DEV TrackDB Solr backend:
DEFAULT_TRACKDB = os.environ.get("TRACKDB_URL","http://trackdb.dapi.wa.bl.uk/solr/tracking")

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='trackdb')

    # Common arguments:
    parser.add_argument('-t', '--trackdb-url', type=str, help='The TrackDB URL to talk to (defaults to %s).' % DEFAULT_TRACKDB, 
        default=DEFAULT_TRACKDB)
    parser.add_argument('--dry-run', action='store_true', help='Do not modify the TrackDB.')
    parser.add_argument('kind', 
        choices= ['files', 'warcs', 'logs', 'launches'], 
        help='The kind of thing to track.')

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers()

    # Add a parser for the 'get' subcommand:
    parser_get = subparsers.add_parser('get', help='Get a single record from the TrackDB.')
    parser_get.add_argument('id', type=str, help='The id to look up.')

    # Add a parser for the 'list' subcommand:
    parser_list = subparsers.add_parser('list', help='Get a list of records from the TrackDB.')
    parser_get.add_argument('--limit', type=int, default=10, help='The maximum number of records to return.')

    # Add a parser for the 'update' subcommand:
    parser_up = subparsers.add_parser('update', help='Create or update on a record in the TrackDB.')
    parser_up.add_argument('--set', type=str, help='Set a field to a given value, using the format "field:value".', nargs="*")
    parser_up.add_argument('--add', type=str, help='Add the given value to a field, using the format "field:value".', nargs="*")
    parser_up.add_argument('--remove', type=str, help='Remove the specified field.', nargs="*")
    parser_up.add_argument('--inc', type=str, help='Increment the specified field, using the format "field:increment" (the increment defaults to 1 if not specified).', nargs="*")
    parser_up.add_argument('id', type=str, help='The record ID to use.')

    # And PARSE it:
    args = parser.parse_args()

    # Set up Solr client:
    tdb = SolrTrackDB(args.trackdb_url, kind=args.kind)
    docs = tdb.list('[* TO *]', '[* TO *]', 'test_s', 'test')
    print(docs)
    tdb.update('anj-test','kind_s','warcs')


if __name__ == "__main__":
    main()
