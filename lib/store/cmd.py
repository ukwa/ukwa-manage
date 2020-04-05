'''
This contains the CLI tool for uploading to HDFS _very carefully_...
'''
import os
import json
import logging
import argparse
from lib.store.webhdfs import WebHDFSStore

logger = logging.getLogger(__name__)

# Defaults to using the production HDFS (via 'safe' gateway):
DEFAULT_WEBHDFS = os.environ.get("WEBHDFS_URL", "http://hdfs.api.wa.bl.uk/")
DEFAULT_WEBHDFS_USER = "access"

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='store')

    # Common arguments:
    parser.add_argument('-w', '--webhdfs-url', type=str, help='The WebHDFS URL to talk to (defaults to %s).' % DEFAULT_WEBHDFS, 
        default=DEFAULT_WEBHDFS)
    parser.add_argument('-u', '--webhdfs-user', type=str, help='The WebHDFS user to act as (defaults to %s).' % DEFAULT_WEBHDFS_USER, 
        default=DEFAULT_WEBHDFS_USER)
    parser.add_argument('--dry-run', action='store_true', help='Do not modify the TrackDB.')
    parser.add_argument('-i', '--indent', type=int, help='Number of spaces to indent when emitting JSON.')

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")

    # Add a parser for the 'get' subcommand:
    parser_get = subparsers.add_parser('get', help='Get a file from HDFS.')
    parser_get.add_argument('path', type=str, help='The file to get.')

    # Add a parser for the 'list' subcommand:
    parser_list = subparsers.add_parser('list', help='List a folder on HDFS.')
    parser_list.add_argument('path', type=str, help='The file to get.')

    # Add a parser for the 'update' subcommand:
    parser_up = subparsers.add_parser('put', help='Put a local file onto HDFS.')
    parser_up.add_argument('local_path', type=str, help='The local path to read.')
    parser_up.add_argument('hdfs_path', type=str, help='The HDFS path to write to.')

    # And PARSE it:
    args = parser.parse_args()

    # Set up client:
    st = WebHDFSStore(args.webhdfs_url, args.webhdfs_user)

    # Ops:
    logger.info("Got args: %s" % args)
    if args.op == 'list':
        listing = st.list(args.path)
        print(json.dumps(listing, indent=args.indent))
    elif args.op == 'get':
        stream = st.get(args.path)
        print(json.dumps(stream, indent=args.indent))
    elif args.op == 'put':
        st.put(args.local_path, args.hdfs_path)
    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
