'''
This contains the CLI tool for uploading to HDFS _very carefully_...
'''
import os
import csv
import sys
import json
import logging
import argparse
from lib.store.webhdfs import WebHDFSStore
from lib.store.nominet import ingest_from_nominet
from lib.store.warc_tidy import warc_tidy_up

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Fields to output in the CSV version:
CSV_FIELDNAMES =  ['permissions_s', 'hdfs_replicas_i', 'hdfs_user_s', 'hdfs_group_s', 'file_size_l', 'modified_at_dt', 'file_path_s']

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='store')

    # Common arguments:
    parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    parser.add_argument('--dry-run', action='store_true', help='Do not modify the TrackDB.')
    parser.add_argument('-i', '--indent', type=int, help='Number of spaces to indent when emitting JSON.')

    # First positional argument:
    parser.add_argument(choices=['h020', 'h3'], dest='service', help='Which Hadoop service to talk to (required).')

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")
    subparsers.required = True

    # 'get' subcommand - retrieves files from the store:
    parser_get = subparsers.add_parser('get', help='Get a file from the store.')
    parser_get.add_argument('--offset', type=int, help='The byte offset to start reading from (default is 0).')
    parser_get.add_argument('--length', type=int, help='The number of bytes to read. (default is to read the whole thing)')
    parser_get.add_argument('path', type=str, help='The file to get.')
    parser_get.add_argument('local_path', type=str, help='The local file to copy to (use "-" for STDOUT).')

    # 'list' subcommand - list what's in the store:
    parser_list = subparsers.add_parser('list', help='List a folder on the store, outputting a list of file paths by default.')
    parser_list.add_argument('-r', '--recursive', action='store_true', help='List files recursively (directories are not listed).')
    parser_list.add_argument('-I', '--ids', action='store_true', help='List record identifiers rather than file paths.')
    parser_list.add_argument('-c', '--csv', action='store_true', help='List in CSV format rather than the default.')
    parser_list.add_argument('-j', '--jsonl', action='store_true', help='List in JSONL format rather than the default.')
    parser_list.add_argument('path', type=str, help='The path to list.')

    # 'put' subcommand - upload a file or folder to the store:
    parser_up = subparsers.add_parser('put', help='Put a local file into the store.')
    parser_up.add_argument('-B', '--backup-and-replace', action='store_true', help='If the file already exists, move it aside using a dated backup file and replace it with the new file.')
    parser_up.add_argument('local_path', type=str, help='The local path to read.')
    parser_up.add_argument('path', type=str, help='The store path to write to.')

    # 'delete' subcommand - delete a file from the store:
    parser_rm = subparsers.add_parser('delete', help='Delete a file from the store.')
    parser_rm.add_argument('path', type=str, help='The file to delete.')

    # 'lsr-to-json' subcommand - read a file listing generated by hadoop fs -lsr ... and convert to JSON:
    parser_cv = subparsers.add_parser('lsr-to-jsonl', help='Read a hadoop fs -lsr format file listing and convert to JSONL')
    parser_cv.add_argument('input_lsr', type=str, help='The file to read, in hadoop fs -lsr format. Can be "-" for STDIN.')
    parser_cv.add_argument('output_jsonl', type=str, help='The file to output to in JSONL format. Can be "-" for STDOUT.')

    # Helper to tidy up WARC output folders:
    parser_wtd = subparsers.add_parser('warctidy', help='Tidy up the crawler output folder.')
    parser_wtd.add_argument('--prefix', 
        help="The location of the root of the crawler storage.", 
        default="/mnt/gluster/fc")

    # 'nominet' subcommand to grab and ingest files from nominet:
    parser_nom = subparsers.add_parser('nominet', help='Update files from Nominet.')

    # And PARSE it:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)    

    # Set up client:
    st = WebHDFSStore(args.service)

    # Ops:
    logger.debug("Got args: %s" % args)
    if args.op == 'list':
        if args.csv:
            writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDNAMES, extrasaction='ignore')
            writer.writeheader()
            for info in st.list(args.path, args.recursive):
                writer.writerow(info)
        elif args.jsonl:
            for info in st.list(args.path, args.recursive):
                print(json.dumps(info))
        elif args.ids:
            for info in st.list(args.path, args.recursive):
                print(info['id'])
        else:
            for info in st.list(args.path, args.recursive):
                print(info['file_path_s'])
    elif args.op == 'get':
        reader = st.read(args.path, offset = args.offset, length = args.length)
        if args.local_path == '-':
            for data in reader:
                sys.stdout.buffer.write(data)
        else:
            if os.path.exists(args.local_path):
                raise Exception("Path %s already exists! Refusing to overwrite.")
            else:
                with open(args.local_path, 'wb') as f:
                    for data in reader:
                        f.write(data)

    elif args.op == 'put':
        st.put(args.local_path, args.path, args.backup_and_replace)
    elif args.op == 'rm':
        st.rm(args.path)
    elif args.op == 'lsr-to-jsonl':
        # Input
        if args.input_lsr == '-':
            reader = sys.stdin
        else:
            reader = open(args.input_lsr, 'r')
        # Output
        if args.output_jsonl == '-':
            writer = sys.stdout
        else:
            writer = open(args.output_jsonl, 'w')

        # Convert and write out:
        for item in st.lsr_to_items(reader):
            writer.write(json.dumps(item))
            writer.write("\n")

        # Close up
        if reader is not sys.stdin.buffer:
            reader.close()
        if writer is not sys.stdout:
            writer.close()
    elif args.op == 'warctidy':
        warc_tidy_up(args.prefix)
    elif args.op == 'nominet':
        ingest_from_nominet(st)
    else:
        raise Exception("Not implemented!")


if __name__ == "__main__":
    main()
