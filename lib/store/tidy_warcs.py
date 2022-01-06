import os
import re
import json
import shutil
import argparse
import logging
import requests
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

def send_metrics(metrics, fs):

    registry = CollectorRegistry()

    g = Gauge('ukwa_files_moved_count',
                'Total number of files moved by this task.',
                labelnames=['fs', 'kind'], registry=registry)
    g.labels(fs=fs, kind='warcprox-warcs').set(metrics['wren_warcs_moved'])

    g = Gauge('ukwa_files_count',
                'Total number of files.',
                labelnames=['fs','kind'], registry=registry)
    for kind in metrics:
        g.labels(fs=fs, kind=kind).set(metrics[kind])

    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job='warc_tidy', registry=registry)
    else:
        logger.error("No metrics PUSH_GATEWAY configured!")


def warc_tidy_up(prefix="/mnt/gluster/fc", output_json=True, do_move=True, trackdb_url='http://solr8.api.wa.bl.uk/solr/tracking/select'):
    logger.info("Scanning %s..." % prefix)

    metrics = { 
        'wren_warcs': 0,
        'wren_warcs_matched': 0,
        'wren_warcs_nomatch': 0,
        'wren_warcs_moved': 0,
    }

    # Expected filenaming:
    p = re.compile("BL-....-WEBRENDER-([a-z\-0-9]+)-([0-9]{14})-([a-z\-0-9]+)\.warc\.gz")
    # List all matching files in source directory:
    webrender_path = os.path.join(prefix, 'heritrix/wren/')
    for file_path in os.listdir(webrender_path):
        if file_path.endswith('.warc.gz'):
            metrics['wren_warcs'] += 1
            file_name = os.path.basename(file_path)
            matches = p.search(file_name)
            if matches:
                metrics['wren_warcs_matched'] += 1
                destination_folder_path = "%s/heritrix/output/%s/%s/warcs" %( prefix, matches.group(1), matches.group(2))
                if not os.path.exists(destination_folder_path):
                    raise Exception("Expected destination folder does not exist! :: %s" % destination_folder_path)
                if not os.path.isdir(destination_folder_path):
                    raise Exception("Expected destination folder is not a folder! :: %s" % destination_folder_path)
                source_file_path = os.path.join(webrender_path, file_name)
                destination_file_path = os.path.join(destination_folder_path, file_name)
                if os.path.exists(destination_file_path):
                   raise Exception("Destination file already exists! :: %s" % destination_file_path)
                if do_move:
                    logger.debug("Moving %s to %s..." %( source_file_path, destination_file_path ))
                    shutil.move( source_file_path, destination_file_path )
                    metrics['wren_warcs_moved'] += 1
            else:
                metrics['wren_warcs_nomatch'] += 1

    # Log activity
    logger.info("Moved %s files." % metrics['wren_warcs_moved'])

    # Also scan main WARCs folder:
    metrics['warcs'] = 0
    metrics['warcs_in_trackdb'] = 0
    metrics['warcs_not_in_trackdb'] = 0
    metrics['warcs_open'] = 0
    metrics['other_files'] = 0
    output_path = os.path.join(prefix, 'heritrix/output/')
    for root, dirnames, filenames in os.walk(output_path):
        for filename in filenames:
            if filename.endswith('warc.gz'):
                metrics['warcs'] += 1
                # TODO: Check if this specific file in on HDFS.
                base_path = root[len(prefix):]
                full_path = f"{base_path}/{filename}"
                logger.debug(f"Checking TrackDB for path: {full_path}")
                r = requests.get(trackdb_url, params={ 'q': f'file_path_s:"{full_path}"', 'wt': 'json'})
                count = r.json()['response']['numFound']
                if count == 0:
                    metrics['warcs_not_in_trackdb'] += 1
                else:
                    logger.info(f"Path {full_path} is in TrackDB as well as being on disk.")
                    metrics['warcs_in_trackdb'] += 1

            elif filename.endswith('warc.gz.open'):
                metrics['warcs_open'] += 1

            else:
                metrics['other_files'] += 1

    # Update metrics
    logger.info(f"warc_tidy final metrics: {metrics}")
    send_metrics(metrics, 'gluster')
    # Finally, print retults as JSON if requested:
    if output_json:
        print(json.dumps(metrics))

def main():
    # Helper to tidy up WARC output folders:
    parser = argparse.ArgumentParser(prog='tidy-warcs', description='Tidy up the WARCs in the crawler output folder.')
    parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    parser.add_argument('-S', '--scan-only', action='store_true', help='Only perform the scan, do not take any actions like moving files.')
    parser.add_argument('--prefix', 
        help="The location of the root of the crawler storage, to be recursively searched for WARC files. [default=%(default)s]", 
        default="/mnt/gluster/fc")

    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)    
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)    

    # Process args:
    if args.scan_only:
        do_move = False
    else:
        do_move = True

    # Run the tidy:
    warc_tidy_up(args.prefix, do_move=do_move)

if __name__ == "__main__":
    main()
