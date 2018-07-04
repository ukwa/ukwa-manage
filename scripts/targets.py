import sys
import json
import logging
import argparse
from tasks.ingest.w3act import TargetList

# Set up a logging handler:
handler = logging.StreamHandler()
# handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
handler.setFormatter(formatter)

# Attach to root logger
logging.root.addHandler(handler)

# Set default logging output for all modules.
logging.root.setLevel(logging.WARNING)

# Set logging for this module and keep the reference handy:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(argv=None):
    parser = argparse.ArgumentParser('Interact with W3ACT, extracting data etc.')
    parser.add_argument('-f', '--frequency', dest='frequency', type=str, default="daily",
                        help="The crawl frequency to look at when filtering targets [default: %(default)s]")
    parser.add_argument('action', metavar='action', help="Action to perform. One of 'extract'.")

    args = parser.parse_args()

    if args.action == 'extract':
        # Find the source files:
        source = TargetList().output()
        if not source.exists():
            logger.error("Targets file %s does not exist! Has it been updated yet? Are you running this task on the right server?" % source.path )
            return

        print("Extracting %s" % args.frequency)
        # Load the targets:
        with source.open() as f:
            all_targets = json.load(f)

        # Grab detailed target data:
        logger.info("Filtering detailed information for %i targets..." % len(all_targets))

        # Filter...
        targets = []
        for t in all_targets:
            if t['field_crawl_frequency'] is None:
                logger.warning("No crawl frequency set for %s" % t)
            elif t['field_crawl_frequency'].lower() == args.frequency.lower():
                for furl in t['fieldUrls']:
                    print(furl['url'])

    else:
        print("Unknown action")


if __name__ == "__main__":
    sys.exit(main())
