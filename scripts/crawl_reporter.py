import argparse
import sys
import datetime
from tasks.ingest.log_analysis_hadoop import CrawlLogLine


def main(argv=None):
    # Set up the args:
    parser = argparse.ArgumentParser('Generate crawl reports from log files.')
    parser.add_argument('filename', metavar='filename', help="Path of file or folder to parse.")

    # Parse the args:
    args = parser.parse_args()

    #
    launch_stats = dict()
    #
    with open(args.filename) as f:
        for line in f.readlines():
            c = CrawlLogLine(line)
            key = c.host()
            is_new = False
            if c.hop_path == '-' or c.hop_path[-1:] == 'P':
                if key in launch_stats:
                    # Counts as new if it is >5mins away:
                    if abs(c.parse_date(launch_stats[key]['timestamp']) - c.date()) > datetime.timedelta(minutes=5):
                       is_new = True
                else:
                    is_new = True

            if is_new:
                if key in launch_stats:
                    print(key,launch_stats[key])
                launch_stats[key] = {'timestamp': c.timestamp, 'stats': { 'bytes': 0} }

            if key in launch_stats:
                if c.content_length != '-':
                    launch_stats[key]['stats']['bytes'] += int(c.content_length)

    for key in launch_stats:
        print("%s\t%s\t%s" % (key, launch_stats[key]['timestamp'], launch_stats[key]['stats']))


if __name__ == "__main__":
    sys.exit(main())
