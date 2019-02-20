import argparse
import sys
import datetime
from tasks.ingest.log_analysis_hadoop import CrawlLogLine


def print_launch_stats(key, launch_stats):
    print("%s\t%s\t%s\t%s\t%s\t%s" % (key,
                                      launch_stats[key].get('timestamp', None),
                                      launch_stats[key].get('launch', False),
                                      launch_stats[key].get('source', '-'),
                                      launch_stats[key].get('first_url', '-'),
                                      launch_stats[key]['stats']))


def main(argv=None):
    # Set up the args:
    parser = argparse.ArgumentParser('Generate crawl reports from log files.')
    parser.add_argument('filename', metavar='filename', help="Path of file or folder to parse.")

    # Parse the args:
    args = parser.parse_args()

    #
    launch_stats = dict()
    launch_stats['-'] = { 'stats': {'count': 0 } }
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
                    print_launch_stats(key, launch_stats)
                launch_stats[key] = {'timestamp': c.timestamp, 'first_url': c.url, 'source': c.source, 'stats': { 'bytes': 0, 'count': 0 } }

            if c.hop_path == '-':
                launch_stats[key]['launch'] = True

            if key in launch_stats:
                launch_stats[key]['stats']['count'] += 1
                if c.content_length != '-':
                    launch_stats[key]['stats']['bytes'] += int(c.content_length)
                # Also tot-up status codes:
                sc = launch_stats[key]['stats'].get('status_codes', dict())
                sc[c.status_code] = sc.get(c.status_code, 0) + 1
                launch_stats[key]['stats']['status_codes'] = sc
            else:
                launch_stats['-']['stats']['count'] += 1

    for key in launch_stats:
        print_launch_stats(key,launch_stats)


if __name__ == "__main__":
    sys.exit(main())
