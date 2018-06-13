import sys
from urlparse import urlparse
import json
import argparse
import logging
from kafka import KafkaConsumer

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


def show_raw_stream(consumer):
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        j = json.loads(message.value.decode('utf-8'))
        if 'parentUrl' in j:
            # This is a discovered URL stream:
            print("%010d:%04d: %-80s via %-80s" % (message.offset, message.partition,
                                                j['url'][-80:], j['parentUrl'][-80:]))
        elif 'status_code' in j:
            # This is a crawled-event stream:
            print("%s %-80s via %-80s" % (j['timestamp'], j['url'][-80:], j.get('via', 'NONE')[-80:]))
        else:
            # This is unknown!
            logger.error("Unrecognised stream! %s" % message.value)
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))
            return


def summarise_stream(consumer, max_messages=-1):
    tot = {}
    msg_count = 0
    for message in consumer:
        msg_count += 1
        if max_messages > 0 and msg_count > max_messages:
            break
        j = json.loads(message.value.decode('utf-8'))
        if 'parentUrl' in j:
            url = j['url']
            via = j['parentUrl']
            urlp = urlparse(url)
            viap = urlparse(via)
            stats = tot.get(urlp.hostname,{})
            if viap.hostname != urlp.hostname and not 'via' in stats:
                stats['via'] = via
                print(j) # hop, isSeed, parentUrlMetadata.pathFromSeed
            stats['tot'] = stats.get('tot', 0) + 1
            tot[urlp.hostname] = stats
    print("URL Host\tDiscovered Via\tTotal URLs")
    for host in tot:
        print("%s\t%s\t%i" %(host, tot[host].get('via', '-'), tot[host]['tot']))


def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='bootstrap_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument('-o', '--offset', dest='starting_at', type=str, default="earliest",
                        help="Whether to start at the earliest or latest message [default: %(default)s]")
    parser.add_argument("-q", "--queue", dest="queue", default="uris.crawled.fc", required=False,
                        help="Name of queue to inspect. [default: %(default)s]")

    args = parser.parse_args()
    #

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(args.queue, auto_offset_reset=args.starting_at,
                             bootstrap_servers=args.bootstrap_server,
                             consumer_timeout_ms=1*1000)
    summarise_stream(consumer)


if __name__ == "__main__":
    sys.exit(main())
