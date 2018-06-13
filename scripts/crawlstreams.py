import sys
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


def show_raw_stream(queue='uris.tocrawl.fc', starting_at='earliest', bootstrap_servers=None):
    # Defaults:
    if bootstrap_servers is None:
        bootstrap_servers = ['localhost:9092']

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(queue, auto_offset_reset=starting_at,
                             bootstrap_servers=bootstrap_servers)
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        j = json.loads(message.value.decode('utf-8'))
        if 'parentUrl' in j:
            # This is a discovered URL stream:
            print("%s:%d:%d: %-80s via %-80s" % (message.topic, message.offset,
                                                  message.partition, j['url'][-80:], j['parentUrl'][-80:]))
        elif 'status_code' in j:
            # This is a crawled-event stream:
            print("%s:%d:%d: %-80s via %-80s" % (message.topic, message.offset,
                                                 message.partition , j['url'][-80:], j.get('via', 'NONE')[-80:]))
        else:
            # This is unknown!
            logger.error("Unrecognised stream! %s" % message.value)
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))
            return



def main(argv=None):
    parser = argparse.ArgumentParser('(Re)Launch URIs into crawl queues.')
    parser.add_argument('-k', '--kafka-bootstrap-server', dest='kafka_server', type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s) to use [default: %(default)s]")
    parser.add_argument("-q", "--queue", dest="queue", default="uris.crawled.fc", required=False,
                        help="Name of queue to inspect. [default: %(default)s]")

    args = parser.parse_args()
    #
    show_raw_stream(queue=args.queue, bootstrap_servers=args.kafka_server)


if __name__ == "__main__":
    sys.exit(main())
