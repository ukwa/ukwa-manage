
import json
import logging
from kafka import KafkaConsumer

from lib.docharvester.find import CrawlLogLine, CrawlLogExtractors, DocumentsFoundDB


logger = logging.getLogger(__name__)

class KafkaDocumentFinder():
    
    def __init__(self, args):
        self.max_messages = args.max_messages

        # Refine args:
        if args.latest:
            args.starting_at = 'latest'
        else:
            args.starting_at = 'earliest'

        if args.timeout != -1:
            # Convert to ms:
            args.timeout = 1000*args.timeout

        # Store the args:
        self.args = args

        # Now set up the document extractor:
        self.extractor = CrawlLogExtractors(targets_path=args.targets)


    def __enter__(self):
        # Get the args:
        args = self.args

        # To consume messages and auto-commit offsets
        self.consumer = KafkaConsumer(
            args.topic, 
            auto_offset_reset=args.starting_at,
            bootstrap_servers=args.bootstrap_server,
            consumer_timeout_ms=args.timeout,
            max_partition_fetch_bytes=128*1024,
            enable_auto_commit=False,
            group_id=args.group_id,
            client_id=args.client_id)

        # And set up target DB
        self.dh = DocumentsFoundDB(batch_size=1)

        return self


    def find(self):
        msg_count = 0
        match_count = 0
        for message in self.consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            j = json.loads(message.value.decode('utf-8'))

            # Only include Heritrix entries, which have a 'thread' key:
            if 'thread' in j:

                #print(message.value.decode('utf-8'))
                # Swap nulls for "-
                for k in j:
                    if j[k] == None:
                        j[k] = "-"

                #line = "%(timestamp)s %(status_code)6s %(size)10s %(url)s %(hop_path)s %(via)s %(mimetype)s #%(thread)s %(start_time_plus_duration)s %(content_digest)s %(seed)s %(annotations)s {}" % j
                #log = CrawlLogLine(line)

                doc = self.extractor.extract_documents_from(
                    j["url"],
                    j["status_code"],
                    j["mimetype"], 
                    j["size"], 
                    j["start_time_plus_duration"],
                    j["via"],
                    j["seed"]
                )

                if doc:
                    # Pick up the job name if present:
                    if "crawl_name" in j:
                        doc["job_name"] = j["crawl_name"]

                    # Send to the Documents Found DB:
                    self.dh.add_document(doc)

                    # Also print:
                    print(doc)

                    # Match counter:
                    match_count += 1

                    # If we've hit the max, commit:
                    if self.max_messages and match_count >= self.max_messages:
                        self.consumer.commit()
                        break

            # Keep count of messages and occasionally report progress:
            msg_count += 1
            if msg_count % 10000 == 0:
                logger.debug(f"MSG {msg_count}: {message}")
                # Occasionlly commit so we don't re-read all messages every time (given a Group ID):
                logger.info(f"Committing Kafka consumer offsets after {msg_count} messages processed.")
                self.consumer.commit()



    def __exit__(self, exc_type, exc_value, traceback):
        self.dh.flush_added_documents()
        stats = {}
        stats["total_sent_records_i"] = self.dh.docs_sent
        print(stats)