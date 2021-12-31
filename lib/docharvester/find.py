'''
Document Harvester: Finding Documents

Contains helper classes for extracting documents from crawl log file lines, etc.

These are routinely run from mr_log_job.py crawl log processing jobs, but can also be run from:

- Import from Kafka.
- Command-line import from older Luigi state files.

'''

import os
import re
import json
import surt
import logging
import tempfile
from datetime import datetime
from urllib.parse import urlparse
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

class DocumentsFoundDB():

    sql = """
        INSERT INTO documents_found (
            document_url, 
            wayback_timestamp,
            filename,
            source,
            landing_page_url,
            launch_id,
            job_name,
            size,
            title,
            target_id,
            status
        )
        VALUES (
            %(document_url)s,
            %(wayback_timestamp)s,
            %(filename)s,
            %(source)s,
            %(landing_page_url)s,
            %(launch_id)s,
            %(job_name)s,
            %(size)s,
            %(title)s,
            %(target_id)s,
            %(status)s
        ) ON CONFLICT DO NOTHING;
    """

    docs = []
    docs_sent = 0

    def __init__(self, db_uri, batch_size=10000 ):
        self.batch_size = batch_size
        self.db_uri = db_uri

    def add_document(self, item):
        logger.debug("Got document: %s" % item)
        if not 'status' in item:
            item['status'] = 'NEW'
        for key in ['title', 'target_id']:
            if not key in item:
                item[key] = None        
        logger.debug("Updated document with defaults: %s" % item)
        self.docs.append(item)
        # Periodically flush documents to the DB
        if len(self.docs) >= self.batch_size:
            self.flush_added_documents()


    def flush_added_documents(self):
        logger.info(f"Sending {len(self.docs)} documents...")
        self._send(self.docs)
        self.docs_sent += len(self.docs)
        self.docs = []
        logger.info(f"Total documents sent: {self.docs_sent}")

    def _open_connection(self):
        connection = psycopg2.connect(self.db_uri)
        return connection


    def _send(self, item_generator):
        # Open connection:
        connection = self._open_connection()
        connection.autocommit = True

        # Send all the documents in large batches:
        with connection.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, self.sql, item_generator, page_size=1000)

        # Commit at the end:
        connection.commit()

    # Pass in calls wiht an update function that will create an update for a document:
    def update_new_documents(self, doc_updater, status_filter="NEW", apply_updates=True, batch_size=100):
        conn = self._open_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # find and lock up to 100 NEW documents:
            cur.execute(f"SELECT * FROM documents_found WHERE status = '{status_filter}' ORDER BY wayback_timestamp ASC LIMIT {batch_size} FOR UPDATE SKIP LOCKED")
            for row in cur.fetchall():
                # get row as a plain dict:
                doc = dict(row)
                print(doc)
                # Here we call the passed function, to perform whatever work is needed on this document:
                updoc = doc_updater.update(doc)
                # If there is an update, apply it:
                if updoc and apply_updates:
                    # Now we use the returned document to update the records:
                    update_sql = """
                    UPDATE documents_found SET 
                    title=%(title)s,
                    landing_page_url=%(landing_page_url)s,
                    target_id=%(target_id)s,
                    status=%(status)s
                    WHERE document_url = %(document_url)s
                    """
                    cur.execute(update_sql, updoc)
            # And finish the transation:
            conn.commit()

         


class CrawlLogLine(object):
    """
    Parsers Heritrix3 format log files, including annotations and any additional extra JSON at the end of the line.
    See https://heritrix.readthedocs.io/en/latest/operating.html#crawl-log
    """
    def __init__(self, line):
        """
        Parse from a standard log-line.
        :param line:
        """
        (self.timestamp, self.status_code, self.content_length, self.url, self.hop_path, self.via,
            self.mime, self.thread, self.start_time_plus_duration, self.hash, self.source,
            self.annotation_string) = re.split(" +", line.strip(), maxsplit=11)
        # Account for any JSON 'extra info' ending, strip or split:
        if self.annotation_string.endswith(' {}'):
            self.annotation_string = self.annotation_string[:-3]
        elif ' {"' in self.annotation_string and self.annotation_string.endswith('}'):
            self.annotation_string, self.extra_json = re.split(re.escape(' {"'), self.annotation_string, maxsplit=1)
            self.extra_json = '{"%s' % self.extra_json
        # And split out the annotations:
        self.annotations = self.annotation_string.split(',')

        # Some regexes:
        self.re_ip = re.compile('^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
        self.re_tries = re.compile('^\d+t$')
        self.re_dol = re.compile('^dol:\d+') # Discarded out-links - make a total?

    @classmethod
    def from_log_line(cls, line):
        pass
    
    def to_log_line(self):
        line = "%(timestamp)s %(status_code)6s %(size)10s %(url)s %(hop_path)s %(via)s %(mimetype)s #%(thread)s %(start_time_plus_duration)s %(content_digest)s %(seed)s %(annotations)s {}" % self
        return line

    @classmethod
    def from_log_msg(cls, msg):
        pass

    def to_log_msg(self):
        pass

    def stats(self):
        """
        This generates the stats that can be meaningfully aggregated over multiple log lines.
        i.e. fairly low-cardinality fields.

        :return:
        """
        stats = {
            'lines' : '', # This will count the lines under each split
            'status_code': self.status_code,
            'content_type': self.mime,
            'hop': self.hop_path[-1:],
            'sum:content_length': self.content_length
        }
        # Add in annotations:
        for annot in self.annotations:
            # Set a prefix based on what it is:
            prefix = ''
            if self.re_tries.match(annot):
                prefix = 'tries:'
            elif self.re_ip.match(annot):
                prefix = "ip:"
            # Skip high-cardinality annotations:
            if annot.startswith('launchTimestamp:'):
                continue
            # Only emit lines with annotations:
            if annot != "-":
                stats["%s%s" % (prefix, annot)] = ""
        return stats

    def host(self):
        """
        Extracts the host, depending on the protocol.

        :return:
        """
        if self.url.startswith("dns:"):
            return self.url[4:]
        else:
            return urlparse(self.url).hostname

    def hour(self):
        """
        Rounds-down to the hour.

        :return:
        """
        return "%s:00:00Z" % self.timestamp[:13]

    def day(self):
        """
        Rounds-down to the day.

        :return:
        """
        return "%sT00:00:00Z" % self.timestamp[:10]

    def date(self):
        return self.parse_date(self.timestamp)

    def parse_date(self, timestamp):
        return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")


def url_to_surt(url):
    return surt.surt(url)

class CrawlLogExtractors(object):

    def __init__(self, job_name=None, targets_path=None):
        self.job_name = job_name
        # Setup targets if provided:
        if targets_path:
            # Find the unique watched seed list:
            logger.debug("Loading path: %s" % targets_path)
            with open(targets_path) as fin:
                targets = json.load(fin)
        else:
            targets = []
        # Assemble the Watched SURTs:
        target_map = {}
        watched = set()
        for t in targets:
            # Build-up reverse mapping
            for seed in t['seeds']:
                target_map[seed] = t['id']
                # And not any watched seeds:
                if t['watched']:
                    watched.add(seed)

        # Convert to SURT form:
        watched_surts = []
        for url in watched:
            watched_surts.append(url_to_surt(url))
        logger.debug("WATCHED SURTS %s" % json.dumps(watched_surts, indent=2))

        self.watched_surts = watched_surts
        self.target_map = target_map

    def analyse_log_file(self, log_file):
        """
        To run a series of analyses on a log file and emit results suitable for reduction.
        :param log_file:
        :return:
        """
        with log_file.open() as f:
            for line in f:
                log = CrawlLogLine(line)
                yield self.extract_documents(log)

    def target_id(self, log):
        return self.target_map.get(log.source, None)

    def extract_documents(self, log):
        """
        Check if this appears to be a potential Document for document harvesting...

        :param log:
        :return:
        """
        return self.extract_documents_from(log.url, log.status_code, log.mime, log.content_length, log.start_time_plus_duration, log.via, log.source, log.annotations)

    def extract_documents_from(self, url, status_code, mime, content_length, start_time_plus_duration, via, source, annotations ):
        """
        Check if this appears to be a potential Document for document harvesting...
        """
        # Skip non-downloads:
        if status_code == '-' or status_code == '' or int(int(status_code) / 100) != 2:
            return
        # Skip de-duplicated records:
        if 'duplicate:digest' in annotations:
            return
        # Check the URL and Content-Type:
        if "application/pdf" in mime:
            document_surt = url_to_surt(url)
            landing_page_surt = url_to_surt(via)
            for prefix in self.watched_surts:
                #logger.warning("Looking for prefix '%s' in '%s' and '%s'" % (prefix,document_surt, landing_page_surt))
                # Are both URIs under the same watched SURT:
                if document_surt.startswith(prefix) or landing_page_surt.startswith(prefix):
                    # Proceed to extract metadata and pass on to W3ACT:
                    doc = {
                        'wayback_timestamp': start_time_plus_duration[:14],
                        'landing_page_url': via,
                        'document_url': url,
                        'filename': os.path.basename(urlparse(url).path),
                        'size': int(content_length),
                        # Add some more metadata to the output so we can work out where this came from later:
                        'job_name': self.job_name,
                        'launch_id': None,
                        'source': source
                    }
                    #logger.info("Found document: %s" % doc)
                    return doc

        return None

