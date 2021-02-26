import json
import tempfile
from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

def run_log_job(items):
    with tempfile.NamedTemporaryFile('w+') as fpaths:
        # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
        for item in items:
            fpaths.write("%s\n" % item['file_path_s'])
        # Make sure temp file is up to date:
        fpaths.flush()

        return run_log_job_with_file(fpaths.name, cdx_endpoint)                

def run_cdx_index_job_with_file(input_file, cdx_endpoint):
    # Set up the CDX indexer map-reduce job:
    mr_job = MRCdxIndexerJarJob(args=[
        '-r', 'hadoop',
        input_file, # < local input file, mrjob will upload it
        ])

    # Run and gather output:
    stats = {}
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            # Normalise key if needed:
            key = key.lower()
            if not key.endswith("_i"):
                key = "%s_i" % key
            # Update counter for the stat:
            i = stats.get(key, 0)
            stats[key] = i + int(value)

    # Raise an exception if the output looks wrong:
    if not "total_sent_records_i" in stats:
        raise Exception("CDX job stats has no total_sent_records_i value! \n%s" % json.dumps(stats))
    if stats['total_sent_records_i'] == 0:
        raise Exception("CDX job stats has total_sent_records_i == 0! \n%s" % json.dumps(stats))

    return stats


class MRLogAnalysisJob(MRJob):

    def mapper_init(self):
        # Set up...
        self.extractor = CrawlLogExtractors(self.job, self.launch_id, self.from_hdfs, targets_path=self.targets_path )

    def mapper(self, _, line):
        # Parse:
        log = CrawlLogLine(line)
        # Extract basic data for summaries, keyed for later aggregation:
        yield "BY_DAY_HOST_SOURCE,%s,%s,%s" % (log.day(), log.host(), log.source), json.dumps(log.stats())
        # Scan for documents, yield sorted in crawl order:
        doc = self.extractor.extract_documents(log)
        if doc:
            yield "DOCUMENT,%s" % log.start_time_plus_duration, doc
        # Check for dead seeds:
        if (int(int(log.status_code) / 100) != 2 and int(int(log.status_code) / 100) != 3  # 2xx/3xx are okay!
                and log.hop_path == "-" and log.via == "-"):  # seed
            yield "DEAD_SEED,%s,%s" % (log.url, log.start_time_plus_duration), line

    def reducer(self, key, values):
      # Just pass documents through:
        if key.startswith("DOCUMENT") or key.startswith("DEAD_SEED"):
            for value in values:
                yield key, value
        else:
            # Build up summaries of other statistics:
            summaries = {}
            for value in values:
                properties = json.loads(value)
                for pkey in properties:
                    # For 'sum:XXX' properties, sum the values:
                    if pkey.startswith('sum:') and properties[pkey] != '-':
                        summaries[pkey] = summaries.get(pkey, 0) + int(properties[pkey])
                        continue
                    # Otherwise, default behaviour is to count occurrences of key-value pairs.
                    if properties[pkey]:
                        # Build a composite key for keys that have non-empty values:
                        prop = "%s:%s" % (pkey, properties[pkey])
                    else:
                        prop = pkey
                    # Aggregate:
                    summaries[prop] = summaries.get(prop, 0) + 1

            yield key, json.dumps(summaries)


class CrawlLogLine(object):
    """
    Parsers Heritrix3 format log files, including annotations and any additional extra JSON at the end of the line.
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


class CrawlLogExtractors(object):

    def __init__(self, job, launch, from_hdfs, targets_path=None):
        self.job = job
        self.launch_id = launch
        # Setup targets if provided:
        if targets_path:
            if from_hdfs:
                hdfs_client = luigi.contrib.hdfs.HdfsClientApache1()
                logger.warning("Loading targets using client: %s" % hdfs_client)
                logger.warning("Loading targets from HDFS: %s" % targets_path)
                targets = luigi.contrib.hdfs.HdfsTarget(path=targets_path, format=Plain, fs=hdfs_client)
            else:
                logger.warning("Loading targets from local FS: %s" % targets_path)
                targets = luigi.LocalTarget(path=targets_path)
            # Find the unique watched seeds list:
            logger.warning("Loading: %s" % targets)
            logger.warning("Loading path: %s" % targets.path)
            targets = json.load(targets.open())
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
        logger.warning("WATCHED SURTS %s" % watched_surts)

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
        # Skip non-downloads:
        if log.status_code == '-' or log.status_code == '' or int(int(log.status_code) / 100) != 2:
            return
        # Check the URL and Content-Type:
        if "application/pdf" in log.mime:
            for prefix in self.watched_surts:
                document_surt = url_to_surt(log.url)
                landing_page_surt = url_to_surt(log.via)
                #logger.warning("Looking for prefix '%s' in '%s' and '%s'" % (prefix,document_surt, landing_page_surt))
                # Are both URIs under the same watched SURT:
                if document_surt.startswith(prefix) or landing_page_surt.startswith(prefix):
                    # Proceed to extract metadata and pass on to W3ACT:
                    doc = {
                        'wayback_timestamp': log.start_time_plus_duration[:14],
                        'landing_page_url': log.via,
                        'document_url': log.url,
                        'filename': os.path.basename(urlparse(log.url).path),
                        'size': int(log.content_length),
                        # Add some more metadata to the output so we can work out where this came from later:
                        'job_name': self.job,
                        'launch_id': self.launch_id,
                        'source': log.source
                    }
                    #logger.info("Found document: %s" % doc)
                    return json.dumps(doc)

        return None


if __name__ == '__main__':
    MRLogAnalysisJob.run()
