import re
import os
import json
import logging
from urlparse import urlparse
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop
from luigi.contrib.hdfs.format import Plain, PlainDir

import crawl # Imported so extra_modules MR-bundle can access the following:
from crawl.h3.utils import url_to_surt

logger = logging.getLogger('luigi-interface')


class CrawlLogLine(object):
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
            'sum:content_length': self.content_length,
            'host': self.host(),
            'source': self.source
        }
        # Add in annotations:
        for annot in self.annotations:
            # Set a prefix based on what it is:
            prefix = ''
            if self.re_tries.match(annot):
                prefix = 'tries:'
            elif self.re_ip.match(annot):
                prefix = "ip:"
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
        return "%s:00:00" % self.timestamp[:13]


class CrawlLogExtractors(object):

    def __init__(self, job, launch, targets_path, from_hdfs):
        self.job = job
        self.launch_id = launch
        # Setup targets:
        if from_hdfs:
            logger.warning("Loading targets from HDFS: %s" % targets_path)
            hdfs_client = luigi.contrib.hdfs.WebHdfsClient(host='hdfs', port='14000', user='hdfs')
            targets = luigi.contrib.hdfs.HdfsTarget(path=targets_path, format=Plain, fs=hdfs_client)
        else:
            logger.warning("Loading targets from local FS: %s" % targets_path)
            targets = luigi.LocalTarget(path=targets_path)
        # Find the unique watched seeds list:
        logger.warning("Loading: %s" % targets)
        logger.warning("Loading path: %s" % targets.path)
        targets = json.load(targets.open())
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
        logger.info("WATCHED SURTS %s" % watched_surts)

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
        if log.status_code == '-' or log.status_code == '' or int(log.status_code) / 100 != 2:
            return
        # Check the URL and Content-Type:
        if "application/pdf" in log.mime:
            for prefix in self.watched_surts:
                document_surt = url_to_surt(log.url)
                landing_page_surt = url_to_surt(log.via)
                # Are both URIs under the same watched SURT:
                if document_surt.startswith(prefix) and landing_page_surt.startswith(prefix):
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


class InputFile(luigi.ExternalTask):
    """
    This ExternalTask defines the Target at the top of the task chain. i.e. resources that are overall inputs rather
    than generated by the tasks themselves.
    """
    path = luigi.Parameter()
    from_hdfs = luigi.BoolParameter(default=False)

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in HDFS.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        if self.from_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=self.path)
        else:
            return luigi.LocalTarget(path=self.path)


class AnalyseLogFile(luigi.contrib.hadoop.JobTask):
    """
    Map-Reduce job that scans a log file for documents associated with 'Watched' targets.

    Should run locally if run with only local inputs.

    Input:

    {
        "annotations": "ip:173.236.225.186,duplicate:digest",
        "content_digest": "sha1:44KA4PQA5TYRAXDIVJIAFD72RN55OQHJ",
        "content_length": 324,
        "extra_info": {},
        "hop_path": "IE",
        "host": "acid.matkelly.com",
        "jobName": "frequent",
        "mimetype": "text/html",
        "seed": "WTID:12321444",
        "size": 511,
        "start_time_plus_duration": "20160127211938966+230",
        "status_code": 404,
        "thread": 189,
        "timestamp": "2016-01-27T21:19:39.200Z",
        "url": "http://acid.matkelly.com/img.png",
        "via": "http://acid.matkelly.com/",
        "warc_filename": "BL-20160127211918391-00001-35~ce37d8d00c1f~8443.warc.gz",
        "warc_offset": 36748
    }

    Note that 'seed' is actually the source tag, and is set up to contain the original (Watched) Target ID.

    Output:

    [
    {
    "id_watched_target":<long>,
    "wayback_timestamp":<String>,
    "landing_page_url":<String>,
    "document_url":<String>,
    "filename":<String>,
    "size":<long>
    },
    <further documents>
    ]

    See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint

    i.e.

    seed -> id_watched_target
    start_time_plus_duration -> wayback_timestamp
    via -> landing_page_url
    url -> document_url (and filename)
    content_length -> size

    Note that, if necessary, this process to refer to the
    cdx-server and wayback to get more information about
    the crawled data and improve the landing page and filename data.


    """

    task_namespace = 'analyse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    log_paths = luigi.ListParameter()
    targets_path = luigi.Parameter()
    from_hdfs = luigi.BoolParameter(default=False)

    extractor = None

    #n_reduce_tasks = 1 # This can be set to 1 if there is intended to be one output file. Default is 25.

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.from_hdfs))
        return reqs

    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%i.analysis.tsjson" % (self.job, self.launch_id, len(self.log_paths))
        if self.from_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=PlainDir)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [crawl]

    def init_mapper(self):
        # Set up...
        self.extractor = CrawlLogExtractors(self.job, self.launch_id, self.targets_path, self.from_hdfs)

    def jobconfs(self):
        """
        Also override number of mappers.

        :return:
        """
        jcs = super(AnalyseLogFile, self).jobconfs()
        jcs.append('mapred.map.tasks=%s' % 100)
        #jcs.append('mapred.min.split.size', ) mapred.max.split.size, in bytes. e.g. 256*1024*1024 = 256M
        return jcs

    def mapper(self, line):
        # Parse:
        log = CrawlLogLine(line)
        # Extract basic data for summaries:
        yield "TOTAL", json.dumps(log.stats())
        yield "BY-HOUR %s" % log.hour(), json.dumps(log.stats())
        yield "BY-HOST %s" % log.host(), json.dumps(log.stats())
        yield "BY-SOURCE %s" % log.source, json.dumps(log.stats())
        yield "BY-TARGET %s" % self.extractor.target_id(log), json.dumps(log.stats())
        # Scan for documents:
        doc = self.extractor.extract_documents(log)
        if doc:
            yield "DOCUMENT", doc

    def reducer(self, key, values):
        """
        A pass-through reducer.

        :param key:
        :param values:
        :return:
        """
        # Just pass documents through:
        if key.startswith("DOCUMENT"):
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
                    # Otherwise, efault behaviour is to count occurrences of key-value pairs.
                    if properties[pkey]:
                        # Build a composite key for keys that have non-empty values:
                        prop = "%s:%s" % (pkey, properties[pkey])
                    else:
                        prop = pkey
                    # Aggregate:
                    summaries[prop] = summaries.get(prop, 0) + 1

            yield key, json.dumps(summaries)


if __name__ == '__main__':
    luigi.run(['analyse.AnalyseLogFile', '--job', 'weekly', '--launch-id', '20170220090024',
               '--log-paths', '[ "/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl.log.cp00001-20170211224931", "/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl.log.cp00001-20130605082749" ]',
               '--targets-path', '/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl-feed.2017-01-02T2100.frequent',
               '--local-scheduler'])
    #luigi.run(['analyse.AnalyseLogFiles', '--date-interval', '2017-02-10-2017-02-12', '--local-scheduler'])
    #luigi.run(['analyse.AnalyseLogFile', '--job', 'weekly', '--launch-id', '20170220090024', '--local-scheduler'])
