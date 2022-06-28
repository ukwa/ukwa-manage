import os
import re
import json
import surt
import logging
import tempfile
from datetime import datetime
from urllib.parse import urlparse

from lib.docharvester.find import DocumentsFoundDB, CrawlLogLine, CrawlLogExtractors

from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

logger = logging.getLogger(__name__)

def run_log_job_with_file(input_file, targets, docs_found_db_uri):
    # Read input file list in as items:
    items = []
    with open(input_file) as fin:
        for line in fin:
            items.append({
                'file_path_s': line.strip()
            })
    # Run the given job:
    return run_log_job(items, targets, docs_found_db_uri)


def run_log_job(items, targets, docs_found_db_uri):
    # Set up the args
    args=[
        '-r', 'hadoop',
    ]
    if targets:
        args += ['-T', targets]

    for item in items:
        # Use the URI form with the implied host, i.e. hdfs:///path/to/file.input
        args.append("hdfs://%s" % item['file_path_s'])

   # Set up the map-reduce job:
    mr_job = MRLogAnalysisJob(args)

    # Run and gather output:
    dh = DocumentsFoundDB(db_uri=docs_found_db_uri)
    stats = {}
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            ##print(key, value)
            # Find any extracted documents and record them:
            if key.startswith("DOCUMENT"):
                doc = json.loads(value)
                dh.add_document(doc)
                print(key, value)
            elif key.startswith("SEED"):
                print(key, value)
            elif key.startswith("BY_DAY_HOST_SOURCE"):
                pass
            ## Normalise key if needed:
            #key = key.lower()
            #if not key.endswith("_i"):
            #    key = "%s_i" % key
            ## Update counter for the stat:
            #i = stats.get(key, 0)
            #stats[key] = i + int(value)
            
    # And now flush the added documents:
    dh.flush_added_documents()
    stats["total_sent_records_i"] = dh.docs_sent

    # Raise an exception if the output looks wrong:
    if not "total_sent_records_i" in stats:
        raise Exception("Log job stats has no total_sent_records_i value! \n%s" % json.dumps(stats))
    if stats['total_sent_records_i'] == 0:
        # This is not necessarily wrong for document harvesting:
        logger.warning("Log job stats has total_sent_records_i == 0! \n%s" % json.dumps(stats))

    return stats


class MRLogAnalysisJob(MRJob):

    # Include needed files from this project:
    # (see https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html#using-other-python-modules-and-packages)
    DIRS = ["../../lib"]

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=10,
            help="Number of reducers to use.")
        # Add a file arg as this will handle including it in the job:
        self.add_file_arg(
            '-T', '--targets', required=False,
            help="The W3ACT Targets Crawl Feed file to use to determine which Targets are Watched.")

    def jobconf(self):
        return {
            'mapred.job.name': '%s_%s' % ( self.__class__.__name__, datetime.now().isoformat() ),
            'mapred.compress.map.output':'true',
            'mapred.output.compress': 'true',
            'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
            'mapreduce.map.java.opts' : '-Xmx6g',
            'mapred.reduce.tasks': str(self.options.num_reducers),
            'mapreduce.job.reduces': str(self.options.num_reducers), # Newer property is called this 
        }

    def mapper_init(self):
        # Document Harvester:
        if self.options.targets:
            logger.warn(f"Loading targets from {self.options.targets}...")
            self.extractor = CrawlLogExtractors(job_name="frequent-npld", targets_path=self.options.targets)
        else:
            self.extractor = None

    def mapper(self, _, line):
        # Parse:
        log = CrawlLogLine(line)
        # Extract basic data for summaries, keyed for later aggregation:
        yield "BY_DAY_HOST_SOURCE,%s,%s,%s" % (log.day(), log.host(), log.source), json.dumps(log.stats())
        # Scan for documents, yield sorted in crawl order:
        if self.extractor:
            doc = self.extractor.extract_documents(log)
            if doc:
                doc['logline'] = line
                yield "DOCUMENT,%s" % log.start_time_plus_duration, json.dumps(doc)
        # Output results from seeds (empty hop path, and via is the same URL or is missing):
        if log.hop_path == "-" and (log.via == "-" or (log.via == log.url)):
            yield "SEED,%s,%s" % (log.url, log.start_time_plus_duration), json.dumps(log.stats())

    def reducer(self, key, values):
      # Just pass documents through:
        if key.startswith("DOCUMENT") or key.startswith("SEED"):
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


if __name__ == '__main__':
    MRLogAnalysisJob.run()
