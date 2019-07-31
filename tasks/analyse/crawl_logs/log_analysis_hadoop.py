import re
import os
import json
import logging
import datetime
from urllib.parse import urlparse
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop
from luigi.contrib.hdfs.format import Plain, PlainDir
from lib.surt import url_to_surt

import lib, dateutil, six # Imported so extra_modules MR-bundle can access them
#import surt, tldextract, idna, requests, urllib3, certifi, chardet, requests_file, six # Unfortunately the surt module has a LOT of dependencies.


logger = logging.getLogger(__name__)


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

    def complete(self):
        """
        Always assume the files are there, don't bother checking (thus permissing wildcard inputs).
        :return: True
        """
        return True

class AnalyseLogFile(luigi.contrib.hadoop.JobTask):
    """
    Map-Reduce job that scans a log file for documents associated with 'Watched' targets.

    Should run locally if run with only local inputs.

    """

    task_namespace = 'analyse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    log_paths = luigi.ListParameter()
    targets_path = luigi.Parameter(default=None)
    from_hdfs = luigi.BoolParameter(default=False)

    # This can be set to 1 if there is intended to be one output file. The usual Luigi default is 25.
    # Using one output file ensures the whole output is sorted but is not suitable for very large crawls.
    n_reduce_tasks = luigi.Parameter(default=25)

    extractor = None


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
        return [lib, dateutil, six]#,surt,tldextract,idna,requests,urllib3,certifi,chardet,requests_file,six]

    def init_mapper(self):
        # Set up...
        self.extractor = CrawlLogExtractors(self.job, self.launch_id, self.from_hdfs, targets_path=self.targets_path )

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
        """
        A pass-through reducer.

        :param key:
        :param values:
        :return:
        """
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


class ExtractLogsForHost(luigi.contrib.hadoop.JobTask):
    """
    Map-Reduce job that scans a log file and extracts just the logs for a particular host.

    Should run locally if run with only local inputs.

    """

    task_namespace = 'analyse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    log_paths = luigi.ListParameter()
    host = luigi.Parameter()
    from_hdfs = luigi.BoolParameter(default=False)

    # Using one output file ensures the whole output is sorted but is not suitable for very large crawls.
    n_reduce_tasks = luigi.Parameter(default=1)

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.from_hdfs))
        return reqs

    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%s.analysis.tsjson" % (self.job, self.launch_id, self.host)
        if self.from_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=PlainDir)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [lib, dateutil, six]#,surt,tldextract,idna,requests,urllib3,certifi,chardet,requests_file,six]

    def mapper(self, line):
        # Parse:
        log = CrawlLogLine(line)
        # Emit matching lines keyed by event timestamp
        if log.host() == self.host:
            yield "%s" % log.start_time_plus_duration, line

    def reducer(self, key, values):
        """
        A pass-through reducer.

        :param key:
        :param values:
        :return:
        """
        # Just pass documents through:
        for value in values:
            yield key, value


class ExtractLogsByHost(luigi.contrib.hadoop.JobTask):
    """
    Map-Reduce job that scans a log file and extracts just the logs for a particular host.

    Should run locally if run with only local inputs.

    """

    task_namespace = 'analyse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    log_paths = luigi.ListParameter()
    host = luigi.Parameter()
    lines = luigi.IntParameter(default=5)
    from_hdfs = luigi.BoolParameter(default=False)

    # Using one output file ensures the whole output is sorted but is not suitable for very large crawls.
    n_reduce_tasks = luigi.Parameter(default=50)

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.from_hdfs))
        return reqs

    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%s.analysis.tsjson" % (self.job, self.launch_id, self.host)
        if self.from_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=PlainDir)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [lib, dateutil, six]#,surt,tldextract,idna,requests,urllib3,certifi,chardet,requests_file,six]

    def mapper(self, line):
        # Parse:
        log = CrawlLogLine(line)
        # Emit lines keyed by host and event timestamp
        yield "%s|%s" % (log.host(), log.timestamp), line

    # For tracking lines emitted:
    current_host = None
    lines_emitted = 0

    def reducer(self, key, values):
        """
        A pass-through reducer, should split and emit the host part of the key

        :param key:
        :param values:
        :return:
        """

        # Get the host part of the key
        host = key.split('|')[0]
        # keep track of the host we're handling:
        if self.current_host != host:
            self.current_host = host
            self.lines_emitted = 0
        for value in values:
            # Limit the lines we emit for each host:
            if self.lines_emitted < self.lines:
                self.lines_emitted += 1
                yield host, value

    # The following is ensures data is partitioned by the host, but ordered by host and timestamp within that.

    def extra_streaming_arguments(self):
        return [("-partitioner","org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")]

    def jobconfs(self):
        '''
        Extend the job configuration to support the keys and partitioning we want.
        :return:
        '''
        jc = super(ExtractLogsByHost, self).jobconfs()

        # Ensure the first three fields are all treated as the key:
        jc.append("map.output.key.field.separator=|")
        #jc.append("stream.num.map.output.key.fields=3")
        # Ensure only the host part of the key (first value) is used for partitioning:
        jc.append("mapred.text.key.partitioner.options=-k1,1")
        # Compress the output and the mapper output:
        #jc.append("mapred.output.compress=true")
        #jc.append("mapred.compress.map.output=true")
        #jc.append("mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")

        return jc


class SummariseLogFiles(luigi.contrib.hadoop.JobTask):
    """
    Based on old code developed for TRAC issue 2478.

    example input: /heritrix/output/logs/crawl*-2014*/crawl.log*.gz
    """
    log_paths = luigi.ListParameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    on_hdfs = luigi.BoolParameter(default=False)

    task_namespace = 'analyse'

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.on_hdfs))
        return reqs

    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%i.summary.tsjson" % (self.job, self.launch_id, len(self.log_paths))
        if self.on_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=Plain)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [lib]#,surt,tldextract,idna,requests,urllib3,certifi,chardet,requests_file,six]

    def mapper(self, line):
        line_test = line.strip().split(None, 11)
        
        if len(line_test) == 12: # standard line (it is a seed if discovery and referrer = "-")
            log_time, status, size, url, discovery_path, referrer, mime, thread, \
                request_time, hash, ignore, annotations  \
                = line_test

        elif len(line_test) == 10: # seed - missing referrer and path
            log_time, status, size, url, mime, thread, \
            request_time, hash, ignore, annotations  \
            = line_test
            discovery_path = "-" # heritrix docs and some code indicate "blank" values, but in the log "-" appears to be in use
            referrer = "-"
        else: 
            logger.info('Log line has unexpected values.\nExpected: 10 or 12. Actual: %s\nLine: %s' % (len(line_test), line))
            return
          
        if not status.isdigit(): return
            
        url_state = ""    
        HTTPStatus = int(status)
            
        if (HTTPStatus == 404  # not comprehensive!     
            and discovery_path == "-" and referrer == "-"): # seed
            url_state = "Has Dead Seeds"    
                      
        if 200 <= HTTPStatus < 400:
            url_state = "Live"
        
        if url_state == "": return
            
        if url_state == "Live":    
            data = {
                "mime": "".join([i if ord(i) < 128 else "" for i in mime]),
            }

            for anno in annotations.split(","):
                if ":" not in anno:
                    continue
                key, value = anno.split(":", 1)
                if key == "ip":
                    data["ip"] = value
                if key == "1":
                    data["virus"] = value.split()[-2]
                    
            data["url_state"] = "Live"
            
        else:
            data = {
                "ip": {},
                "mime": {},
                "virus": {},
                "url_state":"Has Dead Seeds"
            }
            

        parsed_url = urlparse(url)
        host = re.sub("^(www([0-9]+)?)\.", "", parsed_url[1])                        
                                
        yield host, json.dumps(data)

    def reducer(self, key, values):
        sec_level_domains = ["ac", "co", "gov", "judiciary", "ltd", "me", "mod", "net", "nhs", "nic", "org",
                             "parliament", "plc", "sch"]

        current_host_data = {
            "ip": {},
            "mime": {},
            "virus": {},
            "url_state": {}
        }

        host = key
        for value in values:
            data = json.loads(value)
            logger.info(">>> host: %s data: %s " % (host, data))                    
            
            # Some values can only be accumulated for Live hosts    
            if data["url_state"] == "Live":            
                if "ip" in data.keys():
                    if data["ip"] in current_host_data["ip"].keys():
                        current_host_data["ip"][data["ip"]] += 1
                    else:
                        current_host_data["ip"][data["ip"]] = 1
                
                if "mime" in data.keys():                    
                    if data["mime"] in current_host_data["mime"].keys():
                        current_host_data["mime"][data["mime"]] += 1
                    else:
                        current_host_data["mime"][data["mime"]] = 1
                    
                if "virus" in data.keys():
                    if data["virus"] in current_host_data["virus"].keys():
                        current_host_data["virus"][data["virus"]] += 1
                    else:
                        current_host_data["virus"][data["virus"]] = 1
            
            # We assume that even if a host appeared live at some point in the crawl,
            # it can be considered to have dead seeds if at any other point we encountered one.            
            if "url_state" in data.keys(): 
                if current_host_data["url_state"] != "Has Dead Seeds":
                    current_host_data["url_state"] = data["url_state"]                    


        current_host_data["host"] = host
        current_host_data["tld"] = host.split(".")[-1]
        auth = host.split(".")
        if len(auth) > 2:
            sld = host.split(".")[-2]
            if sld in sec_level_domains:
                current_host_data["2ld"] = sld


        yield host, json.dumps(current_host_data)


class ListDeadSeeds(luigi.contrib.hadoop.JobTask):

    """
    Essentially does the same as SummariseLogFiles on the same input, but 
    we only output dead seeds here. In that task, we add them to the JSON.

    The reason we don't output a straight list within that process 
    (i.e. why are we processing the same logs twice?) is that we output 
    JSON there and MR within Luigi doesn't lend itself obviously to 
    either operating on JSON input or outputting multiple files 
    within the same task. TODO.
    """

    log_paths = luigi.ListParameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    on_hdfs = luigi.BoolParameter(default=False)

    task_namespace = 'analyse'

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.on_hdfs))
        return reqs

 
    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%i.dead-seeds.txt" % (self.job, self.launch_id, len(self.log_paths))
        if self.on_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=Plain)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [lib]

    def mapper(self, line):
  
        line_test = line.strip().split(None, 11)
        
        if len(line_test) == 12: # standard line (it is a seed if discovery and referrer = "-")
            _, status, _, url, discovery_path, referrer, _, _, _, _, _, _ \
            = line_test

        elif len(line_test) == 10: # seed - missing referrer and path
            _, status, _, url, _, _, _, _, _, _  \
            = line_test
            discovery_path = "-" # heritrix docs and some code indicate "blank" values, but in the log "-" appears to be in use
            referrer = "-"
        else: 
            logger.info('Log line has unexpected values.\nExpected: 10 or 12. Actual: %s\nLine: %s' % (len(line_test), line))
            return
            
        if not status.isdigit(): 
            logger.info('Log line has unexpected status.\nExpected: Numeric. Actual: %s\nLine: %s' % (status, line))
            return
        
        url_state = ""    
        HTTPStatus = int(status)
            
        if (HTTPStatus == 404  # not comprehensive!     
            and discovery_path == "-" and referrer == "-"): # seed
            url_state = "Dead"    
                      
        if 200 <= HTTPStatus < 400:
            url_state = "Live"
            
        if url_state == "": return
           
        yield url, url_state

        
    def reducer(self, key, values):
        host = key
        
        current_url_state = ""
        for value in values:
        
            # We assume that even if a host appeared dead at some point in the crawl,
            # it can be considered live if at any other point we had a successful request.
            if value in ("Live", "Dead"):        
                if current_url_state != "Live":
                    current_url_state = value
        
        if current_url_state == "Dead":
            yield host, ""
            
            
            
class CountStatusCodes(luigi.contrib.hadoop.JobTask):

    """
    Count of Each Heritrix/HTTP Status returned  
    https://github.com/internetarchive/heritrix3/wiki/Status-Codes    
    """

    log_paths = luigi.ListParameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    on_hdfs = luigi.BoolParameter(default=False)

    task_namespace = 'analyse'

    def requires(self):
        reqs = []
        for log_path in self.log_paths:
            logger.info("LOG FILE TO PROCESS: %s" % log_path)
            reqs.append(InputFile(log_path, self.on_hdfs))
        return reqs

 
    def output(self):
        out_name = "task-state/%s/%s/crawl-logs-%i.status-code-counts.txt" % (self.job, self.launch_id, len(self.log_paths))
        if self.on_hdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=Plain)
        else:
            return luigi.LocalTarget(path=out_name)

    def extra_modules(self):
        return [lib]

    def mapper(self, line):
  
        status = line.strip().split(None, 11)[1]
    
        if not status.lstrip('-').isdigit(): 
            logger.info('Log line has unexpected status.\nExpected: Numeric. Actual: %s\nLine: %s' % (status, line))
            return 
                   
        yield status, "1" # we will count the 1s in the reducer. any single character would suffice though.

        
    def reducer(self, key, values):
        status = key
        
        i = 0
        for value in values:
            i+=1
           
        yield status, str(i) 
            
            

            
if __name__ == '__main__':
    #luigi.run(['analyse.SummariseLogFiles', '--job', 'dc', '--launch-id', '20170220090024',
    #           '--log-paths', '[ "test/logs/fragment-of-a-crawl.log" ]',
    #           '--local-scheduler'])

    #luigi.run(['analyse.SummariseLogFiles', '--job', 'dc', '--launch-id', '20170515',
    #          '--log-paths', '[ "/heritrix/output/logs/dc0-20170515/crawl.log.cp00001-20170610062435" ]',
    #           '--on-hdfs', '--local-scheduler'])

    luigi.run(['analyse.AnalyseLogFile', '--job', 'dc2019', '--launch-id', '20190714151618',
               '--log-paths', '[ "/heritrix/output/dc2019/20190714151618/logs/crawl.log.*" ]',
               '--local-scheduler'])

    #luigi.run(['analyse.AnalyseLogFile', '--job', 'dc', '--launch-id', '20170220090024',
    #           '--log-paths', '[ "/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl.log.cp00001-20170211224931", "/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl.log.cp00001-20130605082749" ]',
    #           '--targets-path', '/Users/andy/Documents/workspace/pulse/python-shepherd/tasks/process/extract/test-data/crawl-feed.2017-01-02T2100.frequent',
    #           '--local-scheduler'])

    #luigi.run(['analyse.AnalyseLogFiles', '--date-interval', '2017-02-10-2017-02-12', '--local-scheduler'])
    #luigi.run(['analyse.AnalyseLogFile', '--job', 'weekly', '--launch-id', '20170220090024', '--local-scheduler'])
