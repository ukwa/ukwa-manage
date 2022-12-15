import re
import json
import tempfile
import logging
import requests
from mrjob.job import MRJob
from types import MethodType
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def run_cdx_index_job_with_file(input_file, cdx_endpoint):
    # Read input file list in as items:
    items = []
    with open(input_file) as fin:
        for line in fin:
            items.append({
                'file_path_s': line.strip()
            })
    # Run the given job:
    return run_cdx_index_job(items, cdx_endpoint)

def run_cdx_index_job(items, cdx_endpoint):
    # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
    # Set up the CDX indexer map-reduce job:
    args = [
        '-r', 'hadoop',
        '--cdx-endpoint', cdx_endpoint
        ]
    for item in items:
        # Use the URI form with the implied host, i.e. hdfs:///path/to/file.input
        args.append("hdfs://%s" % item['file_path_s'])

    # Set up the job:
    mr_job = MRCDXIndexer(args=args)

    # Run and gather output:
    stats = {}
    with mr_job.make_runner() as runner:
        # Block the running from auto-decompressing the inputs:
        def _manifest_uncompress_commands_just_dont(self):
            return []
            #foo.bark = new_bark.__get__(foo, Dog)
        runner._manifest_uncompress_commands = _manifest_uncompress_commands_just_dont.__get__(runner, runner.__class__)
        # Log the configuration so we can debug any configuration problems:
        logger.info(f"Full MrJob runner configuration: {runner._opts}")
        # And run the job:
        runner.run()
        results = process_job_output(mr_job, runner)
        stats = results['metrics']

    # Raise an exception if the output looks wrong:
    if not "total_record_count" in stats:
        raise Exception("CDX job stats has no total_record_count value! \n%s" % json.dumps(stats))
    if stats['total_record_count'] == 0:
        raise Exception("CDX job stats has total_record_count == 0! \n%s" % json.dumps(stats))

    return results

def process_job_output(mr_job, runner):
    # Process the results to separate file and job level information:
    results = {
        'files': {},
        'metrics': {},
    }
    for key, value in mr_job.parse_output(runner.cat_output()):
        # Normalise key if needed:
        if key.startswith("__"):
            key = key.replace("__", "", 1)
        # Check if this is a per-file stat, and split it up as needed:
        if key.startswith("by_file "):
            parts = key.split(" ", maxsplit=3)
            filepath = parts[1]
            metric = parts[2]
            if len(parts) == 4:
                remainder = parts[3]
            else:
                remainder = None
            # Get current metrics for this file:
            file_metrics = results['files'].get(filepath, {})
            # Store as the appropriate type of metric
            if metric.endswith("_l") or metric.endswith("_i"):
                i = file_metrics.get(key, 0)
                file_metrics[metric] = i + int(value)                
            elif metric.endswith("_ss"):
                file_metrics[metric] = remainder.split(" ")
            else:
                file_metrics[metric] = remainder
            # And store:
            results['files'][filepath] = file_metrics
        else:
            # Update counter for task-level metric:
            i = results['metrics'].get(key, 0)
            results['metrics'][key] = i + int(value)

    return results


class MRCDXIndexer(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=5,
            help="Number of reducers to use.")
        self.add_passthru_arg(
            '-C', '--cdx-endpoint', required=True,
            help="CDX service endpoint to use, e.g. 'http://server/collection'.")

    def jobconf(self):
        return {
            'mapred.job.name': '%s_%s' % ( self.__class__.__name__, datetime.now().isoformat() ),
            'mapred.compress.map.output':'true',
            'mapred.output.compress': 'true',
            'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
            'mapred.reduce.tasks': str(self.options.num_reducers),
            'mapreduce.job.reduces': str(self.options.num_reducers)
        }

    # Sort all the values on each Reducer, so the last event wins in the case of timestamp collisions:`
    SORT_VALUES = True

    # Using mapper_raw means MrJob arranges for a copy of each WARC to be placed where we can get to it:
    # (This breaks data locality, but streaming through large files is not performant because they get read into memory)
    # (A FileInputFormat that could reliably split block GZip files would be the only workable fix)
    # (But TBH this is pretty fast as it is)
    def mapper_raw(self, warc_path, warc_uri):
        from cdxj_indexer.main import CDX11Indexer

        cdx_file = 'index.cdx'

        # Using CDX11:
        # CDX N b a m s k r M S V g
        # com,example)/ 20170306040206 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 1242 784 example.warc.gz
        cdx11 = CDX11Indexer(inputs=[warc_path], output=cdx_file, cdx11=True, post_append=True)
        
        # The warc_path we get passed in is just the local temp filename.
        # From here, need to use the HDFS file URI instead and extract the path:
        warc_path = urlparse(warc_uri).path

        # Some WARCs throw an exception during indexing. To avoid everything getting stuck we need to catch these errors and 
        # record them for later investigation rather than killing the whole job:
        self.set_status('Running cdxj_indexer on %s ...' % warc_path)
        try:
            cdx11.process_all()
        except Exception as e:
            yield f"__by_file {warc_path} warc_cdx_indexing_exception_s {str(e)}", 1
            # Do not process output of failed process:
            return


        self.set_status('cdxj_indexer of %s complete.' % warc_path)
        
        first_url = None
        last_url = None
        host_surts = set()
        content_types = set()
        extended_scheme_urls = 0
        with open(cdx_file) as f:
            for line in f:
                line = line.strip()
                parts = line.split(" ")
                # Skip header:
                if parts[0] == 'CDX':
                    continue
                # Count the lines:
                self.increment_counter('CDX', 'CDX_LINES', 1)
                # Replace `warc_path` with proper HDFS path:
                parts[10] = warc_path
                # Store the first and last URLs
                url = parts[2]
                ts = parts[1]
                if first_url is None:
                    first_url = f"{ts}/{url}"
                last_url = f"{ts}/{url}"
                # Key on host to distribute load and collect stats:
                # Deal with URL schemes of the form screenshot:http://host/path -> screenshot_http_host
                match = re.search(r"^[a-z]+:http(.*)", url)
                if match:
                    # Assume this is e.g. screenshot:http: and replace the first : with a _:
                    url = url.replace(':','-', 1)
                    urlp = urlparse(url)
                    host_key = f"{urlp.scheme}-{urlp.hostname}"
                    extended_scheme_urls = extended_scheme_urls + 1
                else:
                    url_surt = parts[0]
                    host_key = url_surt.split(")", 1)[0]
	
                # Skip DNS lines:
                if host_key.startswith("dns:"):
                    continue

                # Skip lines with malformed status codes, but record the fact that they are present:
                status_code = parts[4]
                # A simple '-' is permitted as e.g. resource records have no status code, so don't validate that:
                if status_code != '-':
                    # If it's not a '-', then it should be a valid status code:
                    if not status_code.isdigit() or int(status_code) < 0 or int(status_code) >= 600:
                        yield f"__by_file {warc_path} warc_malformed_status_code_record_count_l", 1
                        continue

                # Normalise the content type:
                parts[3] = parts[3].lower()
                content_types.add(parts[3])

                # Reconstruct the CDX line and yield (except DNS):
                yield host_key, " ".join(parts)
                yield "__total_record_count", 1

                # Also record all the host surts:
                host_surts.add(host_key)
                
                # FIXME WARNING: A lot of things are not yet enable.
                # In this class, there are large data results that we're not currently sure how to handle.

                # Too many fields, so this is not enabled:
                #yield f"__by_file {warc_path} host_{host_key}_count_i", 1

                # Yield a counter for the number of WARC records included:
                yield f"__by_file {warc_path} warc_record_count_l", 1

                # Also record content types and status codes:
                # FIXME quite a lot of fields, so not enabled at present
                #yield f"__by_file {warc_path} content_type_{parts[3]}_count_l", 1
                #yield f"__by_file {warc_path} status_code_{parts[4]}_count_l", 1

                # Also total up bytes:
                if parts[8] != "-":
                    bytes = int(parts[8])
                    yield f"__by_file {warc_path} warc_record_bytes_l", bytes
                    # Host-level stats means a LOT of separate fields per WARC, so we're not using this at present:
                    #yield f"__by_file {warc_path} host_{host_key}_bytes_l", bytes
                    # FIXME quite a lot of fields, so not enabled at present
                    #yield f"__by_file {warc_path} content_type_{parts[3]}_bytes_l", bytes
                    #yield f"__by_file {warc_path} status_code_{parts[4]}_bytes_l", bytes

        # Also return the first+last indexable URLs for each WARC:
        yield f"__by_file {warc_path} first_url_ts_s {first_url}", 1
        yield f"__by_file {warc_path} last_url_ts_s {last_url}", 1

        # Return the set of host SURTs in this WARC:
        # FIXME Large field, so not enable at present
        #host_surts_list = " ".join(host_surts)
        #yield f"__by_file {warc_path} host_surts_ss {host_surts_list}", 1

        # Content types:
        # FIXME Large field, so not enable at present
        #content_types_list = " ".join(content_types)
        #yield f"__by_file {warc_path} content_types_ss {content_types_list}", 1

        # Extended URLs counter:
        yield f"__by_file {warc_path} extended_scheme_url_count_l", extended_scheme_urls

        # Yield a counter for the number of WARCs processed:
        yield "__warc_file_count", 1


    def reducer_init(self):
        self.ocdx = OutbackCDXClient(self.options.cdx_endpoint)

    def reducer(self, key, values):
        # Pass on data fields:
        if key.startswith("__"):
            # Only integer fields are expected to have multiple values:
            yield key, sum(values)
        else:
            # Otherwise send to OutbackCDX:
            for value in values:
                # Send to OutbackCDX:
                self.ocdx.add(value)

    def reducer_final(self):
        self.ocdx.send()
        yield 'total_sent_record_count', self.ocdx.total_sent


class OutbackCDXClient():

    def __init__(self, cdx_server, buf_max=1000):
          self.cdx_server = cdx_server
          self.buf_max = buf_max
          self.postbuffer = []
          self.session = requests.Session()
          self.total_sent = 0
    
    def send(self):
        chunk = "\n".join(self.postbuffer)
        r = self.session.post(self.cdx_server, data=chunk.encode('utf-8'))
        if (r.status_code == 200):
            self.total_sent += len(self.postbuffer)
            self.postbuffer = []
            logger.info("POSTed to cdxserver: %s" % self.cdx_server)
            return
        else:
            logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
            logger.error("Failed submission was: %s" % chunk.encode('utf-8'))
            raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
 
    def add(self, cdx11_line):
        self.postbuffer.append(cdx11_line)
        if len(self.postbuffer) > self.buf_max:
            self.send()



if __name__ == '__main__':
    MRCDXIndexer.run()