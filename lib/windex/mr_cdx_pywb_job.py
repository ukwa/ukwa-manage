import logging
import requests
from mrjob.job import MRJob
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def run_cdx_index_job(items, cdx_endpoint):
    with tempfile.NamedTemporaryFile('w+') as fpaths:
        # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
        for item in items:
            fpaths.write("%s\n" % item['file_path_s'])
        # Make sure temp file is up to date:
        fpaths.flush()

        return run_cdx_index_job_with_file(fpaths.name, cdx_endpoint)                

def run_cdx_index_job_with_file(input_file, cdx_endpoint):
    # Set up the CDX indexer map-reduce job:
    mr_job = MRCDXIndexer(args=[
        '-r', 'hadoop',
        '--cdx-endpoint', cdx_endpoint,
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

        cdx11 = CDX11Indexer(inputs=[warc_path], output=cdx_file, cdx11=True, post_append=True)
        cdx11.process_all()

        warc_path = urlparse(warc_uri).path

        self.set_status('cdxj_indexer of %s complete.' % warc_path)

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
                # Key on host to distribute load:
                host_surt = parts[0].split(")", 1)[0]
                # Reconstruct the CDX line and yield:
                yield host_surt, " ".join(parts)

    def reducer_init(self):
        self.ocdx = OutbackCDXClient(self.options.cdx_endpoint)

    def reducer(self, key, values):
        counter = 0
        for value in values:
            counter += 1
            # Send to OutbackCDX:
            self.ocdx.add(value)

        # Also emit some stats from the job:
        yield key, counter

    def reducer_final(self):
        self.ocdx.send()


class OutbackCDXClient():

    def __init__(self, cdx_server, buf_max=1000):
          self.cdx_server = cdx_server
          self.buf_max = buf_max
          self.postbuffer = []
          self.session = requests.Session()
    
    def send(self):
        chunk = "\n".join(self.postbuffer)
        r = self.session.post(self.cdx_server, data=chunk.encode('utf-8'))
        if (r.status_code == 200):
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