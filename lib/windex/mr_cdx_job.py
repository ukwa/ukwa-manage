import json
import tempfile
from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

def run_cdx_index_job(items, cdx_endpoint):
    with tempfile.NamedTemporaryFile('w+') as fpaths:
        # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
        for item in items:
            fpaths.write("%s\n" % item['file_path_s'])
        # Make sure temp file is up to date:
        fpaths.flush()
                
        # Set up the CDX indexer map-reduce job:
        mr_job = MRCdxIndexerJarJob(args=[
            '-r', 'hadoop',
            '--cdx-endpoint', cdx_endpoint,
            fpaths.name, # < local input file, mrjob will upload it
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

class MRCdxIndexerJarJob(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    jar_path = '/usr/local/bin/warc-hadoop-recordreaders-job.jar'

    def configure_args(self):
        super(MRCdxIndexerJarJob, self).configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=4,
            help="Number of reducers to use.")
        self.add_passthru_arg(
            '-C', '--cdx-endpoint', required=True,
            help="CDX service endpoint to use, e.g. 'http://server/collection'.")

    def steps(self):
        return [JarStep(
            jobconf={
                'mapred.compress.map.output':'true',
                'mapred.output.compress': 'true',
                'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec'
            },
            jar=self.jar_path,
            main_class='uk.bl.wa.hadoop.mapreduce.cdx.ArchiveCDXGenerator',
            args=[
                GENERIC_ARGS, # This ensures the various jobconf etc. params are included.
                '-i', INPUT,
                '-o', OUTPUT,
                '-r', str(self.options.num_reducers), # n.b. if you pass as int it fails mysteriously, use str!
                '-w',
                '-h',
                '-m', '',
                '-t', self.options.cdx_endpoint,
                '-c', "CDX N b a m s k r M S V g"
                ]
            )]

if __name__ == '__main__':
    MRCdxIndexerJarJob.run()
