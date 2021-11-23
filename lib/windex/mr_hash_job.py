import os
import json
import tempfile
from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

def run_hash_index_job(items):
    with tempfile.NamedTemporaryFile('w+') as fpaths:
        # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
        for item in items:
            fpaths.write("%s\n" % item['file_path_s'])
        # Make sure temp file is up to date:
        fpaths.flush()

        return run_hash_index_job_with_file(fpaths.name)                

def run_hash_index_job_with_file(input_file):
    # Set up the CDX indexer map-reduce job:
    mr_job = MRHashIndexerJarJob(args=[
        '-r', 'hadoop',
        '--filelist', input_file, # < local input file, don't let mrjob upload it
        input_file # Dummy - no MrJob-managed input file needed
        ])

    # Run and gather output:
    stats = {}
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            print(key, value)

    # Raise an exception if the output looks wrong:
    if not "total_sent_records_i" in stats:
        raise Exception("Hash job stats has no total_sent_records_i value! \n%s" % json.dumps(stats))
    if stats['total_sent_records_i'] == 0:
        raise Exception("Hash job stats has total_sent_records_i == 0! \n%s" % json.dumps(stats))

    return stats

class MRHashIndexerJarJob(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    jar_path = os.environ.get('WARC_HADOOP_INDEXER_JOB_JAR_PATH', '/usr/local/bin/warc-hadoop-indexer-job.jar')
    #jar_path = '/usr/local/bin/warc-hadoop-recordreaders-job.jar'

    def configure_args(self):
        super(MRHashIndexerJarJob, self).configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=4,
            help="Number of reducers to use.")
        self.add_passthru_arg(
            '--filelist', required=True,
            help="List of HDFS paths to process.")

    def steps(self):
        return [JarStep(
            jobconf={
                'mapred.compress.map.output':'true',
                'mapred.output.compress': 'true',
                'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec'
            },
            jar=self.jar_path,
            main_class='uk.bl.wa.hadoop.mapreduce.hash.HdfsFileHasher',
            args=[
                GENERIC_ARGS, # This ensures the various jobconf etc. params are included.
                '-i', str(self.options.filelist),
                '-o', OUTPUT,
                '-r', str(self.options.num_reducers), # n.b. if you pass as int it fails mysteriously, use str!
                ]
            )]

if __name__ == '__main__':
    MRHashIndexerJarJob.run()
