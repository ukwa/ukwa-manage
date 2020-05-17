from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

class MRCdxIndexerJarJob(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    jar_path = '/usr/local/bin/warc-hadoop-recordreaders-job.jar'
    num_reducers = 5

    def configure_args(self):
        super(MRCdxIndexerJarJob, self).configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=5,
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
