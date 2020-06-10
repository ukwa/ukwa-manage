import os
import json
import tempfile
from mrjob.job import MRJob
from mrjob.step import JarStep, INPUT, OUTPUT, GENERIC_ARGS
from mrjob.protocol import TextProtocol

def run_solr_index_job(items, zks, collection, config, annotations, oa_surts):
    with tempfile.NamedTemporaryFile('w+') as fpaths:
        # This needs to read the TrackDB IDs in the input file and convert to a set of plain paths:
        for item in items:
            fpaths.write("%s\n" % item['file_path_s'])
        # Make sure temp file is up to date:
        fpaths.flush()
                
        # Set up the CDX indexer map-reduce job:
        mr_job = MRSolrIndexerJarJob(args=[
            '-r', 'hadoop',
            '--solr-zookeepers', zks,
            '--solr-collection', collection,
            '--config', config,
            '--annotations', annotations,
            '--oa-surts', oa_surts,
            '--warclist', fpaths.name, # < local input file
            config # Dummy - no input needed
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
        
        # Print stats:
        for k in stats:
            print("%s > %s" %(k, stats[k]))

        # Raise an exception if the output looks wrong:
        if not "num_records_i" in stats:
            raise Exception("Solr job stats has no num_records_i value! \n%s" % json.dumps(stats))
        if stats['num_dropped_records_i'] > 0:
            raise Exception("Solr job stats has num_dropped_records_i > 0! \n%s" % json.dumps(stats))

        return stats

class MRSolrIndexerJarJob(MRJob):

    OUTPUT_PROTOCOL = TextProtocol

    jar_path = '/usr/local/bin/warc-hadoop-indexer-job.jar'

    def configure_args(self):
        super(MRSolrIndexerJarJob, self).configure_args()
        self.add_passthru_arg(
            '-R', '--num-reducers', default=4,
            help="Number of reducers to use.")
        self.add_passthru_arg(
            '-S', '--solr-collection', required=True,
            help="Solr collection to send data to.")
        self.add_passthru_arg(
            '-Z', '--solr-zookeepers', required=True,
            help="The Zookeepers that hold the Solr collections, like 'HOST:PORT,HOST:PORT'")
        self.add_passthru_arg(
            '--config', required=True,
            help="Configuration file to use for the webarchive-discovery indexer.")
        self.add_passthru_arg(
            '--annotations', required=True,
            help="Annotations file to use to enrich records.")
        self.add_passthru_arg(
            '--oa-surts', required=True,
            help="Open Access SURTs file to use to mark records as open access.")
        self.add_passthru_arg(
            '--warclist', required=True,
            help="List of HDFS paths to WARCS to process.")

    def steps(self):
        return [JarStep(
            # Compress intermediate results but not the (brief) output:
            jobconf={
                'mapred.compress.map.output':'true',
                'mapred.map.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
                'mapred.output.compress': 'false',
			    'mapred.reduce.max.attempts': '2'
            },
            jar=self.jar_path,
            main_class='uk.bl.wa.hadoop.indexer.WARCIndexerRunner',
            args=[
                GENERIC_ARGS, # This ensures the various jobconf etc. params are included.
			    "-files", '%s#annotations.json,%s#openAccessSurts.txt' % ( self.options.annotations, self.options.oa_surts),
			    "-c", self.options.config,
			    "-i", self.options.warclist, # Always use local file path.
			    "-o", OUTPUT,
			    "-a", # Apply annotations
			    "-w", # Wait while the job runs
                "--num-reducers", str(self.options.num_reducers), # An 'int' fails to run!
                "--solr-zookeepers", self.options.solr_zookeepers,
                "--solr-collection", self.options.solr_collection
            ]
        )]

if __name__ == '__main__':
    MRSolrIndexerJarJob.run()
