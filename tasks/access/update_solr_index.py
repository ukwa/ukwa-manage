import luigi
import os
import re
import datetime
import logging
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar

from tasks.access.hdfs_list_warcs import ListWarcsForDateRange
from tasks.access.update_cdx_index import CopyToHDFS
from tasks.common import state_file

logger = logging.getLogger('luigi-interface')

class SolrIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
	warcs_file = luigi.Parameter()
	solr_api = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	task_namespace = "access.index"
	# This is used to add a timestamp to the output file, so this task can always be re-run:
	timestamp = luigi.DateSecondParameter(default=datetime.datetime.now())
	# These are the annotation and whitelist files on the mapred2 server
	mapred_dir = '/home/hdfs/gitlab/discovery_supervisor/warcs2solr'
	annotations = 'annotations.json'
	whitelist = 'openAccessSurts.txt'

	def requires(self):
		logger.debug("---- SolrIndexer requires: self.warcs_file {}".format(self.warcs_file))
		# Although date is a parameter in CopyToHDFS, it's not used. Thus it is added here
		# to the prefix
		today = str(datetime.date.today())
		return CopyToHDFS(input_file=self.warcs_file, prefix="/9_processing/warcs2solr/" + today + '/')

	def ssh(self):
		return {'host': 'mapred2', 'key_file': '~/.ssh/id_rsa', 'username': 'hdfs'}

	def jar(self):
		return '/home/hdfs/gitlab/discovery_supervisor/warcs2solr/warc-hadoop-indexer-3.0.0-job.jar'

	def main(self):
		return 'uk.bl.wa.hadoop.indexer.WARCIndexerRunner'

	def args(self):
		warc_config = 'warc-'
		if self.stream == 'domain':
			warc_config += 'npld-dc' + self.year
		elif self.stream == 'selective':
			warc_config += '-selective'
		else:
			warc_config += 'npld-fc' + self.year
		warc_config += '.conf'
		logger.debug("---- SolrIndexer args: -files {}/{},{}/{}".format(self.mapred_dir, self.annotations, self.mapred_dir, self.whitelist))
		logger.debug("---- SolrIndexer args: warc_config {}/{}".format(self.mapred_dir, warc_config))
		return [
			'-Dmapred.compress.map.output=true',
			'-Dmapred.reduce.max.attempts=2',
			"-files {}/{},{}/{}".format(self.mapred_dir, self.annotations, self.mapred_dir, self.whitelist),
			"-c {}/{}".format(self.mapred_dir, warc_config),
			"-i {}".format(self.input().path),
			"-o {}/output/log".format(self.input().path),
			'-a'
		]

	def output(self):
		timestamp = self.timestamp.isoformat()
		timestamp = timestamp.replace(':','-')
		file_prefix = os.path.splitext(os.path.basename(self.warcs_file))[0]
		logger.debug("---- SolrIndexer output: file_prefix {}".format(file_prefix))
		logger.debug("---- SolrIndexer output: timestamp {}".format(timestamp))
		return state_file(self.timestamp, 'warcs2solr', "{}-submitted-{}.txt".format(file_prefix, timestamp), on_hdfs=True)

class SolrIndexAndVerify(luigi.Task):
	tracking_db_url = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	solr_api = luigi.Parameter()
	# task_namespace defines scope of class. Without defining this, other classes
	# could call this class inside their scope, which would be wrong.
	task_namespace = "access.index"

	# status_field is set here as I don't expect it to change in any implementation, 
	# whereas the other fields are parameters to allow triggering script configuration.
	status_field = 'solr_index_ss'

	# Get list of WARCs that don't have flag set indicating already indexed in Solr
	def requires(self):
		return ListWarcsForDateRange(
			start_date=datetime.date.fromisoformat(str(self.year)+'-01-01'),
			end_date=datetime.date.fromisoformat(str(int(self.year)+1)+'-01-01') - datetime.timedelta(days=1),
			stream=self.stream,
			status_field=self.status_field,
			status_value=self.stream + '-' + self.year,
			limit=10,
			tracking_db_url=self.tracking_db_url
		)


	# Index list of WARCs into main Solr search service.
	# Ensure WARCs now in Solr.
	# Flag WARCs as in Solr.
	def run(self):
		# test some data received
		listsize = os.stat(self.input().path)
		if listsize.st_size == 0:
			logger.info("---- SolrIndexAndVerify run: No WARCs listed for {} {}".format(self.stream, self.year))
			self.output().touch()

		else:
			logger.info("---- SolrIndexAndVerify run: Submitting WARCs for {} {} into {}".format(self.stream, self.year, self.solr_api))
			logger.info("---- SolrIndexAndVerify run: WARCs list: {}".format(self.input().path))
			# Submit MapReduce job of WARCs list for Solr search service
			solr_index_task = SolrIndexer(warcs_file=self.input().path, solr_api=self.solr_api, stream=self.stream, year=self.year)
			yield solr_index_task
		

	def output(self):
		logger.debug('---- SolrIndexAndVerify output: ----------------------------------')
		return luigi.LocalTarget("/tmp/{}".format(self.stream + '-' + self.year))


if __name__ == '__main__':
	luigi.run(['access.index.SolrIndexAndVerify', '--workers', '4'])
