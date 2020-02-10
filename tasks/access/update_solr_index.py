import luigi
import os
import re
import datetime
import logging
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
import luigi.contrib.ssh

from tasks.access.hdfs_list_warcs import ListWarcsForDateRange
from lib.webhdfs import WebHdfsPlainFormat

logger = logging.getLogger('luigi-interface')


class CopyToRemote(luigi.Task):
	# Copy file argument to destination directory argument on server argument
	input_file = luigi.Parameter()
	dest_dir = luigi.Parameter()
	host = luigi.Parameter()

	def ssh(self):
		return {'host': 'mapred2', 'key_file': '~/.ssh/id_rsa', 'username': 'hdfs'}

	def run(self):
		logger.info("Uploading {} to {};{}".format(self.input_file, self.host, self.dest_dir))
		self.output().put(self.input_file)

	def output(self):
		fpfile = self.dest_dir + os.path.basename(self.input_file)
		return luigi.contrib.ssh.RemoteTarget(path=fpfile, host=self.host)

class SolrVerify(luigi.Task):
	warcs_file = luigi.Parameter()
	solr_api = luigi.Parameter()
	task_namespace = "access.index"

class SolrIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
	warcs_file = luigi.Parameter()
	solr_api = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	task_namespace = "access.index"
	# Although date is a parameter in CopyToHDFS, it's not used. Thus it is added here to the prefix
	ymdhms = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
	mapred_dir = '/home/hdfs/gitlab/discovery_supervisor/warcs2solr/'
	hdfs_processing_dir = '/9_processing/warcs2solr/'
	# These are the annotation and whitelist files on the mapred2 server
	annotations = 'annotations.json'
	whitelist = 'openAccessSurts.txt'

	def requires(self):
		return CopyToRemote(input_file=self.warcs_file, dest_dir=self.mapred_dir, host='mapred2')

	def ssh(self):
		return {'host': 'mapred2', 'key_file': '~/.ssh/id_rsa', 'username': 'hdfs'}

	def jar(self):
		return '/home/hdfs/gitlab/discovery_supervisor/warcs2solr/warc-hadoop-indexer-3.0.0-job.jar'

	def main(self):
		return 'uk.bl.wa.hadoop.indexer.WARCIndexerRunner'

	def args(self):
		# derive mapreduce job arguments
		annotation_fpfile = self.mapred_dir + self.annotations
		whitelist_fpfile = self.mapred_dir + self.whitelist
		warc_config = 'warc-'
		if self.stream == 'domain':
			warc_config += 'npld-dc' + self.year
		elif self.stream == 'selective':
			warc_config += '-selective'
		else:
			warc_config += 'npld-fc' + self.year
		warc_config_fpfile = self.mapred_dir + warc_config + '.conf'
		output_dir = self.hdfs_processing_dir + self.ymdhms + '/output/log'
		return [
			"-Dmapred.compress.map.output=true",
			"-Dmapred.reduce.max.attempts=2",
			"-files", annotation_fpfile + ',' + whitelist_fpfile,
			"-c", warc_config_fpfile,
			"-i", self.input().path,
			"-o", output_dir,
			"-a"
		]

	def output(self):
		full_path = self.hdfs_processing_dir + self.ymdhms + '/output/_SUCCESS'
		return luigi.contrib.hdfs.HdfsTarget(full_path, format=WebHdfsPlainFormat(use_gzip=False))


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
			limit=20,
			tracking_db_url=self.tracking_db_url
		)

	# Index list of WARCs into main Solr search service.
	# Ensure WARCs now in Solr.
	# Flag WARCs as in Solr.
	def run(self):
		# test some data received
		listsize = os.stat(self.input().path)
		if listsize.st_size == 0:
			logger.warning("No WARCs listed for {} {}".format(self.stream, self.year))
			self.output().touch()

		else:
			# Submit MapReduce job of WARCs list for Solr search service
			logger.info("Submitting WARCs for {} {} into {}".format(self.stream, self.year, self.solr_api))
			logger.info("List of WARCs to be submitted: {}".format(self.input().path))
			solr_index_task = SolrIndexer(warcs_file=self.input().path, solr_api=self.solr_api, stream=self.stream, year=self.year)
			yield solr_index_task
			if not solr_index_task.complete():
				logger.error("SOMETHING WENT WRONG WITH SOLR INDEXING")

			else:
				# Verifying WARCs in Solr
				logger.info("Verifying submitted WARCs in Solr")
				solr_verify_task = SolrVerify(warcs_file=self.input().path, solr_api=self.solr_api)
				yield solr_verify_task

				if not solr_verify_task.complete():
					logger.error("SOMETHING WENT WRONG WITH SOLR INDEX VERIFYING")

				# Mark WARCs in trackdb as indexed into Solr

				# mark luigi task as done
				self.output().touch()

	def output(self):
		logger.debug('---- SolrIndexAndVerify output: ----------------------------------')
		return luigi.LocalTarget("/tmp/{}".format(self.stream + '-' + self.year))


if __name__ == '__main__':
	luigi.run(['access.index.SolrIndexAndVerify', '--workers', '4'])
