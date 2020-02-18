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
from lib.targets import TrackingDBStatusField

logger = logging.getLogger('luigi-interface')

class CopyToRemote(luigi.Task):
	# Copy file argument to destination directory argument on server argument
	input_file = luigi.Parameter()
	dest_dir = luigi.Parameter()
	mapred_host = luigi.Parameter()

	def run(self):
		self.output().put(self.input_file)

	def output(self):
		fpfile = self.dest_dir + os.path.basename(self.input_file)
		return luigi.contrib.ssh.RemoteTarget(path=fpfile, host=self.mapred_host)

class SolrIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
	warcs_file = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	mapred_host = luigi.Parameter()
	mapred_user = luigi.Parameter()
	mapred_dir = luigi.Parameter()
	# These are the annotation and whitelist files on the mapreduce server
	annotations = luigi.Parameter()
	whitelist = luigi.Parameter()
	warc_indexer_jar = luigi.Parameter()
	hdfs_processing_dir = luigi.Parameter()
	solr_api = luigi.Parameter()

	task_namespace = "access.index"
	ymdhms = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

	def requires(self):
		return CopyToRemote(input_file=self.warcs_file, dest_dir=self.mapred_dir, mapred_host=self.mapred_host)

	def ssh(self):
		return {'host': self.mapred_host, 'key_file': '~/.ssh/id_rsa', 'username': self.mapred_user}

	def jar(self):
		return self.warc_indexer_jar

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
		output_dir = self.hdfs_processing_dir + self.ymdhms + '/'
		return [
			"-Dmapred.compress.map.output=true",
			"-Dmapred.reduce.max.attempts=2",
			"-files", annotation_fpfile + ',' + whitelist_fpfile,
			"-c", warc_config_fpfile,
			"-i", self.input().path,
			"-o", output_dir,
			"-a",
			"-w"
		]

	def output(self):
		return luigi.contrib.hdfs.HdfsTarget(path=self.hdfs_processing_dir + self.ymdhms + '/_SUCCESS', format=luigi.contrib.hdfs.PlainFormat())


class SolrVerify(luigi.Task):
	warc = luigi.Parameter()
	solr_api = luigi.Parameter()
	task_namespace = "access.index"

	def run(self):
		with open(self.output().path, 'w') as vo:
			logger.debug("==== writing out {}".format(self.output().path))
			vo.write("WARC verified to be in Solr - {}".format(self.warc))

	def output(self):
		return luigi.LocalTarget("/tmp/luigi/{}-solr_verified".format(os.path.basename(self.warc.strip())))


class MarkWarc(luigi.Task):
	warc = luigi.Parameter()
	tracking_db_url = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	status_field = luigi.Parameter()

	def input(self):
		logger.debug("==== MarkWarc input: {}-{}".format(self.stream, self.year))
		return self.stream + '-' + self.year

	def output(self):
		fp_warc = 'hdfs://hdfs:54310' + self.warc
		return TrackingDBStatusField(doc_id=fp_warc, field=self.status_field, value=self.input(), trackdb=self.tracking_db_url)


class SolrIndexWarcs(luigi.Task):
	tracking_db_url = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	mapred_host = luigi.Parameter()
	mapred_user = luigi.Parameter()
	mapred_dir = luigi.Parameter()
	annotations = luigi.Parameter()
	whitelist = luigi.Parameter()
	warc_indexer_jar = luigi.Parameter()
	hdfs_processing_dir = luigi.Parameter()
	solr_api = luigi.Parameter()
	verify_only = luigi.BoolParameter(default=False)

	# globals for looping through warcs in warc list
	warc_num = 0
	warc_item = ''
	warc_list = []
	# task_namespace defines scope of class. Without defining this, other classes
	# could call this class inside their scope, which would be wrong.
	task_namespace = "access.index"

	# status_field is set here as I don't expect it to change in any implementation, 
	# whereas the other fields are parameters to allow triggering script configuration.
	status_field = 'solr_index_ss'

	# Boolean to capture any process failures
	solr_index_verify_mark_fail = False

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
			logger.warning("==== No WARCs listed for {} {}".format(self.stream, self.year))
			with open(self.output().path, 'w') as out:
				out.write("No WARCs listed for {} {}".format(self.stream, self-year))

		else:
			if not self.verify_only:
				# Submit MapReduce job of WARCs list for Solr search service
				logger.info("---- Submitting WARCs for {} {} into {}".format(self.stream, self.year, self.solr_api))
				logger.info("---- List of WARCs to be submitted: {}".format(self.input().path))
				solr_index_task = SolrIndexer(
					warcs_file=self.input().path, stream=self.stream, year=self.year, solr_api=self.solr_api,
					mapred_host=self.mapred_host, mapred_user=self.mapred_user, mapred_dir=self.mapred_dir,
					annotations=self.annotations, whitelist=self.whitelist,  warc_indexer_jar=self.warc_indexer_jar,
					hdfs_processing_dir=self.hdfs_processing_dir
				)
				yield solr_index_task

	def output(self):
		return luigi.LocalTarget("/tmp/luigi/{}-solr_indexed_verified_marked".format(self.stream + '-' + self.year))


if __name__ == '__main__':
	luigi.run(['access.index.SolrIndexAndVerify', '--workers', '5'])
