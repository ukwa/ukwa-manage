import luigi
import os
import datetime
import logging
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
import luigi.contrib.ssh

import tasks.access.solr_common as solr_common

logger = logging.getLogger('luigi-interface')


class SolrIndexWarcs(luigi.Task):
	'''
	This luigi task:
	1) Gets a list of WARCs from the tracking db that aren't listed as verified indexed in Solr
	2) Submits this list of WARCs into the solr collection calculated from stream and year
	'''
	tracking_db_url = luigi.Parameter()
	mapred_host = luigi.Parameter()
	mapred_user = luigi.Parameter()
	mapred_dir = luigi.Parameter()
	annotations = luigi.Parameter()
	whitelist = luigi.Parameter()
	warc_indexer_jar = luigi.Parameter()
	hdfs_processing_dir = luigi.Parameter()
	solr_api = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	limit = luigi.Parameter()

	# task_namespace defines scope of class. Without defining this, other classes
	# could call this class inside their scope, which would be wrong.
	task_namespace = "access.index"

	# get common vars used in both solr_index.py and solr_verify.py
	ymdhms, tmpdir, status_field, sort_value = solr_common.variables()

	# shorthand solr collection name
	solr_col_name = ''

	# Get list of WARCs that don't have flag set indicating already indexed in Solr
	def requires(self):
		# get shorthand solr collection name
		self.solr_col_name = solr_common.solrColName(self.stream, self.year)
		# ensure tmpdir exists
		if not os.path.isdir(self.tmpdir):
			os.mkdir(self.tmpdir)
		# return list of warcs not marked as verified from trackdb
		return solr_common.TrackDBSolrQuery(
			tracking_db_url=self.tracking_db_url,
			stream=self.stream,
			year=self.year,
			status_field='-' + self.status_field,
			status_value=self.solr_col_name,
			limit=self.limit,
			sort=self.sort_value,
			output_file=self.tmpdir + 'solr_index-' + self.solr_col_name + '-warcs_list_from_trackdb'
		)

	# Index list of WARCs into Solr search service.
	def run(self):
		# test some data received
		listsize = os.stat(self.input().path)
		if listsize.st_size == 0:
			logger.warning("==== No WARCs listed for {} {}".format(self.stream, self.year))

		else:
			# Submit MapReduce job of WARCs list for Solr search service
			logger.info("---- Submitting WARCs for {} {} into {}".format(self.stream, self.year, self.solr_api))
			logger.info("---- List of WARCs to be submitted: {}".format(self.input().path))
			solr_index_task = solr_common.MRSolrWARCSubmitter(
				warcs_file=self.input().path,
				stream=self.stream,
				year=self.year,
				solr_api=self.solr_api,
				mapred_host=self.mapred_host,
				mapred_user=self.mapred_user,
				mapred_dir=self.mapred_dir,
				annotations=self.annotations,
				whitelist=self.whitelist,
				warc_indexer_jar=self.warc_indexer_jar,
				hdfs_processing_dir=self.hdfs_processing_dir
			)
			yield solr_index_task

			# generate luigi task output file
			with open(self.output().path, 'w') as out:
				out.write("WARC file indexed into {}: {}\n".format(self.solr_col_name, self.input().path))

	def output(self):
		return luigi.LocalTarget("{}solr_index-{}-success".format(self.tmpdir, self.solr_col_name))


if __name__ == '__main__':
	luigi.run(['access.index.SolrIndexWarcs', '--workers', '5'])
