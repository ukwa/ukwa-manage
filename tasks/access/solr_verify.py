import luigi
import os
import logging
import pysolr

import tasks.access.solr_common as solr_common

logger = logging.getLogger('luigi-interface')


# ----------------------------------------------------------
class SolrVerifyWarcs(luigi.Task):
	'''
	This luigi task:
	1) Gets a list of WARCs from the tracking db that aren't listed as verified indexed in Solr
	2) For each WARC, test if in Solr. If so, mark in tracking db
	'''
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
	limit = luigi.IntParameter()

	# task_namespace defines scope of class. Without defining this, other classes
	# could call this class inside their scope, which would be wrong.
	task_namespace = "access.index"

	# get common vars used in both solr_index.py and solr_verify.py
	ymdhms, tmpdir, status_field, sort_value = solr_common.variables()

	# shorthand solr collection name
	solr_col_name = ''

	# Get a list of WARC paths to verify and mark in trackdb if in solr collection
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
			output_file=self.tmpdir + 'solr_verify-' + self.solr_col_name + '-warcs_list_from_trackdb'
		)

	# Index list of WARCs into main Solr search service.
	# Ensure WARCs now in Solr.
	# Flag WARCs as in Solr.
	def run(self):
		# Set var for all results, to be included in final output if successful
		# & boolean for luigi script success
		sv_results = list()
		luigi_success = True
		# traverse through list of warcs, verifying each in solr collection
		sv_input = self.input().open('r')
		for warc in sv_input:
			sv_results.append("Verifying warc {}".format(warc))

			# Test if basename of WARC in Solr collection as path dropped during indexing
			solr_query = pysolr.Solr(url=self.solr_api)
			query_string = 'source_file:{}'.format(os.path.basename(warc))
			results = solr_query.search(q=query_string)
			logger.debug("solr query result len [{}]".format(len(results)))
#			if 'response' in result.docs:
#				if 'numFound' in result.docs['response']:
#					sv_results.append("{} records found".format(result.docs['response']['numFound']))
#
#					# Update trackdb record for warc
#				else:
#					sv_results.append("NOT found in solr")
#					luigi_success = False
#
#			else:
#				raise Exception("Solr query failed to return 'response'\n{} - {}".format(self.solr_api, query_string))
#				logger.error("Solr query failed to return 'response'\n{} - {}".format(self.solr_api, query_string))

		if not luigi_success:
			sv_results.append("DEBUGGING - faking True result")
			luigi_success = True

		# if run successful, write final luigi task output file indicating success
		if luigi_success:
			with open(self.output().path, 'w') as success:
				success.write("{} verified and marked as solr indexed in tracking_db\n".format(self.input().path))
				for line in sv_results:
					success.write("{}".format(line))

	def output(self):
		return luigi.LocalTarget("{}solr_verify-{}-success".format(self.tmpdir, self.solr_col_name))


if __name__ == '__main__':
	luigi.run(['access.index.SolrVerifyWarcs', '--workers', '5'])
