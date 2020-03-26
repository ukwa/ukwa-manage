import luigi
import os
import logging
import requests
import json

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
		sv_results = list()
		warcs_count = warcs_found_count = warcs_missing_count = 0

		# traverse through list of warcs, verifying each in solr collection
		sv_input = self.input().open('r')
		for warc in sv_input:
			warcs_count += 1
			warc = warc.rstrip('\n')
			base_warc = os.path.basename(warc)
			hdfs_warc = 'hdfs://hdfs:54310' + warc
			sv_results.append("Verifying warc {}: ".format(base_warc))

			# set solr collection search terms
			solr_query_url = self.solr_api + 'select'
			query_string = { 'q':"source_file:{}".format(base_warc), 'rows':1, 'wt':'json' }

			# gain solr collection search response
			response = ''
			try:
				r = requests.get(url=solr_query_url, params=query_string)
				response = r.json()['response']
			except Exception as e:
				raise Exception("Issue with data from solr collection {}: {}".format(solr_query_url, r.status_code))

			if response['numFound'] > 0:
				warcs_found_count += 1
				sv_results.append("\n{} records found\n".format(response['numFound']))

				# Update trackdb record for warc
				update_trackdb_url = self.tracking_db_url + '/update?commit=true'
				post_headers = {'Content-Type': 'application/json'}
				post_data = { 'id': hdfs_warc, self.status_field: {'add': self.solr_col_name} }

				# gain tracking_db search response
				response = ''
				try:
					r = requests.post(url=update_trackdb_url, headers=post_headers, json=[post_data])
					response = r.json()
					sv_results.append("trackdb post response {}\n".format(response))
				except Exception as e:
					raise Exception("Issue with posting data to trackdb")
					sv_results.append("Issue with posting data to trackdb\n")

			else:
				warcs_missing_count += 1
				sv_results.append("NOT found in solr\n")

		# write final luigi task output file indicating success
		with open(self.output().path, 'w') as success:
			for line in sv_results:
				success.write("{}".format(line))
			success.write("{} warcs checked, {} in solr, {} not in solr.\n".format(
				warcs_count, warcs_found_count, warcs_missing_count))
			success.write("Fin\n")

	def output(self):
		return luigi.LocalTarget("{}solr_verify-{}-success".format(self.tmpdir, self.solr_col_name))


if __name__ == '__main__':
	luigi.run(['access.index.SolrVerifyWarcs', '--workers', '5'])
