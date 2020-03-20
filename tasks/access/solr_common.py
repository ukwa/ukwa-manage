import luigi
import logging
import datetime
import pysolr

logger = logging.getLogger('luigi-interface')


# ----------------------------------------------------------
def variables():
	# datestamp for script run, primarily to date output log files
	ymdhms = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

	# tmp working directory for luigi job
	tmpdir = '/tmp/luigi/' + ymdhms + '/'

	# status_field is set here as I don't expect it to change in any implementation,
	# whereas the other fields are parameters to allow triggering script configuration.
	status_field = 'solr_index_ss'

	# sort_field ensures most recent additions to trackdb, within the stream and
	# year parameters, are returned first.
	sort_value = str('timestamp_dt desc')

	return ymdhms, tmpdir, status_field, sort_value

# ----------------------------------------------------------
def solrColName(stream, year):
	# Solr collection names are (NPLD-)?((FC|DC)\-\d{4}|selective)\-\d{6}
	# where \d{4} is the collection year
	# and \d{6} is the creation date of the collection - not related to the collection year
	# Within the tracking_db, a Solr collection is only known by its shortened name, which excludes
	# the final \d{6} collection creation date.

	if stream == 'domain':
		solr_col_name = "NPLD-DC{}".format(year)
	elif stream == 'selective':
		solr_col_name = 'selective'
	else:
		solr_col_name = "NPLD-FC{}".format(year)

	return solr_col_name

# ----------------------------------------------------------
class TrackDBSolrQuery(luigi.Task):
	tracking_db_url = luigi.Parameter()
	stream = luigi.Parameter()
	year = luigi.Parameter()
	status_field = luigi.Parameter()
	status_value = luigi.Parameter()
	sort = luigi.Parameter(default='timestamp_dt desc')
	limit = luigi.IntParameter(default=100)
	output_file = luigi.Parameter()

	task_namespace = "access.index"

	# variables unexpected to change, thus not passed as parameters
	# trackdb schema field, identifying warcs
	kind = 'warcs'

	def run(self):
		# set solr search parameters
		solr_query = pysolr.Solr(url=self.tracking_db_url)
		query_string = "kind_s:{} AND stream_s:{} AND year_i:{} AND {}:{}".format(
			self.kind, self.stream, self.year, self.status_field, self.status_value)
		params = {'rows':self.limit, 'sort':self.sort}

		# perform solr search
		result = solr_query.search(q=query_string, **params)

		# write output
		with self.output().open('w') as res:
			for doc in result.docs:
				res.write(doc['file_path_s'] + '\n')

	def output(self):
		return luigi.LocalTarget("{}".format(self.output_file))
