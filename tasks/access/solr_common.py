import luigi
import logging
import datetime
import os
import pysolr
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
import luigi.contrib.ssh

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
			try:
				for doc in result.docs:
					res.write(doc['file_path_s'] + '\n')
			except Exception as e:
				raise Exception("Issue with data returned from trackdb solr query")

	def output(self):
		return luigi.LocalTarget("{}".format(self.output_file))

# ----------------------------------------------------------
class CopyToRemote(luigi.Task):
	# Copy file argument to destination directory argument on server argument
	input_file = luigi.Parameter()
	dest_dir = luigi.Parameter()
	mapred_user = luigi.Parameter()
	mapred_host = luigi.Parameter()

	def run(self):
		self.output().put(self.input_file)

	def output(self):
		fpfile = self.dest_dir + os.path.basename(self.input_file)
		return luigi.contrib.ssh.RemoteTarget(path=fpfile, username=self.mapred_user, host=self.mapred_host)

class MRSolrWARCSubmitter(luigi.contrib.hadoop_jar.HadoopJarJobTask):
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
		return CopyToRemote(input_file=self.warcs_file, dest_dir=self.mapred_dir, 
			mapred_user=self.mapred_user, mapred_host=self.mapred_host)

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
		# warc_config defines the solr indexing configuration file that details the
		# solr collection. This is vital to processing warcs.
		# This warc_config is located at hdfs@mapred:/home/hdfs/gitlab/discovery_supervisor/warcs2solr/
		# and stored within the gitlab discovery_supervisor repo
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
