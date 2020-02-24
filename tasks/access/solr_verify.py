import luigi
import os
import re
import datetime
import logging

logger = logging.getLogger('luigi-interface')


# ----------------------------------------------------------
class WarcToTest(luigi.Task):
	def run(self):
		with open('/mnt/fakemount/access/task-state/warc-file-list/2020-12/2020-12-31-warc-file-list--to-2020-01-01-as-of-2020-02-13_16-37-00.txt', 'r') as fake_input_source:
			line = fake_input_source.readline()
			logger.debug("==== WarcToTest run: line {}".format(line))
			yield [line]


# ----------------------------------------------------------
class WarcToVerify(luigi.Task):
	start_date = luigi.Parameter()
	end_date = luigi.Parameter()
	stream = luigi.Parameter()
	status_field = luigi.Parameter()
	status_value = luigi.Parameter()
	tracking_db_url = luigi.Parameter()
	tmpdir = luigi.Parameter()

	# output
	warc_name = ''

	def requires(self):
		return WarcToTest()

	def run(self):
		for warc in self.input():
			self.warc_name = os.path.basename(warc)
			logger.debug("==== WarcToVerify run: fake warc {}".format(warc))
			logger.debug("==== WarcToVerify run: fake warc_name {}".format(self.warc_name))
			with open(self.output().path, 'w') as warc_record:
				warc_record.write(warc)		

	def output(self):
		if os.path.isfile("{}{}".format(self.tmpdir, self.warc_name)):
			logger.debug("==== WarcToVerify output: fake WARC {}".format(self.warc_name))
			return luigi.LocalTarget("{}{}".format(self.tmpdir, self.warc_name))


# ----------------------------------------------------------
class SolrIndexAndVerify(luigi.Task):
	'''
	This luigi task:
	1) requests X number of WARC paths from the tracking_db service
	2) submits them as input to a mapreduce job, which runs another server (one which is purposed to submit 
	mapreduce jobs), that populates	the appropriate Solr collection with our UKWA search data. 
	3) After this it checks that this submission has succeeded and that the WARC paths exist in the Solr collection. 
	4) If successful, that WARC path is marked in the tracking_db as "indexed in solr".

	As this is luigi, the code for this is written from the required end result, backwards through the tasks
	that produce that outcome. So the task to achieve in the main class is step 4!
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
	verify_only = luigi.BoolParameter(default=False)

	# task_namespace defines scope of class. Without defining this, other classes
	# could call this class inside their scope, which would be wrong.
	task_namespace = "access.index"

	# datestamp for script run, primarily to date output log files
	ymdhms = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

	# tmp working directory for luigi job
	tmpdir = '/tmp/luigi/' + ymdhms + '/'

	# status_field is set here as I don't expect it to change in any implementation, 
	# whereas the other fields are parameters to allow triggering script configuration.
	status_field = 'solr_index_ss'

	# Solr collection names are (NPLD-)?((FC|DC)\-\d{4}|selective)\-\d{6}
	# where \d{4} is the collection year
	# and \d{6} is the creation date of the collection - not related to the collection year
	# Within the tracking_db, a Solr collection is known by its shorthand, which excludes
	# the final \d{6} collection creation date
	def _solrCollection(self):
		sc = ''
		if self.stream == 'domain':
			sc = "npld-dc{}".format(self.year)
		elif self.stream == 'selective':
			sc = 'selective'
		else:
			sc = "npld-fc{}".format(self.year)
		return sc

	# The shorthand Solr collection name given the script parameters
	solr_collection = ''

	# Get a WARC path to mark
	def requires(self):
		logger.debug("==== main requires ====================================================================================")
		# set shorthand solr collection name
		self.solr_collection = self._solrCollection()
		# ensure tmpdir exists
		if not os.path.isdir(self.tmpdir):
			os.mkdir(self.tmpdir)

		return WarcToVerify(
			start_date=datetime.date.fromisoformat(str(self.year)+'-01-01'),
			end_date=datetime.date.fromisoformat(str(int(self.year)+1)+'-01-01') - datetime.timedelta(days=1),
			stream=self.stream,
			status_field=self.status_field,
			status_value=self.solr_collection,
			tracking_db_url=self.tracking_db_url,
			tmpdir=self.tmpdir
		)

	# Index list of WARCs into main Solr search service.
	# Ensure WARCs now in Solr.
	# Flag WARCs as in Solr.
	def run(self):
		logger.debug("==== main run ====================================================================================")
#		tracking_db_record = TrackingDB(tracking_db_url=self.tracking_db_url, warc=self.input().path)
#		logger.debug("==== tracking_db_record {}".format(tracking_db_record))
#		if self.status_field in tracking_db_record:
#			logger.debug("==== status_field in tracking_db_record")
#			if tracking_db_record[self.status_field] == solr_collection:
#				logger.debug("==== solr_collection in status_field")
#				with open(self.output().path, 'w') as marked:
#					logger.debug("writing output")
#					marked.write("{} confirmed as marked as solr indexed in tracking_db", self.input().path)
		with open(self.output().path, 'w') as marked:
			logger.debug("==== main run: writing output")
			marked.write("{} confirmed as marked as solr indexed in tracking_db".format(self.input().path))

	def output(self):
		logger.debug("==== main output ====================================================================================")
		return luigi.LocalTarget("{}solr_indexed_verified_marked-{}".format(self.tmpdir, self.solr_collection))


if __name__ == '__main__':
	luigi.run(['access.index.SolrIndexAndVerify', '--workers', '5'])
