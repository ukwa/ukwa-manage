import os
import json
import shutil
import luigi
import tempfile
import datetime
from w3act.cli_csv import get_csv, csv_to_zip, load_csv, filtered_targets, to_crawl_feed_format
from tasks.common import logger, state_file

# Define environment variable names here:
ENV_ACT_URL = 'ACT_URL'
ENV_ACT_USER = 'ACT_USER'
ENV_ACT_PASSWORD = 'ACT_PASSWORD'

ACT_URL = os.environ[ENV_ACT_URL]
ACT_USER = os.environ[ENV_ACT_USER]
ACT_PASSWORD = os.environ[ENV_ACT_PASSWORD]


class GetW3actAsCsvZip(luigi.Task):
    """
    Connect to the W3ACT database and dump the whole thing as CSV files.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    db_name = luigi.Parameter(default='w3act')
    db_user = luigi.Parameter(default='w3act')
    db_host = luigi.Parameter(default='prod1.n45.wa.bl.uk')
    db_port = luigi.IntParameter(default='5432')

    task_namespace = 'w3act'

    def output(self):
        return state_file(self.date,'w3act-csv', 'db-csv.zip')

    def run(self):
        # Setup connection params
        params = {
            'password': os.environ.get("W3ACT_PSQL_PASSWORD", None),
            'database': self.db_name,
            'user': self.db_user,
            'host': self.db_host,
            'port': self.db_port
        }
        # And pull down the data tables as CSV:
        tmp_dir = tempfile.mkdtemp()
        csv_dir = os.path.join(tmp_dir, 'w3act-db-csv')
        os.mkdir(csv_dir)
        get_csv(csv_dir, params=params)
        zip_file = csv_to_zip(csv_dir)

        # Move it to the output location (using Luigi helper)
        # https://luigi.readthedocs.io/en/stable/api/luigi.target.html#luigi.target.FileSystemTarget.temporary_path
        with self.output().temporary_path() as temp_output_path:
            shutil.move(zip_file, temp_output_path)

        # And delete the temp dir:
        shutil.rmtree(tmp_dir)


class CopyW3actZipToHDFS(luigi.Task):
    """
    This puts a copy of the file list onto HDFS
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = 'w3act'

    def requires(self):
        return GetW3actAsCsvZip(self.date)

    def output(self):
        return state_file(self.date,'w3act-csv','db-csv.zip', on_hdfs=True)

    def run(self):
        # Read the file in and write it to HDFS:
        with open(self.input().path, 'rb') as f_in, self.output().open('w') as f_out:
            shutil.copyfileobj(f_in, f_out)


class GenerateW3actJsonFromCsv(luigi.Task):
    """
    This emits the Targets as JSON Lines
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = 'w3act'

    def requires(self):
        return GetW3actAsCsvZip(self.date)

    def output(self):
        return state_file(self.date,'w3act-csv','all.json')

    def run(self):
        # Unpack the CSV:
        tmp_dir = tempfile.mkdtemp()
        shutil.unpack_archive(self.input().path, tmp_dir)
        csv_dir = os.path.join(tmp_dir, 'w3act-db-csv')

        # Turn into json
        all = load_csv(csv_dir)

        # Write out as JSON Lines
        with self.output().open('w') as f_out:
            json.dump(all, f_out, indent=2)


class CrawlFeed(luigi.Task):
    """
    Get the feed of targets to crawl at a given frequency. Only core metadata per target, and
    only includes scheduled targets.
    """
    task_namespace = 'w3act'
    frequency = luigi.Parameter(default=None)
    feed = luigi.Parameter(default='npld')
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def requires(self):
        return GenerateW3actJsonFromCsv(self.date)

    def output(self):
        return state_file(self.date,'w3act-csv', 'crawl-feed-%s.%s.json' % (self.feed, self.frequency))

    def run(self):
        # Load all the data including targets:
        with self.input().open() as f_in:
            w3a = json.load(f_in)

        # Filter by npld/bypm, crawl dates, and by frequency (all means not NEVERCRAWL?)
        targets = filtered_targets(w3a['targets'], frequency=self.frequency, terms=self.feed, include_expired=False)

        feed = []
        for target in targets:
            feed.append(to_crawl_feed_format(target))

        # Check the result is sensible:
        if feed is None or len(feed) == 0:
            raise Exception("No feed data downloaded!")
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(feed, indent=4)))


class CrawlFeedAllOpenAccess(luigi.Task):
    """
    Get the feed of targets to crawl at a given frequency. Only core metadata per target, and
    only includes scheduled targets.
    """
    task_namespace = 'w3act'
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def requires(self):
        return GenerateW3actJsonFromCsv(self.date)

    def output(self):
        return state_file(self.date, 'w3act-csv', 'crawl-feed-but-all-oa.json')

    def run(self):
        # Load all the data including targets:
        with self.input().open() as f_in:
            w3a = json.load(f_in)

        # Filter out to all valid OA stuff:
        targets = filtered_targets(w3a['targets'], frequency='all', terms='oa', include_expired=True, include_hidden=False)

        feed = []
        for target in targets:
            feed.append(to_crawl_feed_format(target))

        # Check the result is sensible:
        if feed is None or len(feed) == 0:
            raise Exception("No feed data downloaded!")
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(feed, indent=4)))


class CollectionList(luigi.Task):
    """
    Get the lists of all collections.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def requires(self):
        return GenerateW3actJsonFromCsv(self.date)

    def output(self):
        return state_file(self.date,'w3act-collections', 'collections.json')

    def run(self):
        # Load all the data:
        with self.input().open() as f_in:
            w3a = json.load(f_in)

        # Persist to disk:
        collections = list(w3a['collections'].values())
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(collections , indent=4)))


class SubjectList(luigi.Task):
    """
    Get the lists of all subjects.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def requires(self):
        return GenerateW3actJsonFromCsv(self.date)

    def output(self):
        return state_file(self.date,'w3act-subjects', 'subject-list.json')

    def run(self):
        # Load all the data:
        with self.input().open() as f_in:
            w3a = json.load(f_in)

        # Persist to disk:
        subjects = list(w3a['subjects'].values())
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(subjects, indent=4)))


class TargetList(luigi.Task):
    """
    Get the lists of all targets, with full details for each, whether or not they are scheduled for crawling.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GenerateW3actJsonFromCsv(self.date)

    def output(self):
        return state_file(self.date,'w3act-target-list', 'target-list.json')

    def run(self):
        # Load all the data:
        with self.input().open() as f_in:
            w3a = json.load(f_in)

        # Persist to disk:
        targets = list(w3a['targets'].values())
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(targets, indent=4)))


class TargetListForFrequency(luigi.Task):
    """
    Get the lists of all targets, with full details for each, whether or not they are scheduled for crawling.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    frequency = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return TargetList()

    def output(self):
        return state_file(self.date,'w3act-target-list', 'target-list-%s.json' % self.frequency)

    def run(self):
        # Load the targets:
        with self.input().open() as f:
            all_targets = json.load(f)

        # Grab detailed target data:
        logger.info("Filtering detailed information for %i targets..." % len(all_targets))

        # Filter...
        targets = []
        for t in all_targets:
            if t['crawl_frequency'] is None:
                logger.warning("No crawl frequency set for %s" % t)
            elif t['crawl_frequency'].lower() == self.frequency.lower():
                targets.append(t)

        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(targets, indent=4)))




