import os
import json
import shutil
import luigi
import tempfile
import datetime
from w3act.w3act import w3act
from w3act.cli_csv import get_csv, csv_to_zip
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

    def output(self):
        return state_file(self.date,'w3act', 'db-csv.zip')

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
        csv_dir = tempfile.mkdtemp()
        get_csv(csv_dir, params=params)
        zip_file = csv_to_zip(csv_dir)

        # Move it to the output location (using Luigi helper)
        # https://luigi.readthedocs.io/en/stable/api/luigi.target.html#luigi.target.FileSystemTarget.temporary_path
        with self.output().temporary_path() as temp_output_path:
            shutil.move(zip_file, temp_output_path)

        # And delete the temp dir:
        shutil.rmtree(csv_dir)


class CrawlFeed(luigi.Task):
    """
    Get the feed of targets to crawl at a given frequency. Only core metadata per target, and
    only includes scheduled targets.
    """
    task_namespace = 'w3act'
    frequency = luigi.Parameter()
    feed = luigi.Parameter(default='ld')
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def output(self):
        return state_file(self.date,'w3act', 'crawl-feed-%s.%s.json' % (self.feed, self.frequency))

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        # Grab those targets:
        if self.feed == 'oa':
            targets = w.get_oa_export(self.frequency)
        else:
            targets = w.get_ld_export(self.frequency)
        # Check the result is sensible:
        if targets is None or len(targets) == 0:
            raise Exception("No target data downloaded!")
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(targets, indent=4)))


class GetTargetIDs(luigi.Task):
    """
    Get the lists of all targets, just IDs.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    frequency = luigi.Parameter(default='all')
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return state_file(self.date,'w3act', 'target-ids-%s.json' % self.frequency)

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        # Load the targets:
        if self.frequency == 'all':
            target_ids = w.get_target_ids()
        else:
            target_ids = w.get_ld_target_ids(self.frequency)
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(target_ids, indent=4)))


class GetTarget(luigi.Task):
    """
    Get the metadata for one target

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    id = luigi.IntParameter()
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return state_file(self.date,'w3act-targets', 'target-%i.json' % self.id)

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        # Load the targets:
        target = self.w.get_target(self.id)
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(target, indent=4)))


class CollectionList(luigi.Task):
    """
    Get the lists of all collections.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def output(self):
        return state_file(self.date,'w3act-collections', 'collections.json')

    def add_collection_detail(self, col, collections):
        c_id = int(col['key'])
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        collections.append(w.get_collection(c_id))

    def run(self):
        # Lookup the whole tree and then scan the top-level:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        collection_tree = w.get_collection_tree()
        collections = []
        for c in collection_tree:
            self.add_collection_detail(c, collections)

        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(collections , indent=4)))


class SubjectList(luigi.Task):
    """
    Get the lists of all subjects.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def output(self):
        return state_file(self.date,'w3act-subjects', 'subject-list.json')

    def run(self):
        # Paged downloads...
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        subjects = w.get_subjects()

        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(subjects, indent=4)))


class TargetList(luigi.Task):
    """
    Get the lists of all targets, with full details for each, whether or not they are scheduled for crawling.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return state_file(self.date,'w3act-target-list', 'target-list.json')

    def run(self):
        # Paged downloads...
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        page = 0
        targets = []
        while True:
            # This raises an exception if there's an error, but returns
            # an empty value on the 404 when the end of the list is reached:
            tpage = w.get_targets(page=page, page_length=1000)
            if not tpage:
                break
            for t in tpage:
               targets.append(t)
            page += 1
        # Catch problems:
        if len(targets) == 0:
          raise Exception("No target data downloaded!")

        # Persist to disk:
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
            if t['field_crawl_frequency'] is None:
                logger.warning("No crawl frequency set for %s" % t)
            elif t['field_crawl_frequency'].lower() == self.frequency.lower():
                targets.append(t)

        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(targets, indent=4)))




