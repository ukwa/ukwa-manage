import os
import json
import luigi
import logging
import datetime
from crawl.w3act.w3act import w3act

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
ACT_URL = os.environ['ACT_URL']
ACT_USER = os.environ['ACT_USER']
ACT_PASSWORD = os.environ['ACT_PASSWORD']


class CrawlFeed(luigi.Task):
    """
    Get the feed of targets to crawl at a given frequency. Only core metadata per target, and
    only includes scheduled targets.
    """
    task_namespace = 'w3act'
    frequency = luigi.Parameter()
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/crawl-feed-ld.%s.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string, self.frequency))

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        # Grab those targets:
        targets = w.get_ld_export(self.frequency)
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
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/target-%s-ids.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], self.frequency, datetime_string))

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

    # Set up connection to W3ACT:
    w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/targets/target-%i.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], self.id, datetime_string))

    def run(self):
        # Load the targets:
        target = self.w.get_target(self.id);
        # Persist to disk:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(target, indent=4)))



class TargetList(luigi.Task):
    """
    Get the lists of all targets, with full details for each, whether or not they are scheduled for crawling.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/target-list.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

    def run(self):
        # Paged downloads...
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        page = 0
        targets = []
        while True:
            tpage = w.get_targets(page=page, page_length=500)
            if not tpage:
                break
            for t in tpage:
               targets.append(t)
            page += 1

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
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/target-list.%s.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], self.frequency, datetime_string))

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
