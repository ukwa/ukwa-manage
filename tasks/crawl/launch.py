#!/usr/bin/env python
# encoding: utf-8
'''
task.crawl.launch -- Launches the Frequent Crawls

@author:     Andrew Jackson

@copyright:  2018 The British Library.

@license:    Apache 2.0

@contact:    Andrew.Jackson@bl.uk
@deffield    updated: 2018-09-04
'''

import datetime
import time
import json
import luigi
import logging
from tasks.crawl.w3act import CrawlFeed
from lib.targets import IngestTaskDBTarget
from prometheus_client import CollectorRegistry, Gauge
from crawlstreams import enqueue


logger = logging.getLogger('luigi-interface')

class UpdateScopeFiles(luigi.Task):
    """
    Get the lists of all targets and updates the crawlers scope files.
    """
    task_namespace = 'crawl'
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def requires(self):
        # Get the crawl feed of interest:
        return {
            'all' : CrawlFeed(frequency='all', date=self.date),
            'nevercrawl': CrawlFeed(frequency='nevercrawl', date=self.date)
        }

    def output(self):
        return IngestTaskDBTarget('crawl', self.task_id)

    def run(self):
        # Load the targets:
        with self.input()['all'].open() as f:
            all_targets = json.load(f)
            self.write_surt_file(all_targets, self.scope_in_file)
            self.write_watched_surt_file(all_targets, self.scope_watched_file)
        # Load the block list:
        with self.input()['nevercrawl'].open() as f:
            blocked_targets = json.load(f)
            self.write_surt_file(blocked_targets, self.scope_blocked_file)
        # Record that all went well:
        self.output().touch()

    def write_surt_file(self, targets, filename):
        with open(filename, 'w') as f:
            for t in targets:
                for seed in t['seeds']:
                    # f.write("%s\n" % url_to_surt(seed)) # needs a '+'?
                    f.write("%s\n" % seed)

    def write_watched_surt_file(self, targets, filename):
        with open(filename, 'w') as f:
            for t in targets:
                if t['watched']:
                    for seed in t['seeds']:
                        # f.write("%s\n" % url_to_surt(seed))
                        f.write("%s\n" % seed)


class LaunchCrawls(luigi.Task):
    """
    Get the lists of all targets from the crawl feed and launch those that need launching.
    """
    task_namespace = 'crawl'
    frequency = luigi.Parameter(default='all')
    date = luigi.DateHourParameter(default=datetime.datetime.today())
    kafka_server = luigi.Parameter(default='crawler02.n45.bl.uk:9094')
    queue = luigi.Parameter(default='fc.candidates')

    # The message launcher:
    launcher = None
    i_launches = 0
    target_errors = 0

    def requires(self):
        # Get the crawl feed of interest:
        return CrawlFeed(frequency=self.frequency, date=self.date)

    def output(self):
        return IngestTaskDBTarget('crawl', self.task_id)

    def run(self):
        # Load the targets:
        with self.input().open() as f:
            all_targets = json.load(f)

        # Grab detailed target data:
        logger.info("Filtering detailed information for %i targets..." % len(all_targets))

        # Set up launcher:
        self.launcher = enqueue.KafkaLauncher(kafka_server=self.kafka_server, topic=self.queue)

        # Get current time
        now = self.date
        logger.debug("Now timestamp: %s" % str(now))

        # Process looking for due:
        self.i_launches = 0
        for t in all_targets:
            logger.debug("----------")
            logger.debug("Looking at %s (tid:%d)" % (t['title'], t['id']))

            # Look out for problems:
            if len(t['seeds']) == 0:
                logger.error("This target has no seeds! tid: %d" % t['id'])
                self.target_errors += 1
                continue

            # Add a source tag if this is a watched target:
            source = "tid:%d:%s" % (t['id'], t['seeds'][0])

            # Check the scheduling:
            for schedule in t['schedules']:
                # Skip if target schedule outside of start/end range
                startDate = datetime.datetime.strptime(schedule['startDate'], "%Y-%m-%d %H:%M:%S")
                logger.debug("Target schedule start date: %s" % str(startDate))
                if (now < startDate):
                    logger.debug("Start date %s not yet reached" % startDate)
                    continue
                endDate = 'N/S'
                if schedule['endDate']:
                    endDate = datetime.datetime.strptime(schedule['endDate'], "%Y-%m-%d %H:%M:%S")
                    if now > endDate:
                        logger.debug("End date %s passed" % endDate)
                        continue
                logger.debug("Target schedule end date:  %s" % str(endDate))
                logger.debug("Target frequency: %s" % schedule['frequency'])

                # Check if the frequency and date match up:
                if schedule['frequency'] == "DAILY":
                    self.launch_by_hour(now, startDate, endDate, t, source, 'DAILY')

                elif schedule['frequency'] == "WEEKLY":
                    if now.isoweekday() == startDate.isoweekday():
                        self.launch_by_hour(now, startDate, endDate, t, source, 'WEEKLY')
                    else:
                        logger.debug("WEEKLY: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))

                elif schedule['frequency'] == "MONTHLY":
                    if now.day == startDate.day:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'MONTHLY')
                    else:
                        logger.debug("MONTHLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug("MONTHLY: day %s differs from schedule %s" % (now.day, startDate.day))

                elif schedule['frequency'] == "QUARTERLY":
                    if now.day == startDate.day and now.month % 3 == startDate.month % 3:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'QUARTERLY')
                    else:
                        logger.debug("QUARTERLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug(
                            "QUARTERLY: month3 %s versus schedule %s" % (now.month % 3, startDate.month % 3))

                elif schedule['frequency'] == "SIXMONTHLY":
                    if now.day == startDate.day and now.month % 6 == startDate.month % 6:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'SIXMONTHLY')
                    else:
                        logger.debug("SIXMONTHLY: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug(
                            "SIXMONTHLY: month6 %s versus schedule %s" % (now.month % 6, startDate.month % 6))

                elif schedule['frequency'] == "ANNUAL":
                    if now.day == startDate.day and now.month == startDate.month:
                        self.launch_by_hour(now, startDate, endDate, t, source, 'ANNUAL')
                    else:
                        logger.debug("ANNUAL: date %s does not match schedule %s" % (
                            now, startDate))
                        logger.debug("ANNUAL: month %s versus schedule %s" % (now.month, startDate.month))
                else:
                    logger.error("Don't understand crawl frequency " + schedule['frequency'])

        logger.info("Completed. Launches this hour: %s" % self.i_launches)
        # Record that all went well:
        self.output().touch()

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_seeds_launched',
                  'Total number of seeds launched.',
                  labelnames=['stream'], registry=registry)
        g.labels(stream=self.frequency).set(self.i_launches)

        g = Gauge('ukwa_target_errors',
                  'Total number of targets that appear malformed.',
                  labelnames=['stream'], registry=registry)
        g.labels(stream=self.frequency).set(self.target_errors)

    def launch_by_hour(self, now, startDate, endDate, t, source, freq):
        # Is it the current hour?
        if now.hour is startDate.hour:
            logger.info(
                "%s target %s (tid: %s) scheduled to crawl (now: %s, start: %s, end: %s), sending to FC-3-uris-to-crawl" % (
                freq, t['title'], t['id'], now, startDate, endDate))
            counter = 0
            for seed in t['seeds']:
                # For now, only treat the first URL as a scope-defining seed that we force a re-crawl for:
                # FIXME Can likely relax this once we separate aliases from seeds, so seeds won't be used for URLs that end up in the same place.
                if counter == 0:
                    isSeed = True
                else:
                    isSeed = False

                # Set-up sheets
                sheets = []

                # Robots.txt
                if t['ignoreRobotsTxt']:
                    sheets.append('ignoreRobots')
                # Scope
                if t['scope'] == 'subdomains':
                    sheets.append('subdomainsScope')
                elif t['scope'] == 'plus1Scope':
                    sheets.append('plus1Scope')
                # Limits
                if t['depth'] == 'CAPPED_LARGE':
                    sheets.append('higherLimit')
                elif t['depth'] == 'DEEP':
                    sheets.append('noLimit')

                # Set up the launch_ts: (Should be startDate but if that happens to be in the future this will all break)
                launch_timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(time.mktime(now.timetuple())))

                # And send launch message, always resetting any crawl quotas:
                self.launcher.launch(seed, source, isSeed, forceFetch=True, sheets=sheets, reset_quotas=True, launch_ts=launch_timestamp, inherit_launch_ts=False)
                counter = counter + 1
                self.i_launches = self.i_launches + 1

        else:
            logger.debug("The hour (%s) is not current." % startDate.hour)


if __name__ == '__main__':
    luigi.run(['crawl.LaunchCrawls', '--kafka-server', 'crawler02.n45.bl.uk:9094', '--queue', 'fc.to-crawl', '--local-scheduler'])
