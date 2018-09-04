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
import json
import luigi
from tasks.ingest.w3act import CrawlFeed
from tasks.common import logger, state_file

from lib.enqueue import KafkaLauncher


class LaunchCrawls(luigi.Task):
    """
    Get the lists of all targets, with full details for each, whether or not they are scheduled for crawling.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'crawl'
    frequency = luigi.Parameter(default='weekly')
    date = luigi.DateHourParameter(default=datetime.datetime.today())
    kafka_server = luigi.Parameter(default='localhost:9092')
    queue = luigi.Parameter(default='fc.candidates')

    # Set up launcher:
    launcher = KafkaLauncher(kafka_server=[str(kafka_server)], topic=queue)
    i_launches = 0

    def requires(self):
        # Get the crawl feed of interest:
        return CrawlFeed(frequency=self.frequency, date=self.date)

    def output(self):
        return state_file(self.date,'w3act-target-list', 'target-launch-%s.json' % self.frequency)

    def run(self):
        # Load the targets:
        with self.input().open() as f:
            all_targets = json.load(f)

        # Grab detailed target data:
        logger.info("Filtering detailed information for %i targets..." % len(all_targets))

        # Destination is always h3
        destination = 'h3'

        # Get current time
        now = datetime.datetime.today()
        logger.debug("Now timestamp: %s" % str(now))

        # Process looking for due:
        self.i_launches = 0
        for t in all_targets:
            logger.debug("----------")
            logger.debug("Looking at %s (tid:%d)" % (t['title'], t['id']))

            # Add a source tag if this is a watched target:
            source = ''
            if t['watched']:
                source = t['seeds'][0]

            # Check the scheduling:
            for schedule in t['schedules']:
                # Skip if target schedule outside of start/end range
                startDate = datetime.datetime.utcfromtimestamp(schedule['startDate'] / 1000)
                logger.debug("Target schedule start date: %s" % str(startDate))
                if (now < startDate):
                    logger.debug("Start date %s not yet reached" % startDate)
                    continue
                endDate = 'N/S'
                if schedule['endDate']:
                    endDate = datetime.datetime.utcfromtimestamp(schedule['endDate'] / 1000)
                    if now > endDate:
                        logger.debug("End date %s passed" % endDate)
                        continue
                logger.debug("Target schedule end date:  %s" % str(endDate))
                logger.debug("Target frequency: %s" % schedule['frequency'])

                # Check if the frequency and date match up:
                if schedule['frequency'] == "DAILY":
                    self.launch_by_hour(now, startDate, endDate, t, destination, source, 'DAILY')

                elif schedule['frequency'] == "WEEKLY":
                    if now.isoweekday() == startDate.isoweekday():
                        self.launch_by_hour(now, startDate, endDate, t, destination, source, 'WEEKLY')
                    else:
                        logger.debug("WEEKLY: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))

                elif schedule['frequency'] == "MONTHLY":
                    if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day:
                        self.launch_by_hour(now, startDate, endDate, t, destination, source, 'MONTHLY')
                    else:
                        logger.debug("MONTHLY: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))
                        logger.debug("MONTHLY: day %s differs from schedule %s" % (now.day, startDate.day))

                elif schedule['frequency'] == "QUARTERLY":
                    if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month % 3 == startDate.month % 3:
                        self.launch_by_hour(now, startDate, endDate, t, destination, source, 'QUARTERLY')
                    else:
                        logger.debug("QUARTERLY: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))
                        logger.debug(
                            "QUARTERLY: month3 %s differs from schedule %s" % (now.month % 3, startDate.month % 3))

                elif schedule['frequency'] == "SIXMONTHLY":
                    if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month % 6 == startDate.month % 6:
                        self.launch_by_hour(now, startDate, endDate, t, destination, source, 'SIXMONTHLY')
                    else:
                        logger.debug("SIXMONTHLY: isoweekday %s differs from schedule %s" % (
                        now.isoweekday(), startDate.isoweekday()))
                        logger.debug(
                            "SIXMONTHLY: month6 %s differs from schedule %s" % (now.month % 6, startDate.month % 6))

                elif schedule['frequency'] == "ANNUAL":
                    if now.isoweekday() == startDate.isoweekday() and now.day == startDate.day and now.month == startDate.month:
                        self.launch_by_hour(now, startDate, endDate, t, destination, source, 'ANNUAL')
                    else:
                        logger.debug("ANNUAL: isoweekday %s differs from schedule %s" % (
                            now.isoweekday(), startDate.isoweekday()))
                        logger.debug("ANNUAL: month %s differs from schedule %s" % (now.month, startDate.month))
                else:
                    logger.error("Don't understand crawl frequency " + schedule['frequency'])

        logger.info("Completed. Launches this hour: %s" % self.i_launches)

    def launch_by_hour(self, now, startDate, endDate, t, destination, source, freq):
        # Is it the current hour?
        if now.hour is startDate.hour:
            logger.info(
                "%s target %s (tid: %s) scheduled to crawl (now: %s, start: %s, end: %s), sending to FC-3-uris-to-crawl" % (
                freq, t['title'], t['id'], now, startDate, endDate))
            counter = 0
            for seed in t['seeds']:
                # For now, only treat the first URL as a scope-defining seed that we force a re-crawl for:
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
                # Frequency:
                if freq == 'DAILY':
                    sheets.append('recrawl-1day')
                elif freq == 'WEEKLY':
                    sheets.append('recrawl-1week')
                elif freq == 'MONTHLY':
                    sheets.append('recrawl-27days')
                elif freq == 'QUARTERLY':
                    sheets.append('recrawl-12weeks')
                elif freq == 'SIXMONTHLY':
                    sheets.append('recrawl-24weeks')
                elif freq == 'ANNUAL':
                    sheets.append('recrawl-365days')

                # And send launch message:
                self.launcher.launch(destination, seed, source, isSeed, sheets=sheets)
                counter = counter + 1
                self.i_launches = self.i_launches + 1

        else:
            logger.debug("The hour (%s) is not current." % startDate.hour)

    def write_surt_file(self, targets, filename):
        with open(filename, 'w') as f:
            for t in targets:
                for seed in t['seeds']:
                    # f.write("%s\n" % url_to_surt(seed))
                    f.write("%s\n" % seed)

    def write_watched_surt_file(self, targets, filename):
        with open(filename, 'w') as f:
            for t in targets:
                if t['watched']:
                    for seed in t['seeds']:
                        # f.write("%s\n" % url_to_surt(seed))
                        f.write("%s\n" % seed)


if __name__ == '__main__':
    luigi.run(['crawl.LaunchCrawls', '--kafka-server', 'crawler02.n45.bl.uk:9094', '--queue', 'fc.to-crawl', '--local-scheduler'])
