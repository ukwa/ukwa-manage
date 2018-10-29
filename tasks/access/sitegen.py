# -*- coding: utf-8 -*-
import json
import luigi
import pysolr
import logging
import base64
import hashlib
import datetime
import tldextract
#from lib.cdx import CdxIndex
#from tasks.ingest.w3act import TargetList, SubjectList, CollectionList
from tasks.common import state_file
from jinja2 import Environment, PackageLoader
from prometheus_client import CollectorRegistry, Gauge

logger = logging.getLogger('luigi-interface')


class GenerateSitePages(luigi.Task):
    task_namespace = 'site'
    date = luigi.DateParameter(default=datetime.date.today())

    record_count = 0
    blocked_record_count = 0
    missing_record_count = 0
    embargoed_record_count = 0

    target_count = 0
    collection_count = 0
    collection_published_count = 0
    subject_count = 0

    #def requires(self):
    #    return [TargetList(self.date), CollectionList(self.date), SubjectList(self.date)]

    def input(self):
        return [
            luigi.LocalTarget('/Users/andy/Documents/workspace/ukwa-manage/test/w3act-export-20181019/2018-10-19-w3act-target-list-target-list.json'),
            luigi.LocalTarget('/Users/andy/Documents/workspace/ukwa-manage/test/w3act-export-20181019/2018-10-19-w3act-collections-collections.json'),
            luigi.LocalTarget('/Users/andy/Documents/workspace/ukwa-manage/test/w3act-export-20181019/2018-10-19-w3act-subjects-subject-list.json')
            ]

    def output(self):
        return state_file(self.date,'access-data', 'title-level-metadata-w3act.xml')

    def run(self):
        # Get the data:
        targets = json.load(self.input()[0].open(),encoding='utf-8')
        self.target_count = len(targets)
        collections = json.load(self.input()[1].open())
        self.collection_count = len(collections)
        subjects = json.load(self.input()[2].open())
        self.subject_count = len(subjects)

        # Index collections by ID:
        collections_by_id = {}
        for col in collections:
            collections_by_id[int(col['id'])] = col
            if col['field_publish']:
                self.collection_published_count += 1

        # Setup template environment:
        env = Environment(loader=PackageLoader('tasks.access.sitegen', 'templates'))

        # Targets
        self.generate_targets(env, targets, collections_by_id)

        # Collections
        self.generate_collections("/Users/andy/Documents/workspace/ukwa-site/content/collection", env, collections)

    def generate_collections(self, base_path, env, collections):
        template = env.get_template('site-target-template.md')
        # Emit this level:
        for col in collections:
            # Skip unpublished collections:
            if col['field_publish'] != True:
                logger.warning("The Collection '%s' not to be published!" % col['name'] )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # And write:
            col['title'] = col['name']
            if 'description' in col and col['description'] != None:
                col['description'] = col['description'].replace('\r\n', '\n')
            col.pop('url', None)
            target_md = luigi.LocalTarget("%s/%s/index.en.md" % (base_path, col['id']))
            with target_md.open('w') as f:
                for part in template.generate({ "record": col, "json": json.dumps(col, indent=2) }):
                    f.write(part.encode("utf-8"))


    def generate_targets(self, env, targets,collections_by_id):
        # Setup specific template:
        template = env.get_template('site-target-template.md')

        # Export targets
        for target in targets:
            # Skip blocked items:
            if target['field_crawl_frequency'] == 'NEVERCRAWL':
                logger.warning("The Target '%s' is blocked (NEVERCRAWL)." % target['title'])
                self.blocked_record_count += 1
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # Skip items that have no crawl permission?
            # hasOpenAccessLicense == False, and inScopeForLegalDeposit == False ?
            # Skip items with no URLs:
            if len(target['fieldUrls']) == 0:
                logger.warning("The Target '%s' has no URLs!" % target['title'] )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # Skip hidden targets:
            if target['field_hidden']:
                logger.warning("The Target '%s' is hidden!" % target['title'] )
                # FIXME SHOULD DELETE THE FILE IF IT EXISTS!
                continue
            # Get the ID, WCT ID preferred:
            tid = target['id']
            if target.get('field_wct_id', None):
                tid = target['field_wct_id']
            # Get the url, use the first:
            url = target['fieldUrls'][0]['url']
            # Extract the domain:
            parsed_url = tldextract.extract(url)
            publisher = parsed_url.registered_domain
            # Lookup in CDX:
            #wayback_date_str = CdxIndex().get_first_capture_date(url) # Get date in '20130401120000' form.
            #if wayback_date_str is None:
            #    logger.warning("The URL '%s' is not yet available, inScopeForLegalDeposit = %s" % (url, target['inScopeForLegalDeposit']))
            #    self.missing_record_count += 1
            #    continue
            start_date = target.get('crawlStartDateISO')
            if start_date is None:
                wayback_date = datetime.datetime.now()
            else:
                wayback_date = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')
            wayback_date_str = wayback_date.strftime('%Y%m%dT%H%M%S')
            first_date = wayback_date.isoformat()
            record_id = "%s/%s" % (wayback_date_str, base64.b64encode(hashlib.md5(url.encode('utf-8')).digest()))

            # Honour embargo
            #ago = datetime.datetime.now() - wayback_date
            #if ago.days <= 7:
            #    self.embargoed_record_count += 1
            #    continue

            # Strip out Windows newlines
            if 'description' in target and target['description'] != None:
                target['description'] = target['description'].replace('\r\n', '\n')

            # Otherwise, build the record:
            rec = {
                'record_id': record_id,
                'date': first_date,
                'target_url': url,
                'title': target['title'],
                'publisher': publisher,
                'description': target['description'],
                'start_date': target['crawlStartDateISO'],
                'end_date': target['crawlEndDateISO'],
                'open_access': target['hasOpenAccessLicense']
            }

            # Add any collection:
            if len(target['collectionIds']) > 0:
                col = collections_by_id.get(int(target['collectionIds'][0]), {})
                rec['subject'] = col.get('name', None)

            # And write:
            target_md = luigi.LocalTarget("/Users/andy/Documents/workspace/ukwa-site/content/target/%s/index.en.md" % tid)
            with target_md.open('w') as f:
                for part in template.generate({ "record": rec, "json": json.dumps(rec, indent=2) }):
                    f.write(part.encode("utf-8"))



if __name__ == '__main__':
    luigi.run(['site.GenerateSitePages', '--local-scheduler'])
