# -*- coding: utf-8 -*-
import re
import json
import luigi
import pysolr
import logging
import base64
import hashlib
import datetime
import tldextract
import unicodedata
#from lib.cdx import CdxIndex
#from tasks.ingest.w3act import TargetList, SubjectList, CollectionList
from tasks.common import state_file
from jinja2 import Environment, PackageLoader
from prometheus_client import CollectorRegistry, Gauge

logger = logging.getLogger('luigi-interface')


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to hyphens. Also strips leading and
    trailing whitespace.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return re.sub('[-\s]+', '-', value)


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

    def filter_down(self, targets, collections):
        '''
        This is used to filter down the whole collection to a much smaller subset, for rapid testing purposes.
        :return:
        '''
        new_collections = []
        new_collection_ids = []
        for col in collections:
            if len(new_collections) < 5 or col['id'] == 329: # Add in known collection with sub-collections
                if col['field_publish']:
                    new_collections.append(col)
                    new_collection_ids.append(col['id'])

        new_targets = []
        for target in targets:
            for col_id in target['collectionIds']:
                if col_id in new_collection_ids:
                    new_targets.append(target)

        return new_targets, new_collections

    def run(self):
        # Get the data:
        targets = json.load(self.input()[0].open(),encoding='utf-8')
        self.target_count = len(targets)
        collections = json.load(self.input()[1].open())
        self.collection_count = len(collections)
        subjects = json.load(self.input()[2].open())
        self.subject_count = len(subjects)

        # Filter down, for testing:
        targets, collections = self.filter_down(targets,collections)

        # Index collections by ID:
        collections_by_id = {}
        for col in collections:
            collections_by_id[int(col['id'])] = col
            if col['field_publish']:
                self.collection_published_count += 1

        # Index targets by ID:
        targets_by_id = {}
        for target in targets:
            targets_by_id[int(target['id'])] = target

        # Setup template environment:
        env = Environment(loader=PackageLoader('tasks.access.sitegen', 'templates'))

        # Targets
        # FIXME this should build up an 'id' to 'page-source-path' mapping, and link to collections:
        self.generate_targets(env, targets, collections_by_id)

        # Collections
        # FIXME this should output targets using 'page-source-path' rather than ID:
        self.generate_collections("/Users/andy/Documents/workspace/ukwa-site/content/collection", env, collections, targets_by_id)

    def generate_collections(self, base_path, env, collections, targets_by_id):
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
            # Use the ID for the URL:
            col['url'] = "collection/%s/" % col['id']
            col['file_path'] = "%s/%s" % (base_path, slugify(col['name']))
            # Recurse to generate child collections:
            if 'children' in col:
                self.generate_collections(col['file_path'], env, col['children'], targets_by_id)
            col.pop('children', None)

            # Collect the Targets
            col['targets'] = []
            col['stats'] = {}
            col['stats']['num_targets'] = 0
            col['stats']['num_oa_targets'] = 0
            for tid in col['targetIds']:
                target = targets_by_id.get(tid, None)
                # FIXME blocking etc.
                col_target = {}
                col_target['id'] = tid
                col_target['title'] = target['title']
                col_target['file_path'] = self.get_target_file_path(target)
                col_target['open_access'] = target['hasOpenAccessLicense']
                if target:
                    col['targets'].append(col_target)
                    col['stats']['num_targets'] += 1
                    if col_target['open_access']:
                        col['stats']['num_oa_targets'] += 1
            # And remove the plain TID list:
            col.pop('targetIds', None)

            # and write:
            col_md = luigi.LocalTarget("%s/_index.en.md" % col['file_path'])
            with col_md.open('w') as f:
                for part in template.generate({ "record": col, "json": json.dumps(col, indent=2), "description": col['description'] }):
                    f.write(part.encode("utf-8"))

    def get_target_start_date(self, target):
        start_date = target.get('crawlStartDateISO')
        if start_date is None:
            start_date = "2006-01-01T12:00:00Z"
        return start_date

    def get_target_file_path(self, target):
        start_date = self.get_target_start_date(target)
        return "%s/%s-%s" % (start_date[:4], start_date[:10], slugify(target['title']))

    def generate_targets(self, env, targets, collections_by_id):
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
            start_date = self.get_target_start_date(target)
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
                'slug': tid,
                'id': target['id'],
                'wct_id': target.get('field_wct_id', None),
                'record_id': record_id,
                'date': first_date,
                'target_url': url,
                'title': target['title'],
                'publisher': publisher,
                'start_date': target['crawlStartDateISO'],
                'end_date': target['crawlEndDateISO'],
                'open_access': target['hasOpenAccessLicense'],
                'npld': target['inScopeForLegalDeposit'],
                'scope': target['field_scope'],
                'nominating_organisation': target.get('nominating_organisation', {}).get('title',None),
                'collections': [],
                'subjects': []
            }

            # Add any collection:
            for col_id in target['collectionIds']:
                col = collections_by_id.get(int(col_id), {})
                if 'name' in col:
                    rec['collections'].append({
                        'id': col['id'],
                        'name': col['name']
                    })

            # For subjects
            for sub_id in target['subjectIds']:
                pass
                #col = subjects.get(int(target['collectionIds'][0]), {})
                #if 'name' in col:
                #    rec['collections'].append({
                #        'id': col['id'],
                #        'name': col['name']
                #    })

            # And the organisation:
            if 'nominating_organisation' in target and target['nominating_organisation'] != None:
                rec['organisation'] = {
                    'id': target['nominating_organisation']['id'],
                    'name': target['nominating_organisation']['title'],
                    'abbreviation': target['nominating_organisation']['field_abbreviation']
                }

            # And write:
            file_path = self.get_target_file_path(target)
            target['file_path'] = file_path
            target_md = luigi.LocalTarget("/Users/andy/Documents/workspace/ukwa-site/content/target/%s/index.en.md" % file_path)
            with target_md.open('w') as f:
                for part in template.generate({ "record": rec, "json": json.dumps(rec, indent=2), "description": target['description'] }):
                    f.write(part.encode("utf-8"))



if __name__ == '__main__':
    luigi.run(['site.GenerateSitePages', '--local-scheduler'])

    #
    # Example Target
    #
    # {
    #     "formUrl": null,
    #     "webFormDateText": "",
    #     "synonyms": "",
    #     "updatedAt": 1539798881752,
    #     "keywords": "",
    #     "crawlEndDateText": "",
    #     "licenseStatus": "NOT_INITIATED",
    #     "legacySiteId": null,
    #     "archivistNotes": "",
    #     "title": "Scarlets Rugby (@scarlets_rugby) on Twitter",
    #     "field_uk_hosting": false,
    #     "field_key_site": false,
    #     "flagNotes": "",
    #     "tabStatus": null,
    #     "collectionIds": [
    #         1490,
    #         1503
    #     ],
    #     "watchedTarget": null,
    #     "format": null,
    #     "field_collection_cats": null,
    #     "field_hidden": false,
    #     "field_scope": "root",
    #     "field_notes": null,
    #     "notes": null,
    #     "summary": null,
    #     "field_no_ld_criteria_met": false,
    #     "crawlStartDateText": "18-10-2018 09:00",
    #     "field_instances": null,
    #     "webFormInfo": "",
    #     "field_description": null,
    #     "fieldUrls": [
    #         {
    #             "domain": "twitter.com",
    #             "url": "https://twitter.com/scarlets_rugby/",
    #             "updatedAt": 1539798881752,
    #             "position": 0,
    #             "id": 137364,
    #             "createdAt": null
    #         }
    #     ],
    #     "field_depth": "CAPPED",
    #     "crawlEndDateISO": null,
    #     "licenses": [],
    #     "field_collection_categories": null,
    #     "isTopLevelDomain": false,
    #     "createdAt": 1539798881752,
    #     "nominating_organisation": {
    #         "title": "The British Library",
    #         "url": "act-101",
    #         "id": 1,
    #         "updatedAt": 1423490802527,
    #         "field_abbreviation": "BL",
    #         "createdAt": 1358261596000
    #     },
    #     "whiteList": "",
    #     "tags": [],
    #     "secondLanguage": "",
    #     "field_crawl_frequency": "WEEKLY",
    #     "field_professional_judgement": true,
    #     "fieldUrl": null,
    #     "field_uk_domain": null,
    #     "justification": null,
    #     "field_wct_id": null,
    #     "isUkRegistration": false,
    #     "field_via_correspondence": false,
    #     "field_nominating_organisation": null,
    #     "active": true,
    #     "language": "EN",
    #     "selectionType": "SELECTION",
    #     "field_subjects": null,
    #     "field_uk_postal_address_url": null,
    #     "field_snapshots": null,
    #     "field_special_dispensation_reaso": null,
    #     "dateOfPublicationText": null,
    #     "field_uk_geoip": null,
    #     "collectionSelect": null,
    #     "field_professional_judgement_exp": "Published in the UK",
    #     "logoutUrl": null,
    #     "qaIssue": null,
    #     "authorUser": {
    #         "status": null,
    #         "name": "Carlos Rarugal",
    #         "language": null,
    #         "lastLogin": 1539617796449,
    #         "feed_nid": null,
    #         "created": null,
    #         "rolesAct": null,
    #         "field_affiliation": null,
    #         "url": "act-1063",
    #         "theme": null,
    #         "last_login": null,
    #         "createdAt": 1499074763947,
    #         "ddhaptUser": true,
    #         "updatedAt": 1539617796449,
    #         "mail": "carlos.rarugal@bl.uk",
    #         "last_access": null,
    #         "id": 1063,
    #         "edit_url": null,
    #         "uid": null
    #     },
    #     "webFormDate": null,
    #     "hasOpenAccessLicense": false,
    #     "field_qa_status": null,
    #     "uk_postal_address_url": "",
    #     "documentOwner": null,
    #     "field_spt_id": null,
    #     "field_nominating_org": null,
    #     "value": "",
    #     "originating_organisation": "",
    #     "field_url": null,
    #     "field_subject": null,
    #     "field_urls": null,
    #     "field_crawl_permission": null,
    #     "crawlStartDateISO": "2018-10-18T09:00:00Z",
    #     "subjectSelect": null,
    #     "field_collections": null,
    #     "selectorNotes": "",
    #     "watched": false,
    #     "id": 81628,
    #     "field_license": null,
    #     "field_ignore_robots_txt": null,
    #     "blackList": "",
    #     "dateOfPublication": null,
    #     "inScopeForLegalDeposit": true,
    #     "field_crawl_start_date": null,
    #     "revision": null,
    #     "field_suggested_collections": null,
    #     "description": "The official Twitter page of Scarlets Rugby. Trydar swyddogol y Scarlets.",
    #     "field_special_dispensation": false,
    #     "loginPageUrl": null,
    #     "subjectIds": [
    #         74,
    #         264
    #     ],
    #     "secretId": null,
    #     "field_uk_postal_address": false,
    #     "field_live_site_status": "LIVE",
    #     "selector": null,
    #     "url": "act-679914289658510445",
    #     "field_crawl_end_date": null,
    #     "authorIdText": null,
    #     "flags": [],
    #     "edit_url": null,
    #     "crawlPermissions": []
    # },
