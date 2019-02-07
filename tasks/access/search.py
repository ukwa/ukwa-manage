# -*- coding: utf-8 -*-
import json
import luigi
import pysolr
import logging
import base64
import hashlib
import datetime
import tldextract
from lib.cdx import CdxIndex
from tasks.ingest.w3act import TargetList, SubjectList, CollectionList
from tasks.common import state_file
from jinja2 import Environment, PackageLoader
from prometheus_client import CollectorRegistry, Gauge

logger = logging.getLogger('luigi-interface')


class GenerateIndexAnnotations(luigi.Task):
    """
    Gets the annotations needed for full-text indexing.
    """
    task_namespace = 'discovery'
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return state_file(self.date,'access-data', 'indexer-annotations.json')

    def requires(self):
        return [TargetList(), CollectionList(), SubjectList()]

    def add_annotations(self, annotations, collection, targets_by_id, prefix=""):
        # assemble full collection name:
        collection_name = "%s%s" % (prefix, collection['name'])
        # deal with all targets:
        for tid in collection['targetIds']:
            if tid not in targets_by_id:
                logger.error("Target %i not found in targets list!" % tid)
                continue
            target = targets_by_id[tid]
            scope = target['field_scope']
            if scope is None:
                logger.error("Scope not set for %s - %s!" % (tid, target['fieldUrls']) )
                continue
            for fieldUrl in target['fieldUrls']:
                url = fieldUrl['url']
                ann = annotations['collections'][scope].get(url, {'collection': collection_name, 'collections': [], 'subject': []})
                if collection_name not in ann['collections']:
                    ann['collections'].append(collection_name)
                # And subjects:
                for sid in target['subjectIds']:
                    subject_name = self.subjects_by_id[sid]['name']
                    if subject_name not in ann['subject']:
                        ann['subject'].append(subject_name)
                # and patch back in:
                annotations['collections'][scope][url] = ann

        # And add date ranges:
        annotations['collectionDateRanges'][collection_name] = {}
        if collection['startDate']:
            annotations['collectionDateRanges'][collection_name]['start'] = datetime.datetime.utcfromtimestamp(collection['startDate'] / 1e3).isoformat()
        else:
            annotations['collectionDateRanges'][collection_name]['start'] = None
        if collection['endDate']:
            annotations['collectionDateRanges'][collection_name]['end'] = datetime.datetime.utcfromtimestamp(collection['endDate'] / 1e3).isoformat()
        else:
            annotations['collectionDateRanges'][collection_name]['end'] = None

        # And process child collections:
        for child_collection in collection['children']:
            self.add_annotations(annotations, child_collection, targets_by_id, prefix="%s|" % collection_name)

    def run(self):
        targets = json.load(self.input()[0].open())
        collections = json.load(self.input()[1].open())
        subjects = json.load(self.input()[2].open())

        # build look-up table for Target IDs
        targets_by_id = {}
        target_count = 0
        for target in targets:
            tid = target['id']
            targets_by_id[tid] = target
            target_count += 1
        logger.info("Found %i targets..." % target_count)

        # build look-up table for subjects
        self.subjects_by_id = {}
        for top_level_subject in subjects:
            self.subjects_by_id[top_level_subject['id']] = top_level_subject
            for child_subject in top_level_subject['children']:
                self.subjects_by_id[child_subject['id']] = child_subject

        # Assemble the annotations, keyed on scope + url:
        annotations = {
            "collections": {
                "subdomains": {
                },
                "resource": {
                },
                "root": {
                },
                "plus1": {
                }
            },
            "collectionDateRanges": {
            }
        }

        for collection in collections:
            self.add_annotations(annotations, collection, targets_by_id)

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(annotations, indent=4)))


class GenerateW3ACTTitleExport(luigi.Task):
    task_namespace = 'discovery'
    date = luigi.DateParameter(default=datetime.date.today())

    record_count = 0
    blocked_record_count = 0
    missing_record_count = 0
    embargoed_record_count = 0

    target_count = 0
    collection_count = 0
    collection_published_count = 0
    subject_count = 0

    def requires(self):
        return [TargetList(self.date), CollectionList(self.date), SubjectList(self.date)]

    def output(self):
        return state_file(self.date,'access-data', 'title-level-metadata-w3act.xml')

    def run(self):
        # Get the data:
        targets = json.load(self.input()[0].open())
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

        # Convert to records:
        records = []
        for target in targets:
            # Skip blocked items:
            if target['field_crawl_frequency'] == 'NEVERCRAWL':
                logger.warning("The Target '%s' is blocked (NEVERCRAWL)." % target['title'])
                self.blocked_record_count += 1
                continue
            # Skip items that have no crawl permission?
            # hasOpenAccessLicense == False, and inScopeForLegalDeposit == False ?
            # Skip items with no URLs:
            if len(target['fieldUrls']) == 0:
                continue
            # Get the url, use the first:
            url = target['fieldUrls'][0]['url']
            # Extract the domain:
            parsed_url = tldextract.extract(url)
            publisher = parsed_url.registered_domain
            # Lookup in CDX:
            wayback_date_str = CdxIndex().get_first_capture_date(url) # Get date in '20130401120000' form.
            if wayback_date_str is None:
                logger.warning("The URL '%s' is not yet available, inScopeForLegalDeposit = %s" % (url, target['inScopeForLegalDeposit']))
                self.missing_record_count += 1
                continue
            wayback_date = datetime.datetime.strptime(wayback_date_str, '%Y%m%d%H%M%S')
            first_date = wayback_date.isoformat()
            record_id = "%s/%s" % (wayback_date_str, base64.b64encode(hashlib.md5(url.encode('utf-8')).digest()))

            # Honour embargo
            ago = datetime.datetime.now() - wayback_date
            if ago.days <= 7:
                self.embargoed_record_count += 1
                continue

            # Otherwise, build the record:
            rec = {
                'id': record_id,
                'date': first_date,
                'url': url,
                'title': target['title'],
                'publisher': publisher
            }
            # Add any collection:
            if len(target['collectionIds']) > 0:
                col = collections_by_id.get(int(target['collectionIds'][0]), {})
                rec['subject'] = col.get('name', None)

            # And append record to the set:
            records.append(rec)
            self.record_count += 1

        # Setup templates:
        env = Environment(loader=PackageLoader('tasks.access.search', 'templates'))
        template = env.get_template('title-level-template.xml')

        # And write:
        with self.output().open('w') as f:
            for part in template.generate({ "records": records }):
                f.write(part.encode("utf-8"))

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_record_count',
                  'Total number of UKWA records.',
                    labelnames=['kind', 'status'], registry=registry)

        g.labels(kind='targets', status='_any_').set(self.target_count)
        g.labels(kind='collections', status='_any_').set(self.collection_count)
        g.labels(kind='collections', status='published').set(self.collection_published_count)
        g.labels(kind='subjects', status='_any_').set(self.subject_count)

        g.labels(kind='title_level', status='complete').set(self.record_count)
        g.labels(kind='title_level', status='blocked').set(self.blocked_record_count)
        g.labels(kind='title_level', status='missing').set(self.missing_record_count)
        g.labels(kind='title_level', status='embargoed').set(self.embargoed_record_count)


class UpdateCollectionsSolr(luigi.Task):
    task_namespace = 'discovery'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())
    solr_endpoint = luigi.Parameter(default='http://localhost:8983/solr/collections')

    def requires(self):
        return [TargetList(self.date), CollectionList(self.date), SubjectList(self.date)]

    def output(self):
        return state_file(self.date,'access-data', 'updated-collections-solr.json')

    @staticmethod
    def add_collection(s, targets_by_id, col, parent_id):
        if col['field_publish']:
            print("Publishing...", col['name'])

            # add a document to the Solr index
            s.add([
                {
                    "id": col["id"],
                    "type": "collection",
                    "name": col["name"],
                    "description": col["description"],
                    "parentId": parent_id
                }
            ], commit=False)

            # Look up all Targets within this Collection and add them.
            for tid in col['targetIds']:
                target = targets_by_id.get(tid, None)
                if not target:
                    logger.error("Warning! Could not find target %i" % tid)
                    continue

                # Skip items with no URLs:
                if len(target['fieldUrls']) == 0:
                    continue

                # Determine license status:
                licenses = []
                if target.get('hasOpenAccessLicense', False):
                    licenses = [l["id"] for l in target["licenses"]]
                    # Use a special value to indicate an inherited license:
                    if len(licenses) == 0:
                        licenses = ['1000']

                # add a document to the Solr index
                s.add([{
                    "id": "cid:%i-tid:%i" % (col['id'], target['id']),
                    "type": "target",
                    "parentId": col['id'],
                    "title": target["title"],
                    "description": target["description"],
                    "url": target["fieldUrls"][0]["url"],
                    "additionalUrl": [t["url"] for t in target["fieldUrls"] if t["position"] > 0],
                    "language": target["language"],
                    "startDate": target["crawlStartDateISO"],
                    "endDate": target["crawlEndDateISO"],
                    "licenses": licenses
                }], commit=False)

            # Add child collections
            for cc in col["children"]:
                UpdateCollectionsSolr.add_collection(s, targets_by_id, cc, col['id'])
        else:
            print("Skipping...", col['name'])

        return

    def run(self):
        targets = json.load(self.input()[0].open())
        collections = json.load(self.input()[1].open())
        subjects = json.load(self.input()[2].open())

        # build look-up table for Target IDs
        targets_by_id = {}
        target_count = 0
        for target in targets:
            tid = target['id']
            targets_by_id[tid] = target
            target_count += 1
        logger.info("Found %i targets..." % target_count)

        s = pysolr.Solr(self.solr_endpoint, timeout=30)

        # First, we delete everything (!)
        s.delete(q="*:*", commit=False)

        # Update the collections:
        for col in collections:
            UpdateCollectionsSolr.add_collection(s, targets_by_id, col, None)

        # Now commit all changes:
        s.commit()

        # Record that we have completed this task successfully:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(collections, indent=4)))


class PopulateBetaCollectionsSolr(luigi.WrapperTask):
    task_namespace = 'discovery'

    def requires(self):
        return UpdateCollectionsSolr(solr_endpoint='http://ukwadev2:8983/solr/collections')


if __name__ == '__main__':
    #luigi.run(['discovery.UpdateCollectionsSolr',  '--date', '2017-04-28', '--local-scheduler'])
    luigi.run(['discovery.PopulateBetaCollectionsSolr', '--local-scheduler'])
