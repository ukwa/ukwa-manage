import os
import re
import json
import luigi
import logging
import datetime
from surt import surt
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
    feed = luigi.Parameter(default='ld')
    date = luigi.DateHourParameter(default=datetime.datetime.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/crawl-feed-%s.%s.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], self.feed, datetime_string, self.frequency))

    def run(self):
        # Set up connection to W3ACT:
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        # Grab those targets:
        if self.feed == 'oa':
            targets = w.get_oa_export(self.frequency)
        else:
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


class CollectionList(luigi.Task):
    """
    Get the lists of all collections.

    Only generated once per day as this is rather heavy going.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/collection-list.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

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
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/subject-list.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

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
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/target-list.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

    def run(self):
        # Paged downloads...
        w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
        page = 0
        targets = []
        while True:
            tpage = w.get_targets(page=page, page_length=1000)
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


RE_NONCHARS = re.compile(r"""
[^	# search for any characters that aren't those below
\w
:
/
\.
\-
=
?
&
~
%
+
@
,
;
]
""", re.VERBOSE)
RE_SCHEME = re.compile('https?://')
allSurtsFile = '/opt/wayback-whitelist.txt'
w3actURLsFile = '/home/tomcat/oukwa-wayback-whitelist/w3act_urls'


class GenerateAccessWhitelist(luigi.Task):
    """
    Gets the open-access whitelist needed for full-text indexing.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())
    wct_url_file = luigi.Parameter(default=os.path.join(os.path.dirname(__file__), "wct_urls.txt"))

    all_surts = set()

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/access-whitelist.%s.txt' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

    def requires(self):
        return CrawlFeed('all','oa')

    def generate_surt(self, url):
        if RE_NONCHARS.search(url):
            logger.warn("Questionable characters found in URL [%s]" % url)

        surtVal = surt(url)

        #### WA: ensure SURT has scheme of original URL ------------
        # line_scheme = RE_SCHEME.match(line)           # would allow http and https (and any others)
        line_scheme = 'http://'  # for wayback, all schemes need to be only http
        surt_scheme = RE_SCHEME.match(surtVal)

        if line_scheme and not surt_scheme:
            if re.match(r'\(', surtVal):
                # surtVal = line_scheme.group(0) + surtVal
                surtVal = line_scheme + surtVal
                logger.debug("Added scheme [%s] to surt [%s]" % (line_scheme, surtVal))
            else:
                # surtVal = line_scheme.group(0) + '(' + surtVal
                surtVal = line_scheme + '(' + surtVal
                # logger.debug("Added scheme [%s] and ( to surt [%s]" % (line_scheme, surtVal))

        surtVal = re.sub(r'\)/$', ',', surtVal)

        return surtVal

    def surts_from_wct(self):
        count = 0
        # process every URL from WCT
        with open(self.wct_url_file, 'r') as wcturls:
            # strip any whitespace from beginning or end of line
            lines = wcturls.readlines()
            lines = [l.strip() for l in lines]

            # for all WCT URLs, generate surt. Using a set disallows duplicates
            for line in lines:
                self.all_surts.add(self.generate_surt(line))
                count += 1

        logger.info("%s surts from WCT generated" % count)

    def surts_from_w3act(self):
        # Surts from ACT:
        with self.input().open() as f:
            targets = json.load(f)
        for target in targets:
            for seed in target['seeds']:
                surtVal = self.generate_surt(seed)
                self.all_surts.add(surtVal)


    def run(self):
        # collate surts
        self.surts_from_wct()
        self.surts_from_w3act()

        # And write out the SURTs:
        with self.output().open('w') as f:
            for surt in sorted(self.all_surts):
                f.write("%s\n" % surt)


class GenerateIndexAnnotations(luigi.Task):
    """
    Gets the annotations needed for full-text indexing.
    """
    task_namespace = 'w3act'
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        datetime_string = self.date.strftime(luigi.DateParameter.date_format)
        return luigi.LocalTarget('%s/%s/w3act/indexer-annotations.%s.json' % (
            LUIGI_STATE_FOLDER, datetime_string[0:7], datetime_string))

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


class GenerateAnnotationsAndWhitelist(luigi.WrapperTask):
    task_namespace = 'w3act'

    def requires(self):
        return [ GenerateAccessWhitelist(), GenerateIndexAnnotations() ]

if __name__ == '__main__':
    luigi.run(['w3act.GenerateAnnotationsAndWhitelist', '--local-scheduler'])
