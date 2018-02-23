import os
import re
import json
import luigi
import datetime
from surt import surt
from ukwa.tasks.w3act.feeds import CrawlFeed
from ukwa.tasks.common import LUIGI_STATE_FOLDER, logger

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
    task_namespace = 'access'
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
