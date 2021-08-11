import re
import json
import shutil
import logging
import datetime
import surt
from tasks.crawl.w3act import CrawlFeedAllOpenAccess
import luigi.contrib.hadoop_jar
from tasks.common import state_file, CopyToTableInDB
from prometheus_client import CollectorRegistry, Gauge

logger = logging.getLogger('luigi-interface')


class GenerateAccessWhitelist(luigi.Task):
    """
    Gets the open-access whitelist needed for full-text indexing.
    """
    task_namespace = 'access'
    date = luigi.DateParameter(default=datetime.date.today())

    all_surts = set()
    all_surts_and_urls = list()

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
    #allSurtsFile = '/opt/wayback-whitelist.txt'
    #w3actURLsFile = '/home/tomcat/oukwa-wayback-whitelist/w3act_urls'

    def output(self):
        return {
            'owb': state_file(self.date,'access-data', 'access-whitelist-beta.txt'),
            'pywb': state_file(self.date,'access-data', 'access-whitelist-beta.aclj')
        }

    def requires(self):
        return CrawlFeedAllOpenAccess()

    def generate_surt(self, url):
        if self.RE_NONCHARS.search(url):
            logger.warn("Questionable characters found in URL [%s]" % url)
            return None

        surtVal = surt.surt(url)

        #### WA: ensure SURT has scheme of original URL ------------
        # line_scheme = RE_SCHEME.match(line)           # would allow http and https (and any others)
        line_scheme = 'http://'  # for wayback, all schemes need to be only http
        surt_scheme = self.RE_SCHEME.match(surtVal)

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

    def surts_for_cdns(self):
        '''
        This adds in dome hard-coded SURTs that correspond to common CDNs.

        :return:
        '''
        cdn_surts = [
            'http://(com,wp,s0',
            'http://(com,wp,s1',
            'http://(com,wp,s2',
            'http://(com,wordpress,files,',
            'http://(com,twimg,',
            'http://(com,blogspot,bp,',
            'http://(com,blogblog,img1',
            'http://(com,blogblog,img2',
            'http://(com,squarespace,static)',
            'http://(net,typekit,use)',
            'http://(com,blogger,www)/img/',
            'http://(com,blogger,www)/static/',
            'http://(com,blogger,www)/dyn-css/',
            'http://(net,cloudfront,',
            'http://(com,googleusercontent,',
            # See https://github.com/ukwa/ukwa-manage/issues/77
            'http://(uk,co,bbc,newsimg,',
            # Allow Twitter Service Worker and API.
            'http://(com,twitter)/sw.js',
            'http://(com,twitter,api)'
        ]
        # Add them in:
        for cdn_surt in cdn_surts:
            self.all_surts.add(cdn_surt)
            self.all_surts_and_urls.append({
                'surt': cdn_surt,
                'url': cdn_surt
            })

        logger.info("%s surts for CDNs added" % len(cdn_surts))

    def surts_from_w3act(self):
        # Surts from ACT:
        with self.input().open() as f:
            targets = json.load(f)
        for target in targets:
            for seed in target['seeds']:
                act_surt = self.generate_surt(seed)
                if act_surt is not None:
                    self.all_surts.add(act_surt)
                    self.all_surts_and_urls.append({
                        'surt': act_surt,
                        'url': seed
                    })
                else:
                    logger.warning("Got no SURT from %s" % seed)

    def run(self):
        # collate surts
        self.surts_for_cdns()
        self.surts_from_w3act()

        # And write out the SURTs:
        with self.output()['owb'].open('w') as f:
            for surt in sorted(self.all_surts):
                f.write("%s\n" % surt)
        # Also in pywb format:
        with self.output()['pywb'].open('w') as f:
            pywb_rules = set()
            for item in self.all_surts_and_urls:
                rule = {
                    'access': 'allow',
                    'url': item['url']
                }
                surt = item['surt']
                surt = surt.replace('http://(', '', 1)
                surt = surt.rstrip(',') # Strip any trailing comma
                pywb_rules.add("%s - %s" % (surt, json.dumps(rule)))
            for rule in sorted(pywb_rules, reverse=True):
                f.write("%s\n" % rule)

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None

        g = Gauge('ukwa_url_count',
                  'Number of URLs.',
                  labelnames=['set'], registry=registry)
        g.labels(set='ukwa-oa').set(len(self.all_surts))


class UpdateAccessWhitelist(luigi.Task):
    """
    This takes the updated access list and puts it in the right place.
    """
    task_namespace = 'access'
    date = luigi.DateParameter(default=datetime.date.today())
    wb_oa_whitelist = luigi.Parameter(default='/root/wayback-config/open-access-whitelist.txt')
    pywb_oa_whitelist = luigi.Parameter(default='/root/wayback-config/acl/allows.aclj')

    def requires(self):
        return GenerateAccessWhitelist(self.date)

    def output(self):
        return state_file(self.date,'access-data', 'access-whitelist-updated.txt')

    def run(self):
        # Copy the file to the deployment location (atomically):
        temp_path = "%s.tmp" % self.wb_oa_whitelist
        shutil.copy(self.input()['owb'].path, temp_path)
        shutil.move(temp_path, self.wb_oa_whitelist)

        # Copy the file to the deployment location (atomically):
        temp_path = "%s.tmp" % self.pywb_oa_whitelist
        shutil.copy(self.input()['pywb'].path, temp_path)
        shutil.move(temp_path, self.pywb_oa_whitelist)

        # Note that we've completed this work successfully
        with self.output().open('w') as f:
            f.write('Written SURTS from %s to %s' % (self.input()['pywb'].path, self.pywb_oa_whitelist))
            f.write('Written SURTS from %s to %s' % (self.input()['owb'].path, self.wb_oa_whitelist))


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.interface.setup_interface_logging()

    luigi.run(['CdxIndexAndVerify', '--workers', '10'])

#    very = CdxIndexAndVerify(
#        date=datetime.datetime.strptime("2018-02-16","%Y-%m-%d"),
#        target_date = datetime.datetime.strptime("2018-02-10", "%Y-%m-%d")
#    )
#    cdx = CheckCdxIndex(input_file=very.input().path)
#    cdx.run()

    #input = os.path.join(os.getcwd(),'test/input-list.txt')
    #luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
