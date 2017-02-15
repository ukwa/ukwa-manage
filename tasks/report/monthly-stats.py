#!/usr/bin/env python

# python and BL-written modules
import os
import hdfs
import json
import luigi
import luigi.contrib.hdfs
import logging
from urlparse import urlparse
from collections import Counter
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from tasks.w3act.feeds import TargetListForFrequency

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']


class GenerateMonthlyReport(luigi.Task):
    """

    """
    task_namespace = 'report'
    date_stamp = luigi.MonthParameter(default=date.today())

    a_frequencies = ["daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual", "domaincrawl", "nevercrawl"]
    a_ldls = [("The British Library", "DLS-LON-WB01"), ("Trinity College Dublin", "DLS-BSP-WB04"),
              ("Bodleian Library", "DLS-BSP-WB03"), ("Cambridge University Library", "DLS-BSP-WB02"),
              ("The National Library of Scotland", "DLS-NLS-WB01"), ("The National Library of Wales", "dls-nlw-wb01")]

    # Set up the HDFS client:
    client = hdfs.InsecureClient(url=os.environ['WEBHDFS_URL'], user=os.environ['WEBHDFS_USER'])

    def requires(self):
        feeds = {}
        for frequency in self.a_frequencies:
            feeds[frequency] = TargetListForFrequency(frequency)
        return feeds

    def output(self):#/var/www/html/act/
        datetime_string = self.date.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget('%s/%s/monthly-stats-%s.html' %
                                 (LUIGI_STATE_FOLDER, datetime_string[0:7], self.date_stamp.strftime("%Y%m%d")))

    def run(self):
        # initialise
        d_counts = {}
        d_counts['total'] = {}
        d_counts['total']['.uk'] = d_counts['total']['.scot'] = d_counts['total']['.wales'] = d_counts['total']['.cymru'] = d_counts['total']['.london'] = d_counts['total']['not_uk']= 0
        d_counts['total']['uk_domain'] = d_counts['total']['uk_geoip'] = d_counts['total']['uk_postal_address'] = d_counts['total']['via_correspondence'] = d_counts['total']['prof_judgement'] = 0
        i_wct_uids = 0
        a_orgs = []
        a_schedules = []

        # enable and start logging
        logger = logging.getLogger()
        logger.debug('Script initialized')

        # get counts
        self.process_frequent_exports(d_counts, a_schedules)
        # i_wct_uids = get_ukwa_licensed_content(w3act_exporter, logger)
        #i_new_instances = self.calculate_instances()
        i_new_instances = 0
        i_new_sips = self.calculate_sips()

        # calculate organisations and schedules
        a_orgs = Counter(a_orgs)
        a_schedules = Counter(a_schedules)

        # output results
        self.output_results(d_counts, a_orgs, i_wct_uids, a_schedules, i_new_sips, i_new_instances, self.a_ldls, logger)

    # functions ---------------------------------------------------------------------------------------
    def process_frequent_exports(self, d_counts, a_schedules):
        # for each crawl frequency
        for frequency in self.input():
            logger.debug('Reading W3ACT export for '+frequency)
            freq_export = json.load(self.input()[frequency].open())

            d_counts[frequency] = {}
            d_counts[frequency]['.uk'] = d_counts[frequency]['.scot'] = d_counts[frequency]['.wales'] = d_counts[frequency]['.cymru'] = d_counts[frequency]['.london'] = d_counts[frequency]['not_uk']= 0
            d_counts[frequency]['uk_domain'] = d_counts[frequency]['uk_geoip'] = d_counts[frequency]['uk_postal_address'] = d_counts[frequency]['via_correspondence'] = d_counts[frequency]['prof_judgement'] = 0

            # for each frequency with collected data, count URL country codes
            if freq_export is None:
                logger.debug("None returned for " + frequency)
            else:
                for node in freq_export:
                    #logger.info(node)
                    a_schedules.append(frequency) # This doesn't really make sense I think?
                    for url in [u["url"] for u in node["fieldUrls"]]:
                        if urlparse(url).netloc.endswith(".uk"):
                            d_counts[frequency]['.uk'] += 1
                            d_counts[frequency]['uk_domain'] += 1
                        elif urlparse(url).netloc.endswith(".london"):
                            d_counts[frequency]['.london'] += 1
                            d_counts[frequency]['uk_domain'] += 1
                        elif urlparse(url).netloc.endswith(".wales"):
                            d_counts[frequency]['.wales'] += 1
                            d_counts[frequency]['uk_domain'] += 1
                        elif urlparse(url).netloc.endswith(".cymru"):
                            d_counts[frequency]['.cymru'] += 1
                            d_counts[frequency]['uk_domain'] += 1
                        elif urlparse(url).netloc.endswith(".scot"):
                            d_counts[frequency]['.scot'] += 1
                            d_counts[frequency]['uk_domain'] += 1
                        else:
                            d_counts[frequency]['not_uk'] += 1

                    if node[ "field_uk_hosting" ]:
                        d_counts[frequency]['uk_geoip'] += 1
                    if node[ "field_uk_postal_address" ]:
                        d_counts[frequency]['uk_postal_address'] += 1
                    if node[ "field_via_correspondence" ]:
                        d_counts[frequency]['via_correspondence'] += 1
                    if node[ "field_professional_judgement" ]:
                        d_counts[frequency]['prof_judgement'] += 1

            # log frequency counts
            for subset in sorted(d_counts[frequency]):
                logger.debug("\t" + subset + " = " + str(d_counts[frequency][subset]))

            # accumulate total values
            for subset in d_counts[frequency]:
                d_counts['total'][subset] += d_counts[frequency][subset]

        # log count totals
        for subset in sorted(d_counts['total']):
            logger.info(subset + " = " + str(d_counts['total'][subset]))


    def get_ukwa_licensed_content(self, w3act_exporter, logger):
        i_wct_uids = 0
        logger.debug('Getting W3ACT export get_by_all')
        try:
            export_all = w3act_exporter.get_by_export("all")
            i_wct_uids = len(export_all)
            logger.debug('Size of get_by_all export ' + str(i_wct_uids))
        except:
            logger.debug('get_by_all export failed')
            i_wct_uids = 'ERROR: stats.py script failed to export get_by_all from W3ACT'
        return i_wct_uids

    def calculate_instances(self):
        i_new_instances = 0
        o_targets = self.client.list("/data/wayback/cdx-index/")["FileStatuses"]["FileStatus"]
        for o_target in o_targets:
            o_instances = self.client.list("/data/wayback/cdx-index/%s/" \
                % o_target["pathSuffix"])["FileStatuses"]["FileStatus"]
            for o_instance in o_instances:
                i_mod = datetime.fromtimestamp(o_instance["modificationTime"]/1000)
                if i_mod > (datetime.now() - relativedelta(months=-1)):
                    i_new_instances += 1
        logger.debug('New instances = ' + str(i_new_instances))
        return i_new_instances

    def calculate_sips(self):
        i_new_sips = 0
        o_dirs = self.client.list("/heritrix/sips/")
        logger.info(o_dirs)
        for o_dir in o_dirs:
            logger.info(o_dir)
            o_sips = self.client.list("/heritrix/sips/%s/" % o_dir["pathSuffix"])["FileStatuses"]["FileStatus"]
            for o_sip in o_sips:
                i_mod = datetime.fromtimestamp(o_sip["modificationTime"]/1000)
                if i_mod > (datetime.now() + relativedelta(months=-1)):
                    i_new_sips += 1
        logger.debug('New SIPs = ' + str(i_new_sips))
        return i_new_sips

    @staticmethod
    def print_dict(o_out, d_dict):
        for key in sorted(d_dict, key=d_dict.get, reverse=True):
            o_out.write("<tr><td>%s</td><td>%s</td></tr>\n" % (str(key), str(d_dict[key])))

    def output_results(self, d_counts, a_orgs, i_wct_uids, a_schedules, i_new_sips, i_new_instances, a_ldls, logger):
        i_ymdhmsnow = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s_outputfile = self.output()

        with open(s_outputfile, "wb") as o_out:
            o_out.write("<!DOCTYPE html>\n<html lang=\"en-GB\">\n")
            o_out.write("<head>\n<style>html * { font-family: Arial }</style>\n</head>\n<body>\n")
            o_out.write("<h1>Monthly Crawl Stats - %s</h1>\n" % datetime.now().strftime("%Y-%m-%d"))

            o_out.write("<h2>Total no of ACT records</h2>\n")
            o_out.write("<table>\n")
    #		print_dict(o_out, a_orgs)
            o_out.write("<tr><td>No of ACT records with UKWA Selective Archive Licence:</td><td>%s</td></tr>\n" % i_wct_uids)
            o_out.write("<tr><td>Total number of .uk URLs in ACT</td><td>%s</td></tr>\n" % d_counts['total']['.uk'])
            o_out.write("<tr><td>Total number of .scot URLs in ACT:</td><td>%s</td></tr>\n" % d_counts['total']['.scot'])
            o_out.write("<tr><td>Total number of .wales URLs in ACT:</td><td>%s</td></tr>\n" % d_counts['total']['.wales'])
            o_out.write("<tr><td>Total number of .cymru URLs in ACT:</td><td>%s</td></tr>\n" % d_counts['total']['.cymru'])
            o_out.write("<tr><td>Total number of .london URLs in ACT:</td><td>%s</td></tr>\n" % d_counts['total']['.london'])
            o_out.write("<tr><td>Total number of non UK Domain URLs in ACT:</td><td>%s</td></tr>\n" % d_counts['total']['not_uk'])
            o_out.write("</table>\n")

            o_out.write("<h2>Summary crawl schedules report</h2>\n")
            o_out.write("<table>\n")
    #		print_dict(o_out, a_schedules)
            o_out.write("<tr><td>UK Domain</td><td>%s</td></tr>\n" % d_counts['total']['uk_domain'])
            o_out.write("<tr><td>UK GeoIP:</td><td>%s</td></tr>\n" % d_counts['total']['uk_geoip'])
            o_out.write("<tr><td>UK Postal Address:</td><td>%s</td></tr>\n" % d_counts['total']['uk_postal_address'])
            o_out.write("<tr><td>Via Correspondence:</td><td>%s</td></tr>\n" % d_counts['total']['via_correspondence'])
            o_out.write("<tr><td>Professional Judgement:</td><td>%s</td></tr>\n" % d_counts['total']['prof_judgement'])
            o_out.write("<tr><td>SIPs created:</td><td>%s</td></tr>\n" % i_new_sips)
            o_out.write("<tr><td>Instances migrated to UKWA:</td><td>%s</td></tr>\n" % i_new_instances)

            # subprocess details for individual LDLs
    #		logger.debug("Starting subprocesses")
    #		today = date.today()
    #		last_month = today - relativedelta(months=1)
    #		for name, server in a_ldls:
    #			logger.debug("Processing {0}: {1} - {2} {3}".format(server, name, last_month.strftime("%b"), last_month.strftime("%Y")))
    #			subprocess.Popen([LDREPORT, last_month.strftime("%b"), last_month.strftime("%Y"), server, name], stdout=o_out, stderr=subprocess.STDOUT)
            o_out.write("<tr><td cols=\"2\"><br/><b>LDL stats produced differently now, ask about da-super wastats</b></td></tr>\n")

        with open(s_outputfile, "ab") as o_out:
            o_out.write("</table>\n</body>\n</html>")

# main --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    luigi.run(['report.GenerateMonthlyReport', '--local-scheduler'])
