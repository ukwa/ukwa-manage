import os
import json
import luigi
import socket
import logging
import requests
import datetime
from lxml import etree
from crawl.h3 import hapyx
from common import *

logger = logging.getLogger('luigi-interface')

socket.setdefaulttimeout(10)

class CheckStatus(luigi.Task):
    """
    """
    task_namespace = 'monitor'
    date = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def output(self):
        return luigi.LocalTarget('%s/monitor/checkstatus.%s' % (state().state_folder, self.date.strftime(luigi.DateMinuteParameter.date_format)))

    def run(self):
        servers = self.load_as_json(systems().servers)
        services = self.load_as_json(systems().services)

        for job in services.get('jobs', []):
            server = servers[services['jobs'][job]['server']]
            # app.logger.info(json.dumps(server, indent=4))
            services['jobs'][job]['state'] = self.get_h3_status(services['jobs'][job]['name'], server)
            services['jobs'][job]['url'] = server['url']

        for queue in services.get('queues', []):
            server = servers[services['queues'][queue]['server']]
            services['queues'][queue]['prefix'] = server['user_prefix']
            services['queues'][queue]['state'] = self.get_queue_status(services['queues'][queue]['name'], server)

        for http in services.get('http', []):
            services['http'][http]['state'] = self.get_http_status(services['http'][http]['url'])

        for hdfs in services.get('hdfs', []):
            services['hdfs'][hdfs]['state'] = self.get_hdfs_status(services['hdfs'][hdfs])

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(services, indent=4)))

    def get_h3_status(self, job, server):
        # Set up connection to H3:
        h = hapyx.HapyX(server['url'], username=server['user'], password=server['pass'], timeout=5)
        state = {}
        try:
            logger.info("Getting status for job %s on %s" % (job, server))
            info = h.get_job_info(job)
            state['details'] = info
            if info.has_key('job'):
                state['status'] = info['job'].get("crawlControllerState", None)
                if not state['status']:
                    state['status'] = info['job'].get("statusDescription", None)
                state['status'] = state['status'].upper()
        except Exception as e:
            state['status'] = "DOWN"
            state['error'] = "Could not reach Heritrix! %s" % e
            # app.logger.exception(e)
        # Classify
        if state['status'] == "DOWN":
            state['status-class'] = "status-oos"
        elif state['status'] == "RUNNING":
            # Replacing RUNNING with docs/second rate
            rate = state['details']['job']['rateReport']['currentDocsPerSecond']
            state['rate'] = "%.1f" % float(rate)
            if rate < 1.0:
                state['status-class'] = "status-warning"
            else:
                state['status-class'] = "status-good"
        else:
            state['status-class'] = "status-warning"

        return state

    def get_queue_status(self, queue, server):
        state = {}
        try:
            logger.info("Getting status for queue %s on %s" % (queue, server))
            qurl = '%s%s' % (server['prefix'], queue)
            # app.logger.info("GET: %s" % qurl)
            r = requests.get(qurl, timeout=5)
            state['details'] = r.json()
            state['count'] = "{:0,}".format(state['details']['messages'])
            if 'error' in state['details']:
                state['status'] = "ERROR"
                state['status-class'] = "status-alert"
                state['error'] = state['details']['reason']
            elif state['details']['consumers'] == 0:
                state['status'] = "BECALMED"
                state['status-class'] = "status-oos"
                state['error'] = 'No consumers!'
            else:
                state['status'] = state['details']['messages']
                state['status-class'] = "status-good"
        except Exception as e:
            state['status'] = "DOWN"
            state['status-class'] = "status-alert"
            logger.exception(e)

        return state

    def get_http_status(self, url):
        state = {}
        try:
            logger.info("Getting status for %s" % (url))
            r = requests.get(url, allow_redirects=False, timeout=10)
            state['status'] = "%s" % r.status_code
            if r.status_code / 100 == 2 or r.status_code / 100 == 3:
                state['status'] = "%.3fs" % r.elapsed.total_seconds()
                state['status-class'] = "status-good"
            else:
                state['status-class'] = "status-warning"
        except:
            state['status'] = "DOWN"
            state['status-class'] = "status-alert"

        return state

    def get_hdfs_status(self, hdfs):
        state = {}
        try:
            logger.info("Getting status for hdfs %s" % (hdfs))
            r = requests.get(hdfs['url'], timeout=5)
            state['status'] = "%s" % r.status_code
            if r.status_code / 100 == 2:
                state['status-class'] = "status-good"
                tree = etree.fromstring(r.text, etree.HTMLParser())
                percent = tree.xpath("//div[@id='dfstable']//tr[5]/td[3]")[0].text
                percent = percent.replace(" ", "")
                state['percent'] = percent
                state['remaining'] = tree.xpath("//div[@id='dfstable']//tr[4]/td[3]")[0].text.replace(" ", "")
                underr = int(tree.xpath("//div[@id='dfstable']//tr[10]/td[3]")[0].text)
                if underr != 0:
                    state['status'] = "HDFS has %i under-replicated blocks!" % underr
                    state['status-class'] = "status-warning"
            else:
                state['status-class'] = "status-warning"
        except Exception as e:
            logger.exception(e)
            state['status'] = "DOWN"
            state['status-class'] = "status-alert"

        return state

    def load_as_json(self, filename):
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r') as fi:
            return json.load(fi)


if __name__ == '__main__':
    luigi.run(['monitor.CheckStatus', '--local-scheduler'])
