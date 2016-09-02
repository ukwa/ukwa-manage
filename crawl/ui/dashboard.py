from __future__ import absolute_import

import os
import json
import requests
from lxml import etree
import crawl.tasks
from crawl.h3 import hapyx
from flask import Flask
from flask import render_template, redirect, url_for
app = Flask(__name__)


def get_h3_status(job, server):
    # Set up connection to H3:
    h = hapyx.HapyX(server['url'], username=server['user'], password=server['pass'])
    state = {}
    try:
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
        #app.logger.exception(e)
    # Classify
    if state['status'] == "DOWN":
        state['status-class'] = "status-oos"
    elif state['status'] == "RUNNING":
        # Replacing RUNNING with docs/second rate
        rate = state['details']['job']['rateReport']['currentDocsPerSecond']
        state['status'] = "%.2f URI/s" % float(rate)
        if rate < 1.0:
            state['status-class'] = "status-warning"
        else:
            state['status-class'] = "status-good"
    else:
        state['status-class'] = "status-warning"

    return state


def get_queue_status(queue, server):
    state = {}
    try:
        qurl = '%s%s' %( server['prefix'], queue)
        #app.logger.info("GET: %s" % qurl)
        r = requests.get(qurl)
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
        app.logger.exception(e)

    return state


def get_http_status(url):
    state = {}
    try:
        r = requests.get(url, allow_redirects=False)
        state['status'] = "%s" % r.status_code
        if r.status_code/100 == 2 or r.status_code/100 == 3:
            state['status'] = "%.3fs" % r.elapsed.total_seconds()
            state['status-class'] = "status-good"
        else:
            state['status-class'] = "status-warning"
    except:
        state['status'] = "DOWN"
        state['status-class'] = "status-alert"

    return state


def get_hdfs_status(hdfs):
    state = {}
    try:
        r = requests.get(hdfs['url'])
        state['status'] = "%s" % r.status_code
        if r.status_code / 100 == 2:
            state['status-class'] = "status-good"
            tree = etree.fromstring(r.text, etree.HTMLParser())
            percent = tree.xpath("//div[@id='dfstable']//tr[5]/td[3]")[0].text
            percent = percent.replace(" ","")
            state['percent'] = percent
            state['remaining'] = tree.xpath("//div[@id='dfstable']//tr[4]/td[3]")[0].text.replace(" ","")
            underr = int(tree.xpath("//div[@id='dfstable']//tr[10]/td[3]")[0].text)
            if underr != 0:
                state['status'] = "HDFS has %i under-replicated blocks!" % underr
                state['status-class'] = "status-warning"
        else:
            state['status-class'] = "status-warning"
    except Exception as e:
        app.logger.exception(e)
        state['status'] = "DOWN"
        state['status-class'] = "status-alert"

    return state


def load_as_json(filename):
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir,filename)
    with open(file_path, 'r') as fi:
        return json.load(fi)


@app.route('/')
def status():
    servers = load_as_json('servers.json')
    services = load_as_json('services.json')

    for job in services.get('jobs', []):
        server = servers[services['jobs'][job]['server']]
        #app.logger.info(json.dumps(server, indent=4))
        services['jobs'][job]['state'] = get_h3_status(services['jobs'][job]['name'], server)
        services['jobs'][job]['url'] = server['url']


    for queue in services.get('queues', []):
        server = servers[services['queues'][queue]['server']]
        services['queues'][queue]['prefix'] = server['user_prefix']
        services['queues'][queue]['state'] = get_queue_status(services['queues'][queue]['name'], server)

    for http in services.get('http', []):
        services['http'][http]['state'] = get_http_status(services['http'][http]['url'])

    for hdfs in services.get('hdfs', []):
        services['hdfs'][hdfs]['state'] = get_hdfs_status(services['hdfs'][hdfs])

    # Log collected data:
    #app.logger.info(json.dumps(services, indent=4))

    # And render
    return render_template('dashboard.html', title="Status", services=services)


@app.route('/control/dc/pause')
def pause_dc():
    servers = load_as_json('servers.json')
    services = load_as_json('services.json')
    for job in ['dc0-2016', 'dc1-2016', 'dc2-2016', 'dc3-2016']:
        server = servers[services['jobs'][job]['server']]
        h = hapyx.HapyX(server['url'], username=server['user'], password=server['pass'])
        h.pause_job(services['jobs'][job]['name'])
    return redirect(url_for('status'))


@app.route('/control/dc/unpause')
def unpause_dc():
    servers = load_as_json('servers.json')
    services = load_as_json('services.json')
    for job in ['dc0-2016', 'dc1-2016', 'dc2-2016', 'dc3-2016']:
        server = servers[services['jobs'][job]['server']]
        h = hapyx.HapyX(server['url'], username=server['user'], password=server['pass'])
        h.unpause_job(services['jobs'][job]['name'])
    return redirect(url_for('status'))


@app.route('/stop/<frequency>')
def stop(frequency=None):
    if frequency:
        crawl.tasks.stop_start_job(frequency,restart=False)
    return redirect(url_for('status'))


if __name__ == "__main__":
    app.run(debug=True)