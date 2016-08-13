from __future__ import absolute_import

import os
import json
import requests
import crawl.tasks
from crawl.h3 import hapyx
from flask import Flask
from flask import render_template
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
        app.logger.exception(e)
    # Classify
    if state['status'] == "DOWN":
        state['status-class'] = "status-alert"
    elif state['status'] == "RUNNING":
        # Replacing RUNNING with docs/second rate
        rate = state['details']['job']['rateReport']['currentDocsPerSecond']
        state['status'] = "%i URI/s" % rate
        if rate == "0":
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
        app.logger.info("GET: %s" % qurl)
        r = requests.get(qurl)
        state['details'] = r.json()
        if 'error' in state['details']:
            state['status-class'] = "status-alert"
        else:
            state['status-class'] = "status-warning"
    except Exception as e:
        state['status-class'] = "status-alert"
        app.logger.exception(e)

    return state

def load_as_json(filename):
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir,filename)
    with open(file_path, 'r') as fi:
        return json.load(fi)

@app.route('/')
def status():
    servers = load_as_json('servers.json')
    app.logger.info(json.dumps(servers, indent=4))

    services = load_as_json('services.json')

    for job in services.get('jobs', []):
        server = servers[services['jobs'][job]['server']]
        app.logger.info(json.dumps(server, indent=4))
        services['jobs'][job]['state'] = get_h3_status(job, server)

    for queue in services.get('queues', []):
        services['queues'][queue]['state'] = get_queue_status(services['queues'][queue]['name'], servers[services['queues'][queue]['server']])

    # Log collected data:
    app.logger.info(json.dumps(services, indent=4))

    # And render
    return render_template('dashboard.html', title="Status", services=services)

@app.route('/stop/<frequency>')
def stop(frequency=None):
    if frequency:
        crawl.tasks.stop_start_job(frequency,restart=False)