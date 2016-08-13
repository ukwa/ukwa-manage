from __future__ import absolute_import

import os
import json
import requests
import crawl.tasks
from crawl.h3 import hapyx
from flask import Flask
from flask import render_template
app = Flask(__name__)

def get_h3_status(h3):
    # Set up connection to H3:
    h = hapyx.HapyX(h3['url'], username=h3['user'], password=h3['pass'])

    h3['states'] = {}
    h3['statuses'] = {}
    h3['errors'] = {}
    h3['details'] = {}
    for frequency in h3['jobs']:
        try:
            info = h.get_job_info(frequency)
            h3['details'][frequency] = info
            if info.has_key('job'):
                state = info['job'].get("crawlControllerState", None)
                if not state:
                    state = info['job'].get("statusDescription", None)
                h3['states'][frequency] = state.upper()
                app.logger.info("TEST %s" % h3['states'][frequency])
                app.logger.info(json.dumps(info, indent=4))
                rates = info.get('job',{}).get('rateReport',{})
                if rates:
                    app.logger.info(rates.get('currentDocsPerSecond',""))
        except Exception as e:
            h3['states'][frequency] = "DOWN"
            h3['errors'][frequency] = "Could not reach Heritrix! %s" % e
        # Classify
        if h3['states'][frequency] == "DOWN":
            h3['status'] = "status-alert"
        elif h3['states'][frequency] == "RUNNING":
            # Replacing RUNNING with docs/second rate
            rate = h3['details'][frequency]['job']['rateReport']['currentDocsPerSecond']
            h3['status'][frequency] = "%i URI/s" % rate
            if rate == "0":
                h3['status'] = "status-warning"
            else:
                h3['status'] = "status-good"
        else:
            h3['status'] = "status-warning"

    h3['pass'] = ''

def get_queue_status(queue):
    queue['status'] = {}
    queue['states'] = {}
    for q in queue['queues']:
        try:
            r = requests.get('%s%s' %( queue['prefix'], q))
            queue['states'][q] = r.json()
            if 'error' in queue['states'][q]:
                queue['status'][q] = "status-alert"
            else:
                queue['status'][q] = "status-warning"
        except Exception as e:
            queue['status'][q] = "status-alert"
    app.logger.info(json.dumps(queue, indent=4))

@app.route('/')
def status():
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, 'monitoring.config.json')
    with open(file_path, 'r') as fi:
        mon = json.load(fi)

    print(mon)

    for group in mon:
        print(group)
        jobs = 0
        for crawler in group.get('crawlers', []):
            jobs = jobs + len(crawler['jobs'])
            get_h3_status(crawler)
        group['total-jobs'] = jobs
        for queue in group.get('queues', []):
            get_queue_status(queue)

    # And render
    return render_template('status.html', title="Status", mon=mon)


@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)


@app.route('/stop/<frequency>')
def stop(frequency=None):
    if frequency:
        crawl.tasks.stop_start_job(frequency,restart=False)