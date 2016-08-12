from __future__ import absolute_import

import os
import json
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
    for frequency in h3['jobs']:
        try:
            info = h.get_job_info(frequency)
            if info.has_key('job'):
                h3['states'][frequency] = info['job'].get("statusDescription", None)
                h3['statuses'][frequency] = info['job'].get("crawlControllerState", None)
                app.logger.info("TEST %s" % h3['states'][frequency])
                app.logger.info(json.dumps(info, indent=4))
                rates = info.get('job',{}).get('rateReport',{})
                if rates:
                    app.logger.info(rates.get('currentDocsPerSecond',""))
        except Exception as e:
            h3['errors'][frequency] = "Could not reach Heritrix! %s" % e

    h3['pass'] = ''

@app.route('/')
def status():
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, 'monitoring.config.json')
    with open(file_path, 'r') as fi:
        mon = json.load(fi)

    print(mon)

    for group in mon:
        print(group)
        for crawler in group.get('crawlers', []):
            get_h3_status(crawler)

    # Stop job if currently running:
    statuses = {}
    err = None

    print(mon)

    return render_template('status.html', title="Status", mon=mon)


@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)


@app.route('/stop/<frequency>')
def stop(frequency=None):
    if frequency:
        crawl.tasks.stop_start_job(frequency,restart=False)