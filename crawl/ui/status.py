from __future__ import absolute_import

import json
import crawl.tasks
from crawl.celery import cfg
from crawl.h3 import hapyx
from flask import Flask
from flask import render_template
app = Flask(__name__)

@app.route('/')
def status():
    # Set up connection to H3:
    h = hapyx.HapyX("https://%s:%s" % (cfg.get('h3', 'host'), cfg.get('h3', 'port')),
                    username=cfg.get('h3', 'username'), password=cfg.get('h3', 'password'))

    # Stop job if currently running:
    statuses = {}
    err = None
    try:
        for frequency in h.list_jobs():
            statuses[frequency] = h.status(frequency)
            app.logger.info(statuses[frequency])
            info = h.get_job_info(frequency)
            app.logger.info(json.dumps(h.get_job_info(frequency), indent=4))
            app.logger.info(info['job']['rateReport']['currentDocsPerSecond'])
    except Exception as e:
        err = "Could not reach Heritrix"
        app.logger.error("Could not reach Heritrix!")
        app.logger.exception(e)

    return render_template('status.html', statuses=statuses, error=err)


@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name)


@app.route('/stop/<frequency>')
def stop(frequency=None):
    if frequency:
        crawl.tasks.stop_start_job(frequency,restart=False)