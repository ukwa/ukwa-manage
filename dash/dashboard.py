import os
import json
import time
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from flask import Flask
from flask import render_template, redirect, url_for, flash, jsonify
from werkzeug.contrib.cache import FileSystemCache
from lib.heritrix3.collector import Heritrix3Collector

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET', 'dev-mode-key')
cache = FileSystemCache('/__cache__')


@app.route('/')
def status():

    c = Heritrix3Collector()
    s = c.run_api_requests()

    # Log collected data:
    #app.logger.info(json.dumps(s, indent=4))

    # And render
    return render_template('dashboard.html', title="Status", crawls=s)


@app.route('/metrics')
def prometheus_metrics():
    # Set content type for Prometheus metrics:
    headers = {'Content-Type': CONTENT_TYPE_LATEST}
    return generate_latest(Heritrix3Collector()), 200, headers


@app.route('/control/all/<action>')
def control_all(action=None):
    try:
        c = Heritrix3Collector()
        services = c.do(action)
        # Cache the result:
        cache_set = False
        while not cache_set:
            cache_set = cache.set(action, services, timeout=300)

    except Exception as e:
        flash("Something went wrong!\n%s" % e.message)
        return redirect(url_for('status'))

    return redirect(url_for('result_all', action=action))


@app.route('/result/all/<action>')
def result_all(action=None):
    try:
        services = cache.get(action)
        app.logger.info(json.dumps(services, indent=2))
        return jsonify(services)

    except Exception as e:
        flash("Something went wrong!\n%s" % e.message)
        return redirect(url_for('status'))


if __name__ == "__main__":
    app.run(debug=True, port=5505)
