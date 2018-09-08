import os
import json
import time
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from flask import Flask
from flask import render_template, redirect, url_for, flash, jsonify, request, abort, send_file
from werkzeug.contrib.cache import FileSystemCache
from lib.heritrix3.collector import Heritrix3Collector
from dash.kafka_client import Consumer
from dash.screenshots import lookup_in_cdx, get_rendered_original_stream

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET', 'dev-mode-key')
app.config['CACHE_FOLDER'] = os.environ.get('CACHE_FOLDER', '__cache__')
cache = FileSystemCache(os.path.join(app.config['CACHE_FOLDER'], 'request_cache'))

consumer = Consumer('uris.crawled.fc', ['localhost:9092'], 'dashboard')
consumer.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/activity/json')
def get_recent_activity():
    metrics = consumer.get_stats()
    return jsonify(metrics)

@app.route('/get-rendered-original')
def get_rendered_original():
    """
    Grabs a rendered resource.

    Only reason Wayback can't do this is that it does not like the extended URIs
    i.e. 'screenshot:http://' and replaces them with 'http://screenshot:http://'
    """
    url = request.args.get('url')
    app.logger.debug("Got URL: %s" % url)
    #
    type = request.args.get('type', 'screenshot')
    app.logger.debug("Got type: %s" % type)

    # Query URL
    qurl = "%s:%s" % (type, url)
    # Query CDX Server for the item
    (warc_filename, warc_offset, compressed_end_offset) = lookup_in_cdx(qurl)

    # If not found, say so:
    if warc_filename is None:
        abort(404)

    # Grab the payload from the WARC and return it.
    stream, content_type = get_rendered_original_stream(warc_filename,warc_offset, compressed_end_offset)

    return send_file(stream, mimetype=content_type)

@app.route('/control')
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
