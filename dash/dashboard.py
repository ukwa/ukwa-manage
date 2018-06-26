import os
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from flask import Flask
from flask import render_template, redirect, url_for, flash
from lib.heritrix3.collector import Heritrix3Collector
from hapy import hapy

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET', 'dev-mode-key')


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
        services = c.lookup_services()

        for s in services:
            server_user = os.getenv('HERITRIX_USERNAME', "heritrix")
            server_pass = os.getenv('HERITRIX_PASSWORD', "test")
            h = hapy.Hapy(s['url'], username=server_user, password=server_pass)
            if action == 'pause':
                app.logger.info("Requesting pause of job %s on server %s." % (s['job_name'], s['url']))
                h.pause_job(s['job_name'])
                flash("Requested pause of job %s on server %s." % (s['job_name'], s['url']))
            elif action == 'unpause':
                app.logger.info("Requesting unpause of job %s on server %s." % (s['job_name'], s['url']))
                h.unpause_job(s['job_name'])
                flash("Requested unpause of job %s on server %s." % (s['job_name'], s['url']))
            elif action == 'launch':
                app.logger.info("Requesting launch of job %s on server %s." % (s['job_name'], s['url']))
                h.launch_job(s['job_name'])
                flash("Requested launch of job %s on server %s." % (s['job_name'], s['url']))
            elif action == 'terminate':
                app.logger.info("Requesting termination of job %s on server %s." % (s['job_name'], s['url']))
                h.terminate_job(s['job_name'])
                flash("Requested termination of job %s on server %s." % (s['job_name'], s['url']))
            else:
                app.logger.warning("Unrecognised crawler action! '%s'" % action)
                flash("Unrecognised crawler action! '%s'" % action)

    except Exception as e:
        flash("Something went wrong!\n%s" % e)

    return redirect(url_for('status'))


if __name__ == "__main__":
    app.run(debug=True, port=5505)
