import io
import json
import datetime
import crawl.tasks
from requests.utils import quote
import xml.dom.minidom
import requests
import zlib
from pywb.warc.archiveiterator import DefaultRecordParser
from pywb.warc.recordloader import ArcWarcRecordLoader
from pywb.utils.bufferedreaders import DecompressingBufferedReader
from crawl.h3 import hapyx
from tasks.monitor import CheckStatus
from tasks.common import systems, h3
from luigi.contrib.hdfs.webhdfs_client import webhdfs
from flask import Flask
from flask import render_template, redirect, url_for, request, Response, send_file, abort
app = Flask(__name__)


@app.route('/')
def status():

    json_file = CheckStatus(date=datetime.datetime.today() - datetime.timedelta(minutes=1)).output().path
    app.logger.info("Attempting to load %s" % json_file)
    with open(json_file,'r') as reader:
        services = json.load(reader)

    # Log collected data:
    #app.logger.info(json.dumps(services, indent=4))

    # And render
    return render_template('dashboard.html', title="Status", services=services)


@app.route('/ping')
def ping_pong():
    return 'pong!'


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
    (warc_filename, warc_offset) = lookup_in_cdx(qurl)

    # If not found, say so:
    if warc_filename is None:
        abort(404)

    # Grab the payload from the WARC and return it.
    r = requests.get("%s%s%s?op=OPEN&user=%s&offset=%s" % (systems().webhdfs, h3().hdfs_root_folder,
                                                           warc_filename, webhdfs().user, warc_offset))
    r.raw.decode_content = False
    rl = ArcWarcRecordLoader()
    record = rl.parse_record_stream(DecompressingBufferedReader(stream=io.BytesIO(r.content)))
    print(record)
    print(record.length)
    print(record.stream.limit)

    return send_file(record.stream, mimetype=record.content_type)

    #return "Test %s@%s" % (warc_filename, warc_offset)


def lookup_in_cdx(qurl):
    """
    Checks if a resource is in the CDX index.
    :return:
    """
    query = "%s?q=type:urlquery+url:%s" % (systems().cdxserver, quote(qurl))
    r = requests.get(query)
    print(r.url)
    app.logger.debug("Availability response: %d" % r.status_code)
    print(r.status_code, r.text)
    # Is it known, with a matching timestamp?
    if r.status_code == 200:
        try:
            dom = xml.dom.minidom.parseString(r.text)
            for result in dom.getElementsByTagName('result'):
                file = result.getElementsByTagName('file')[0].firstChild.nodeValue
                compressedoffset = result.getElementsByTagName('compressedoffset')[0].firstChild.nodeValue
                return file, compressedoffset
        except Exception as e:
            app.logger.error("Lookup failed for %s!" % qurl)
            app.logger.exception(e)
        #for de in dom.getElementsByTagName('capturedate'):
        #    if de.firstChild.nodeValue == self.ts:
        #        # Excellent, it's been found:
        #        return
    return None, None


@app.route('/control/dc/pause')
def pause_dc():
    servers = json.load(systems().servers)
    services = json.load(systems().services)
    for job in ['dc0-2016', 'dc1-2016', 'dc2-2016', 'dc3-2016']:
        server = servers[services['jobs'][job]['server']]
        h = hapyx.HapyX(server['url'], username=server['user'], password=server['pass'])
        h.pause_job(services['jobs'][job]['name'])
    return redirect(url_for('status'))


@app.route('/control/dc/unpause')
def unpause_dc():
    servers = json.load(systems().servers)
    services = json.load(systems().services)
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
    app.run(debug=True, port=5505)
