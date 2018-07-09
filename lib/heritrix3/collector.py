import os
import re
import math
import json
import time
import socket
import urllib2
from multiprocessing import Pool, Process
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
import logging
from hapy import hapy
from multiprocessing import Pool, Process

# Avoid warnings about certs.
import urllib3
urllib3.disable_warnings()

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

logger = logging.getLogger(__name__)

# Avoid hangs when systems are unreachable:
TIMEOUT = 10
socket.setdefaulttimeout(TIMEOUT)

# Config file:
CRAWL_JOBS_FILE = os.environ.get("CRAWL_JOBS_FILE", 'crawl-jobs.json')


class Heritrix3Collector(object):

    def __init__(self):
        self.pool = Pool(20)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.close()

    def load_as_json(self, filename):
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r') as fi:
            return json.load(fi)

    def lookup_services(self):
        # Load the config file:
        service_list = self.load_as_json(os.path.join(os.path.dirname(__file__), CRAWL_JOBS_FILE))

        # Find the services. If there are any DNS Service Discovery entries, filter them out.
        services = []
        dns_sd = []
        for job in service_list:
            if 'dns_sd_name' in job:
                dns_sd.append(job)
            else:
                services.append(job)

        # For each DNS SD entry, use DNS to discover the service:
        for job in dns_sd:
            # DNS SD under Docker uses this form of naming to discover services:
            dns_name = 'tasks.%s' % job['dns_sd_name']
            #
            # WARNING Under 'alpine' builds this only ever returned 12 or less entries!
            #
            try:
                # Look up service IP addresses via DNS:
                (hostname, alias, ipaddrlist) = socket.gethostbyname_ex(dns_name)
                for ip in ipaddrlist:
                    logger.debug("For %s got (%s,%s,%s)" % (dns_name, hostname, alias, ipaddrlist))
                    # Make a copy of the dict to put the values in:
                    dns_job = dict(job)
                    # Default to using the IP address:
                    dns_host = ip
                    dns_job['id'] = '%s:%s' % (dns_job['id'], ip)
                    # Find the IP-level hostname via reverse lookup:
                    (r_hostname, r_aliaslist, r_ipaddrlist) = socket.gethostbyaddr(ip)
                    # look for a domain alias that matches the expected form:
                    for r_alias in r_aliaslist:
                        if r_alias.startswith(job['dns_sd_name']):
                            # Use this instead of the raw IP:
                            dns_host = r_alias
                            dns_job['id'] = r_alias
                            break
                    # Set the URL:
                    dns_job['url'] = 'https://%s:8443/' % dns_host
                    # Remember:
                    services.append(dns_job)
            except socket.gaierror as e:
                print(e)
                pass

        return services

    def run_api_requests(self):
        # Find the list of Heritrixen to talk to
        services = self.lookup_services()

        # Parallel check for H3 job status:
        argsv = []
        for job in services:
            logger.debug("Looking up %s" % job)
            server_url = job['url']
            server_user = os.getenv('HERITRIX_USERNAME', "admin")
            server_pass = os.getenv('HERITRIX_PASSWORD', "heritrix")
            # app.logger.info(json.dumps(server, indent=4))
            argsv.append((job['id'], job['job_name'], server_url, server_user, server_pass))
        # Wait for all...
        result_list = self.pool.map(get_h3_status, argsv)
        self.pool.terminate()
        self.pool.join()
        # Collect:
        results = {}
        for job, status in result_list:
            results[job] = status

        # Merge the results in:
        for job in services:
            job['state'] = results[job['id']]
            if not job['url']:
                job['state']['status'] = "LOOKUP FAILED"

        # Sort services by ID:
        services = sorted(services, key=lambda k: k['id'])

        return services

    def collect(self):
        for m in self._collect():
            filtered = []
            for s in m.samples:
                name, labels, value = s
                if not isinstance(value, float):
                    logger.warning("This sample is not a float! %s, %s, %s" % (name, labels, value))
                else:
                    filtered.append(s)
                m.samples = filtered
            yield m

    def _collect(self):
        # type: () -> Generator[GaugeMetricFamily]

        m_uri_down = GaugeMetricFamily(
            'heritrix3_crawl_job_uris_downloaded_total',
            'Total URIs downloaded by a Heritrix3 crawl job',
            labels=["jobname", "deployment", "status", "id"]) # No hyphens in label names please!

        m_uri_known = GaugeMetricFamily(
            'heritrix3_crawl_job_uris_known_total',
            'Total URIs discovered by a Heritrix3 crawl job',
            labels=["jobname", "deployment", "status", "id"]) # No hyphens in label names please!

        m_uris = GaugeMetricFamily(
            'heritrix3_crawl_job_uris_total',
            'URI counters from a Heritrix3 crawl job, labeled by kind',
            labels=["jobname", "deployment", "id", "kind"]) # No hyphens in label names please!

        m_bytes = GaugeMetricFamily(
            'heritrix3_crawl_job_bytes_total',
            'Byte counters from a Heritrix3 crawl job, labeled by kind',
            labels=["jobname", "deployment", "id", "kind"]) # No hyphens in label names please!

        m_qs = GaugeMetricFamily(
            'heritrix3_crawl_job_queues_total',
            'Queue counters from a Heritrix3 crawl job, labeled by kind',
            labels=["jobname", "deployment", "id", "kind"]) # No hyphens in label names please!

        m_ts = GaugeMetricFamily(
            'heritrix3_crawl_job_threads_total',
            'Thread counters from a Heritrix3 crawl job, labeled by kind',
            labels=["jobname", "deployment", "id", "kind"]) # No hyphens in label names please!

        result = self.run_api_requests()

        for job in result:
            #print(json.dumps(job))
            # Get hold of the state and flags etc
            name = job['job_name']
            id = job['id']
            deployment = job['deployment']
            state = job['state'] or {}
            status = state['status'] or None

            # Get the URI metrics
            try:
                # URIs:
                ji = state.get('details',{}).get('job',{})
                utr = ji.get('uriTotalsReport',{})
                if utr is None:
                    utr = {}
                docs_total = utr.get('downloadedUriCount', 0.0)
                known_total = utr.get('totalUriCount', 0.0)
                m_uri_down.add_metric([name, deployment, status, id], docs_total)
                m_uri_known.add_metric([name, deployment, status, id], known_total)
                # New-style metrics:
                m_uris.add_metric([name, deployment, id, 'downloaded'], docs_total)
                m_uris.add_metric([name, deployment, id, 'queued'], known_total)
                m_uris.add_metric([name, deployment, id, 'novel'],
                          ji.get('sizeTotalsReport', {}).get('novelCount', 0.0))
                m_uris.add_metric([name, deployment, id, 'deduplicated'],
                          ji.get('sizeTotalsReport', {}).get('dupByHashCount', 0.0))
                m_uris.add_metric([name, deployment, id, 'deepest-queue-depth'],
                          ji.get('loadReport', {}).get('deepestQueueDepth', 0.0))
                m_uris.add_metric([name, deployment, id, 'average-queue-depth'],
                          ji.get('loadReport', {}).get('averageQueueDepth', 0.0))

                # Bytes:
                m_bytes.add_metric([name, deployment, id, 'novel'],
                          ji.get('sizeTotalsReport', {}).get('novel', 0.0))
                m_bytes.add_metric([name, deployment, id, 'deduplicated'],
                          ji.get('sizeTotalsReport', {}).get('dupByHash', 0.0))
                m_bytes.add_metric([name, deployment, id, 'warc-novel-content'],
                          ji.get('sizeTotalsReport', {}).get('warcNovelContentBytes', 0.0))

                # Queues:
                m_qs.add_metric([name, deployment, id, 'total'],
                          ji.get('frontierReport', {}).get('totalQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'in-process'],
                          ji.get('frontierReport', {}).get('inProcessQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'ready'],
                          ji.get('frontierReport', {}).get('readyQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'snoozed'],
                          ji.get('frontierReport', {}).get('snoozedQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'active'],
                          ji.get('frontierReport', {}).get('activeQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'inactive'],
                          ji.get('frontierReport', {}).get('inactiveQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'ineligible'],
                          ji.get('frontierReport', {}).get('ineligibleQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'retired'],
                          ji.get('frontierReport', {}).get('retiredQueues', 0.0))
                m_qs.add_metric([name, deployment, id, 'exhausted'],
                          ji.get('frontierReport', {}).get('exhaustedQueues', 0.0))

                # Threads:
                m_ts.add_metric([name, deployment, id, 'total'],
                          ji.get('loadReport', {}).get('totalThreads', 0.0))
                m_ts.add_metric([name, deployment, id, 'busy'],
                          ji.get('loadReport', {}).get('busyThreads', 0.0))
                m_ts.add_metric([name, deployment, id, 'toe-count'],
                          ji.get('threadReport', {}).get('toeCount', 0.0))
                # Congestion ratio can be literal 'null':
                congestion = ji.get('loadReport', {}).get('congestionRatio', 0.0)
                if congestion is not None:
                    m_ts.add_metric([name, deployment, id, 'congestion-ratio'], congestion)
                # Thread Steps (could be an array or just one entry):
                steps = ji.get('threadReport', {}).get('steps', {})
                if steps is not None:
                    steps = steps.get('value',[])
                    if isinstance(steps, basestring):
                        steps = [steps]
                    for step_value in steps:
                        splut = re.split(' ', step_value, maxsplit=1)
                        if len(splut) == 2:
                            count, step = splut
                            step = "step-%s" % step.lower()
                            m_ts.add_metric([name, deployment, id, step], float(int(count)))
                        else:
                            logger.warning("Could not handle step value: %s" % step_value)
                # Thread Processors (could be an array or just one entry):
                procs = ji.get('threadReport', {}).get('processors', {})
                if procs is not None:
                    procs = procs.get('value',[])
                    if isinstance(procs, basestring):
                        procs = [procs]
                    for proc_value in procs:
                        splut = re.split(' ', proc_value, maxsplit=1)
                        if len(splut) == 2:
                            count, proc = splut
                            proc = "processor-%s" % proc.lower()
                            m_ts.add_metric([name, deployment, id, proc], float(count))
                        else:
                            logger.warning("Could not handle processor value: '%s'" % proc_value)

            except Exception as e:
                logger.exception("Exception while parsing metrics!")
                logger.info("Printing raw JSON in case there's an underlying issue: %s" % json.dumps(job, indent=2))

        # And return the metrics:
        yield m_uri_down
        yield m_uri_known
        yield m_uris
        yield m_bytes
        yield m_qs
        yield m_ts


def dict_values_to_floats(d, k, excluding=list()):
    if d.has_key(k):
        for sk in d[k]:
            if not sk in excluding:
                d[k][sk] = float(d[k][sk])
                if math.isnan(d[k][sk]) or math.isinf(d[k][sk]):
                    d[k][sk] = None


def get_h3_status(args):
    job_id, job_name, server_url, server_user, server_pass = args
    # Set up connection to H3:
    h = hapy.Hapy(server_url, username=server_user, password=server_pass, timeout=TIMEOUT)
    state = {}
    try:
        logger.info("Getting status for job %s on %s" % (job_name, server_url))
        info = h.get_job_info(job_name)
        state['details'] = info
        if info.has_key('job'):
            state['status'] = info['job'].get("crawlControllerState", None)
            if not state['status']:
                state['status'] = info['job'].get("statusDescription", None)
            state['status'] = state['status'].upper()
            if state['status'] != "UNBUILT":
                # Also look to store useful numbers as actual numbers:
                dict_values_to_floats(info['job'], 'loadReport')
                dict_values_to_floats(info['job'], 'heapReport')
                dict_values_to_floats(info['job'], 'rateReport')
                dict_values_to_floats(info['job'], 'threadReport', ['steps','processors'])
                dict_values_to_floats(info['job'], 'sizeTotalsReport')
                dict_values_to_floats(info['job'], 'uriTotalsReport')
                dict_values_to_floats(info['job'], 'frontierReport', ['lastReachedState'])
    except Exception as e:
        state['status'] = "DOWN"
        state['error'] = "Exception while checking Heritrix! %s" % e
        logger.exception(e)

    return job_id, state


if __name__ == "__main__":
    REGISTRY.register(Heritrix3Collector())
    start_http_server(9118)
    while True: time.sleep(1)


# https://localhost:8443/engine/job/frequent/report/KafkaUrlReceiverReport