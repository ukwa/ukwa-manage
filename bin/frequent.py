#!/usr/bin/env python

import os
import act
import sys
import glob
import json
import pika
import time
import shutil
import httplib
import rfc3987
import urllib2
import logging
import argparse
import heritrix
import operator
import requests
import StringIO
import subprocess32
import dateutil.parser
import timeout_decorator
from lxml import etree
from settings_w3act import *
from warcindexer import *
from retry_decorator import *
from datetime import datetime
from collections import Counter, OrderedDict
from hanzo.warctools import WarcRecord
from xml.etree.ElementTree import ParseError
from requests.exceptions import ConnectionError

requests.packages.urllib3.disable_warnings()

parser = argparse.ArgumentParser(description="Extract ARK-WARC lookup from METS.")
parser.add_argument("-t", dest="timestamp", type=str, required=False, help="Timestamp")
parser.add_argument("-f", dest="frequency", type=str, required=False, help="Frequency")
args = parser.parse_args()

frequencies = ["daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual"]
heritrix_ports = { "daily": "8444", "weekly": "8445", "monthly": "8446", "quarterly": "8443", "sixmonthly": "8443", "annual": "8443" }
clamd_ports = { "daily": "3311", "weekly": "3312", "monthly": "3313", "quarterly": "3310", "sixmonthly": "3310", "annual": "3310" }
max_render_depth = { "daily": 0, "weekly": 1, "monthly": 1, "quarterly": 1, "sixmonthly": 1, "annual": 1 }

if args.frequency:
    frequencies = [args.frequency]

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.WARNING)
logger = logging.getLogger("frequency")

pika_logger = logging.getLogger("pika")
pika_logger.setLevel(logging.WARNING)

global api
global now

class Seed:
    def tosurt(self, url):
        parsed = rfc3987.parse(url, rule="URI")
        authority = parsed["authority"].split(".")
        authority.reverse()
        return "http://(" + ",".join(authority) + ","

    def __init__(self, url, depth="capped", scope="root", ignore_robots=False):
        self.url = url
        self.scope = scope
        self.depth = depth.lower() #capped=default, capped_large=higherLimit, deep=noLimit
        self.surt = self.tosurt(self.url)
        self.ignore_robots = ignore_robots

def verify_api():
    try:
        api.rescan()
    except ConnectionError:
        logger.error("Can't connect to Heritrix: ConnectionError")
        sys.exit(1)

def jobs_by_status(status):
    jobs = [] 
    for job in api.listjobs(): 
        if api.status(job) == status:
            jobs.append((job, api.launchid(job)))
    return jobs

def waitfor(job, status):
    try:
        while api.status(job) not in status:
            time.sleep(10)
    except ParseError, e:
        logger.error(str(e))
        time.sleep(10)

def killRunningJob(newjob):
    for job in api.listjobs():
        if job.startswith(newjob) and api.status(job) != "":
            logger.info("Killing alread-running job: " + job)
            api.pause(job)
            waitfor(job, "PAUSED")
            api.terminate(job)
            waitfor(job, "FINISHED")
            api.teardown(job)
            waitfor(job, "")

def setupjobdir(newjob):
    root = HERITRIX_JOBS + "/" + newjob
    if not os.path.isdir(root):
        logger.info("Adding %s to Heritrix" % newjob)
        os.mkdir(root)
        api.add(HERITRIX_JOBS + "/" + newjob)
    return root

def addSurtAssociations(seeds, job):
    script = []
    for seed in seeds:
        if seed.depth == "capped_large":
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"higherLimit\");" % seed.surt)
        if seed.depth == "deep":
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"noLimit\");" % seed.surt)
        if seed.depth != "capped":
            logger.info("Amending cap for SURT " + seed.surt + " to " + seed.depth)
    return script


def addBlockingRules():
    script = []
    try:
        act_export = callAct(URL_ROOT + "nevercrawl")
    except urllib2.URLError, e:
        logger.error("Cannot read ACT! " + str(e) + " [" + frequency + "]")
    except httplib.IncompleteRead, i:
        logger.error("IncompleteRead: " + str(i.partial) + " [" + frequency + "]")
    for node in act_export:
        for url in [str(u["url"]) for u in node["fieldUrls"]]:
            try:
                seed = Seed(url)
                script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"blockAll\");" % seed.surt)
            except ValueError, v:
                logger.warning("ValueError: %s" % str(v))
    return script


def addScopingRules(seeds, job):
    script = []
    for seed in seeds:
        if seed.scope == "resource":
            script.append("appCtx.getBean(\"sheetOverlaysManager\").getOrCreateSheet(\"resourceScope\"); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").putSheetOverlay(\"resourceScope\", \"hopsCountReject.maxHops\", 1); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"resourceScope\"); " % seed.surt)
        if seed.scope == "plus1":
            script.append("appCtx.getBean(\"sheetOverlaysManager\").getOrCreateSheet(\"plus1Scope\"); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").putSheetOverlay(\"plus1Scope\", \"hopsCountReject.maxHops\", 1); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").putSheetOverlay(\"plus1Scope\", \"redirectAccept.enabled\", true); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"plus1Scope\"); " % seed.surt)
        if seed.scope == "subdomains":
            script.append("appCtx.getBean(\"sheetOverlaysManager\").getOrCreateSheet(\"subdomainsScope\"); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").putSheetOverlay(\"subdomainsScope\", \"onDomainAccept.enabled\", true); ")
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"subdomainsScope\"); " % seed.surt)
        if seed.scope != "root":
            logger.info("Setting scope for SURT %s to %s." % (seed.surt, seed.scope))
    return script

def writeJobScript(job, script):
    with open("%s/%s/script" % (HERITRIX_JOBS, job), "wb") as o:
        o.writelines("\n".join(script))

def runJobScript(job):
    with open("%s/%s/script" % (HERITRIX_JOBS, job), "rb") as i:
        script = i.read()
        api.execute(engine="beanshell", script=script, job=job)

def ignoreRobots(seeds, job):
    script = []
    for seed in seeds:
        if seed.ignore_robots:
            script.append("appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"ignoreRobots\");" % seed.surt)
            logger.info("Ignoring robots.txt for SURT " + seed.surt)
    return script

def submitjob(newjob, seeds):
    verify_api()
    killRunningJob(newjob)
    root = setupjobdir(newjob)

    #Set up profile, etc.
    frequency = newjob.split("-")[0]
    profile = HERITRIX_PROFILES + "/profile-frequent.cxml"
    if not os.path.exists(profile):
        logger.error("Cannot find profile for " + frequency + " [" + profile + "]")
        return
    tree = etree.parse(profile)
    tree.xinclude()
    xmlstring = etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding="UTF-8")
    #Replace values
    xmlstring = xmlstring.replace("REPLACE_JOB_NAME", newjob)
    xmlstring = xmlstring.replace("REPLACE_CLAMD_PORT", clamd_ports[frequency])
    xmlstring = xmlstring.replace("REPLACE_JOB_ROOT", newjob)
    xmlstring = xmlstring.replace("REPLACE_HERITRIX_JOBS", HERITRIX_JOBS)
    cxml = open(root + "/crawler-beans.cxml", "w")
    cxml.write(xmlstring)
    cxml.close()
    #Copy files
    shutil.copy(HERITRIX_SHORTENERS, root)
    shutil.copy(HERITRIX_EXCLUDE, root)
    shutil.copy(HERITRIX_SURTS, root)
    #Write seeds
    seedstxt = open(root + "/seeds.txt", "w")
    for seed in seeds:
        seedstxt.write(seed.url + "\n")
    seedstxt.close()
    #Start the new job
    logger.info("Building %s", newjob)
    api.build(newjob)
    waitfor(newjob, "NASCENT")
    logger.info("Launching %s", newjob)
    api.launch(newjob)
    waitfor(newjob, "PAUSED")
    #Add SURT associations for caps.
    script = addSurtAssociations(seeds, newjob)
    script += addScopingRules(seeds, newjob)
    script += ignoreRobots(seeds, newjob)
    script += addBlockingRules()
    if len(script) > 0:
        writeJobScript(newjob, script)
        runJobScript(newjob)
    logger.info("Unpausing %s", newjob)
    api.unpause(newjob)
    waitfor(newjob, "RUNNING")
    logger.info("%s running. Exiting.", newjob)

@retry((ConnectionError, ValueError, IndexError), tries=5, timeout_secs=600)
def callAct(url):
    response = requests.post(URL_LOGIN, data={"email": EMAIL, "password": PASSWORD})
    cookie = response.history[0].headers["set-cookie"]
    headers = {
        "Cookie": cookie
    }
    r = requests.get(url, headers=headers)
    j = json.loads(r.content)
    return j

def check_frequencies():
    jobs_to_start = []
    for frequency in frequencies:
        SEED_FILE = "/heritrix/" + frequency + "-seeds.txt"
        seeds = []
        global now
        now = datetime.now()
        if args.timestamp is not None:
            now = dateutil.parser.parse(args.timestamp).replace(tzinfo=None)

        try:
            act_export = callAct(URL_ROOT + frequency)
        except urllib2.URLError, e:
            logger.error("Cannot read ACT! " + str(e) + " [" + frequency + "]")
            continue
        except httplib.IncompleteRead, i:
            logger.error("IncompleteRead: " + str(i.partial) + " [" + frequency + "]")
            continue

        def add_seeds(urls, depth, scope, ignore_robots):
            for url in urls:
                try:
                    seed = Seed(url=url, depth=depth, scope=scope, ignore_robots=ignore_robots)
                    seeds.append(seed)
                except ValueError, v:
                    logger.error("INVALID URL: " + url)

        #crawlDateRange will be:
        #    blank            Determined by frequency.
        #    "start_date"        Determined by frequency if start_date < now.
        #    "start_date end_date"    Determined by frequency if start_date < now and end_date > now
        for node in act_export:
            if "watched" in node.keys() and not node["watched"]:
                continue
            start_date = node["crawlStartDateText"]
            end_date = node["crawlEndDateText"]
            depth = node["field_depth"]
            scope = node["field_scope"]
            ignore_robots = node["field_ignore_robots_txt"]

            # If there's no end-date or it's in the future, we're okay.
            if end_date is None or dateutil.parser.parse(end_date, dayfirst=True) > now:
                if start_date is None:
                    start_date = datetime.fromtimestamp(0)
                else:
                    start_date = dateutil.parser.parse(start_date, dayfirst=True)
                hotd = start_date.hour
                dotw = start_date.weekday()
                dotm = start_date.day
                moty = start_date.month

                urls = [u["url"] for u in node["fieldUrls"]]

                if start_date <= now:
                    # All frequencies are hour-dependent.
                    if hotd == now.hour:
                        if frequency == "daily": 
                            add_seeds(urls, depth, scope, ignore_robots)
                        if frequency == "weekly" and dotw == now.weekday():
                            add_seeds(urls, depth, scope, ignore_robots)
                        # Remaining frequencies all run on a specific day.
                        if dotm == now.day:
                            if frequency == "monthly":
                                add_seeds(urls, depth, scope, ignore_robots)
                            if frequency == "quarterly" and moty%3 == now.month%3:
                                add_seeds(urls, depth, scope, ignore_robots)
                            if frequency == "sixmonthly" and moty%6 == now.month%6:
                                add_seeds(urls, depth, scope, ignore_robots)
                            if frequency == "annual" and moty == now.month:
                                add_seeds(urls, depth, scope, ignore_robots)
        if len(seeds) > 0:
            if frequency == "daily": 
                jobname = frequency + "-" + now.strftime("%H") + "00"
            if frequency == "weekly":
                jobname = frequency + "-" + now.strftime("%a%H").lower() + "00"
            if frequency == "monthly":
                jobname = frequency + "-" + now.strftime("%d%H") + "00"
            if frequency == "quarterly":
                jobname = frequency + "-" + str(now.month%3) + "-" + now.strftime("%d%H") + "00"
            if frequency == "sixmonthly":
                jobname = frequency + "-" + str(now.month%6) + "-" + now.strftime("%d%H") + "00"
            if frequency == "annual":
                jobname = frequency + "-" + now.strftime("%m%d%H") + "00"
            global api
            api = heritrix.API(host="https://crawler03.wa.bl.uk:" + heritrix_ports[frequency] + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False)
            jobs_to_start.append((jobname, seeds))
            if (jobname in api.listjobs()) and api.status(jobname) != "":
                logger.info("Emptying Frontier for %s" % jobname)
                api.pause(jobname)
                waitfor(jobname, "PAUSED")
                api.empty_frontier(jobname)
                api.unpause(jobname)
                waitfor(jobname, "EMPTY")
#            submitjob(jobname, seeds)
    return jobs_to_start

def generate_wayback_indexes(job, launchid):
    logger.info("Generating CDX for %s" % job)
    warcs = glob.glob(WARC_ROOT + "/" + job + "/" + launchid + "/*.warc.gz*")
    logger.info("Indexing " + str(len(warcs)) + " WARCs.")
    index_warcs(warcs, QA_CDX, base_cdx=CDX)
    logger.info("Generating resource index for %s" % job)
    generate_path_index(warcs, PATH_INDEX)

def phantomjs_render(urls_to_render):
    logger.info("Rendering " + str(len(urls_to_render)) + " URLs.")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output = ""
    #TODO: Wayback should be running in proxy mode.
    for url in urls_to_render:
        try:
            output += subprocess32.check_output([PHANTOMJS, NETSNIFF, WAYBACK + timestamp + "/" + url], timeout=20)
        except (subprocess32.CalledProcessError, subprocess32.TimeoutExpired):
            pass
    return output


@timeout_decorator.timeout(TIMEOUT)
def checkCompleteness(job, launchid):
    logger.info("Checking job %s" % job)
    frequency = job.split("-")[0]
    depth = max_render_depth[frequency]
    generate_wayback_indexes(job, launchid)
    crawl_logs = glob.glob(LOG_ROOT + "/" + job + "/" + launchid + "/crawl.log*")
    urls_to_render = []
    for crawl_log in crawl_logs:
        with open(crawl_log, "rb") as l:
            for line in l:
                url = line.split()[3]
                discovery_path = line.split()[4]
                if url.startswith("http") and len(discovery_path.replace("R", "")) <= depth:
                    urls_to_render.append(url)
    open(WAYBACK_LOG, "wb").close()
    phantomjs_render(urls_to_render)
    force = []
    with open(WAYBACK_LOG, "rb") as l:
        for line in l:
            if "F+" in line:
                force.append(re.sub("^.*INFO: ", "", line))
    if len(force) > 0:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        #Write the file 'one level up' to avoid conflicts with Heritrix reads.
        action = open(JOB_ROOT + job + "/" + timestamp + ".force", "wb")
        seen = []
        for line in force:
            url = line.split()[1]
            if url not in seen:
                action.write(line + "\n")
                seen.append(url)
        action.close()
        shutil.move(JOB_ROOT + job + "/" + timestamp + ".force", JOB_ROOT + job + "/action/" + timestamp + ".force")
        logger.info("Found " + str(len(seen)) + " distinct new URLs.")
        # Pause briefly to allow file to be picked up.
        time.sleep(35)
        waitfor(job, ["RUNNING", "EMPTY"])
        waitfor(job, "EMPTY")

def humanReadable(bytes, precision=1):
    abbrevs = (
        (1<<50L, "PB"),
        (1<<40L, "TB"),
        (1<<30L, "GB"),
        (1<<20L, "MB"),
        (1<<10L, "kB"),
        (1, "bytes")
    )
    if bytes == 1:
        return "1 byte"
    for factor, suffix in abbrevs:
        if bytes >= factor:
            break
    return "%.*f %s" % (precision, bytes / factor, suffix)

def log_stats(logs):
    response_codes = []
    data_size = 0
    host_regex = re.compile("https?://([^/]+)/.*$")
    all_hosts_data = {}
    for log in logs:
        with open(log, "rb") as l:
            for line in l:
                # Annotations can contain whitespace so limit the split.
                fields = line.split(None, 15)
                match = host_regex.match(fields[3])
                if match is not None:
                    host = match.group(1)
                    try:
                        host_data = all_hosts_data[host]
                    except KeyError:
                        all_hosts_data[host] = { "data_size": 0, "response_codes": [] }
                        host_data = all_hosts_data[host]
                    host_data["response_codes"].append(fields[1])
                    if fields[2] != "-":
                        host_data["data_size"] += int(fields[2])
                    if "serverMaxSuccessKb" in fields[-1]:
                        host_data["reached_cap"] = True
    all_hosts_data = OrderedDict(sorted(all_hosts_data.iteritems(), key=operator.itemgetter(1), reverse=True))
    for host, data in all_hosts_data.iteritems():
        data["response_codes"] = Counter(data["response_codes"])
        data["data_size"] = humanReadable(data["data_size"])
    return all_hosts_data

def check_act(job, launchid):
    global api
    frequency = job.split("-")[0]
    try:
        act_export = callAct(URL_ROOT + frequency)
    except urllib2.URLError, e:
        logger.error("Cannot read ACT! " + str(e) + " [" + job + "/" + launchid + "]")
        return
    except httplib.IncompleteRead, i:
        logger.error("IncompleteRead: " + job + "/" + launchid)
        return
    # Find WCT IDs and corresponding URLs
    wct_data = []
    seeds = api.seeds(job)
    for node in act_export:
        wct_id = node["field_wct_id"]
        urls = [str(u["url"]) for u in node["fieldUrls"]]
        aid = str(node["id"])
        for url in urls:
            if url in seeds:
                wct_data.append((wct_id, aid, urls))
                break
    # Generate stats. from the logs
    crawl_logs = glob.glob(LOG_ROOT + "/" + job + "/" + launchid + "/crawl.log*")
    stats = log_stats(crawl_logs)
    return (wct_data, stats)

@retry(urllib2.URLError, tries=20, timeout_secs=10)
def act_note_reached_cap(act_id, jobname, timestamp):
    logger.debug("act_note_reached_cap(): %s" % act_id)
    a = act.ACT()
    node = a.request_node(act_id)
    if node is not None and len(node["field_notes"]) == 0:
        value = ""
    else:
        value = node["field_notes"]["value"]
    value += "Reached the cap in %s/%s.\n" % (jobname, timestamp)
    update = {}
    field_notes = {}
    field_notes["value"] = value
    update["field_notes"] = field_notes
    a.send_data(act_id, json.dumps(update))

def add_act_instance(target, timestamp, data, wct_data, jobname):
    wct_id, act_id, urls = wct_data
    a_act = act.ACT()
    content = {}
    content["value"] = "WCT ID: %s\nJob ID: %s\nSeeds: %s\nWayback URLs:\n" % (wct_id, jobname, urls)
    for url in urls:
        content["value"] += "http://crawler03.wa.bl.uk:8080/wayback/%s/%s\n" % (timestamp, url)
    content["value"] += "<pre>%s</pre>" % data
    content["format"] = "filtered_html"
    # Need an OrderedDict: 'type' must be the first field.
    instance = OrderedDict([("type", "instance"), ("body", content), ("field_timestamp", timestamp), ("field_target", target)])
    update = json.dumps(instance, sort_keys=False)
    r = a_act.send_data("", update)
    return r

def send_sip_message(jobname, timestamp):
    message = "%s/%s" % (jobname, timestamp)
    connection = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=SIP_QUEUE_NAME, durable=True)
    channel.basic_publish(exchange="",
        routing_key=SIP_QUEUE_KEY,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
        body=message)
    connection.close()

def remove_action_files(jobname):
    root = "%s/%s/latest/actions-done" % (HERITRIX_JOBS, jobname)
    if os.path.exists(root):
        for action in glob.glob("%s/*" % root):
            logger.info("Removing %s" % action)
            os.remove(action)

if __name__ == "__main__":
    # Check for scheduled jobs.
    jobs_to_start = check_frequencies()
    # Check for EMPTY jobs and render for completeness.
    for port in set(heritrix_ports.values()):
        api = heritrix.API(host="https://crawler03.wa.bl.uk:" + port + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False)
        for emptyJob, launchid in jobs_by_status("EMPTY"):
            if emptyJob.startswith("latest"):
                # Don't render 'latest' job.
                continue
            logger.info(emptyJob + " is EMPTY; verifying.")
            try:
                checkCompleteness(emptyJob, launchid)
                logger.info(emptyJob + " checked; terminating.")
            except timeout_decorator.timeout_decorator.TimeoutError:
                logger.warning("Completeness check timeout; terminating anyway.")
            remove_action_files(emptyJob)
            api.terminate(emptyJob)
            waitfor(emptyJob, "FINISHED")
            api.teardown(emptyJob)
            waitfor(emptyJob, "")
            # Check for UKWA Instances.
            wct_data, stats = check_act(emptyJob, launchid)
            js = json.dumps(stats, indent=8, separators=(",", ":"))
            for datum in wct_data:
                wct_id, act_id, urls = datum
                # Create an Instance if we have a WCT ID.
                if wct_id is not None:
                    logger.info("WCT ID: %s" % wct_id)
                    logger.info("ACT ID: %s" % act_id)
                    logger.info("Timestamp: %s" % launchid)
                    logger.info("\tURLs: %s" % urls)
                    # Only add 'daily' Instances once a week.
                    if emptyJob.startswith("daily") and not now.strftime("%A") == "Monday":
                        continue
                    logger.info("Adding Instance: %s" % act_id)
#TODO: Can't be done yet.
#                    add_act_instance(act_id, launchid, js, datum, emptyJob)
            # Update ACT notes if any have reached their cap.
#            for key, item in stats.iteritems():
#                if item.has_key("reached_cap"):
#                    act_note_reached_cap(act_id, emptyJob, launchid)
            logger.info(js)
            # Send a message to the queue for SIP'ing.
            send_sip_message(emptyJob, launchid)
    # Now start up those jobs we forcibly EMPTY'd earlier.
    for job in jobs_to_start:
        name, seeds = job
        freq = name.split("-")[0]
        port = heritrix_ports[freq]
        api = heritrix.API(host="https://crawler03.wa.bl.uk:" + port + "/engine", user="admin", passwd="bl_uk", verbose=False, verify=False)
        submitjob(name, seeds)

