#!/usr/bin/env python

"""
Methods for controlling w3act jobs in Heritrix.
"""

import os
import json
import time
import shutil
import requests
from glob import glob
from lxml import etree
from lib.agents.w3act import w3act
from urlparse import urlparse
from crawl.h3 import hapyx
from crawl.w3act.util import unique_list
from crawl.w3act import credentials
from xml.etree.ElementTree import ParseError
from crawl.celery import HERITRIX_JOBS
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

requests.packages.urllib3.disable_warnings()

mandatory_fields = ["field_url", "field_depth", "field_scope", "url"]
depth_sheets = {"capped_large": "higherLimit", "deep": "noLimit"}
scope_sheets = {"resource": "resourceScope", "plus1": "plus1Scope", "subdomains": "subdomainsScope"}

W3ACT_FIELDS=["id", "title", "schedules", "depth", "scope", "ignoreRobotsTxt"]

HERITRIX_CONFIG_ROOT=os.path.realpath(os.path.join(os.path.dirname(__file__),"../profiles"))
HERITRIX_PROFILE="%s/profile-frequent.cxml" % HERITRIX_CONFIG_ROOT
HERITRIX_EXCLUDE="%s/exclude.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SHORTENERS="%s/url.shorteners.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SURTS="%s/surts.txt" % HERITRIX_CONFIG_ROOT

CLAMD_PORTS = { "daily": "3310", "weekly": "3310", "monthly": "3310", "quarterly": "3310", "sixmonthly": "3310", "annual": "3310" }
CLAMD_DEFAULT_PORT = "3310"
CLAMD_HOSTS = { }
CLAMD_DEFAULT_HOST = "clamd"


def to_surt(url):
        parsed = urlparse(url).netloc
        authority = parsed.split(".")
        authority.reverse()
        return "http://(%s," % ",".join(authority)


def get_surt_association_script(surt, sheet):
    """Creates the beanshell script for a SURT->Sheet association."""
    return "appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"%s\" );" % (surt, sheet)


def get_depth_scripts(seeds, depth):
    """Creates a list of beanshell commands for seed/depth."""
    if depth is None or depth.lower() not in depth_sheets.keys():
        return []
    sheet = depth_sheets[depth.lower()]
    script = [get_surt_association_script(to_surt(seed), sheet) for seed in seeds]
    return script


def get_scope_scripts(seeds, scope):
    """Creates a list of beanshell commands for seed/scope."""
    script = []
    if scope in scope_sheets.keys():
        for seed in seeds:
            surt = to_surt(seed)
            sheet = scope_sheets[scope]
            script.append(get_surt_association_script(surt, sheet))
            logger.info("Setting scope for %s to %s" % (surt, sheet))
    return script

def get_relevant_fields(nodes):
    """Retrieves subset of a Target's fields."""
    targets = []
    for node in nodes:
        target_info = { key: node[key] for key in W3ACT_FIELDS }
        target_info["seeds"] = [u for u in node["seeds"]]
        if "watched" in node.keys():
            target_info["watched"] = node["watched"]
        targets.append(target_info)
    return targets

def remove_action_files(jobname):
    """Removes old 'action' files and symlinks."""
    actions_done = "%s/%s/latest/actions-done" % (HERITRIX_JOBS, jobname)
    done = "%s/%s/action/done" % (HERITRIX_JOBS, jobname)
    for root in [actions_done, done]:
        if os.path.exists(root):
            to_remove = glob("%s/*" % root)
            logger.info("Removing %s action files." % len(to_remove))
            for action in to_remove:
                os.remove(action)

def check_watched_targets(jobname, heritrix):
    """If there are any Watched Targets, send a message."""
    timestamp = heritrix.launchid(jobname)
    if not os.path.exists("%s/%s/%s/w3act-info.json" % (HERITRIX_JOBS, jobname, timestamp)):
        return
    with open("%s/%s/%s/w3act-info.json" % (HERITRIX_JOBS, jobname, timestamp), "rb") as i:
        info = i.read()
    for job in json.loads(info):
        if job["watched"]:
            logger.info("Found a Watched Target in %s/%s." % (jobname, timestamp))
#            send_message(
#                settings.QUEUE_HOST,
#                settings.WATCHED_QUEUE_NAME,
#                settings.WATCHED_QUEUE_KEY,
#                "%s/%s" % (jobname, timestamp)
#            )


class W3actJob(object):
    """Represents a Heritrix job for w3act."""

    def __init__(self, w3act, w3act_targets, name, seeds=None, directory=None, heritrix=None, setup=True, use_credentials=False):
        self.w3act = w3act
        self.use_credentials = use_credentials
        self.name = name
        if seeds is None:
            self.seeds = [s for t in w3act_targets for s in t["seeds"]]
        else:
            self.seeds = seeds
        if setup:
            logger.info("Configuring directory for job '%s'." % self.name)
            self.info = get_relevant_fields(w3act_targets)
            self.setup_job_directory()
        else:
            self.info = w3act_targets
            self.job_dir = directory
        if heritrix is not None:
            self.heritrix = heritrix
            self.heritrix.add_job_directory(self.job_dir)

    def get_blocking_scripts(self):
        """Blocks access to w3act's 'nevercrawl' targets."""
        j = self.w3act.get_ld_export("nevercrawl")
        blocked_urls = unique_list([to_surt(u) for t in j for u in t["seeds"]])
        script = [get_surt_association_script(surt, "blockAll") for surt in blocked_urls]
        return script

    @staticmethod
    def from_directory(w, path, heritrix=None):
        """Build a job from an existing directory."""
        logger.debug("Building job from directory: %s" % path)
        name = os.path.basename(path)
        if os.path.exists("%s/latest/w3act-info.json" % path):
            with open("%s/latest/w3act-info.json" % path, "rb") as i:
                info = json.loads(i.read())
        else:
            info = []
        if os.path.exists("%s/latest/seeds.txt" % path):
            with open("%s/latest/seeds.txt" % path, "rb") as i:
                seeds = [l.strip() for l in i if not l.startswith("#") and len(l.strip()) > 0]
        else:
            seeds = None
        job = W3actJob(w, info, name=name, seeds=seeds, directory=path, heritrix=heritrix, setup=False)
        return job


    def setup_heritrix(self, api=None, host=None, port=None, user="admin", passwd="bl_uk"):
        if api is not None:
            self.heritrix = api
        else:
            self.heritrix = hapyx.HapyX(host="https://%s:%s/engine" % (host, port), user=user, passwd=passwd, verbose=False, verify=False)
        self.heritrix.add_job_directory(self.job_dir)


    def create_profile(self):
        """Creates the CXML content for a H3 job."""
        profile = etree.parse(HERITRIX_PROFILE)
        profile.xinclude()
        cxml = etree.tostring(profile, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        cxml = cxml.replace("REPLACE_JOB_NAME", self.name)
        if self.name in CLAMD_HOSTS.keys():
            cxml = cxml.replace("REPLACE_CLAMD_HOST", CLAMD_HOSTS[self.name])
        else:
            cxml = cxml.replace("REPLACE_CLAMD_HOST", CLAMD_DEFAULT_HOST)
        if self.name in CLAMD_PORTS.keys():
            cxml = cxml.replace("REPLACE_CLAMD_PORT", CLAMD_PORTS[self.name])
        else:
            cxml = cxml.replace("REPLACE_CLAMD_PORT", CLAMD_DEFAULT_PORT)
        cxml = cxml.replace("REPLACE_JOB_ROOT", self.name)
        cxml = cxml.replace("REPLACE_HERITRIX_JOBS", HERITRIX_JOBS)
        self.cxml = cxml


    def setup_job_directory(self):
        """Creates the Heritrix job directory."""
        self.job_dir = "%s/%s/" % (HERITRIX_JOBS, self.name)
        if not os.path.isdir(self.job_dir):
            os.makedirs(self.job_dir)

        shutil.copy(HERITRIX_SHORTENERS, self.job_dir)
        shutil.copy(HERITRIX_EXCLUDE, self.job_dir)
        shutil.copy(HERITRIX_SURTS, self.job_dir)

        # Write seeds to disk:
        with open("%s/seeds.txt" % self.job_dir, "wb") as o:
            o.write("\n".join(self.seeds).encode("utf-8"))

        # Write profile to disk:
        self.create_profile()
        with open("%s/crawler-beans.cxml" % self.job_dir, "wb") as o:
            o.write(self.cxml)

        # Write Sheet-associations to disk:
        commands = []
        for target in self.info:
            commands += get_depth_scripts(target["seeds"], target["depth"])
            commands += get_scope_scripts(target["seeds"], target["scope"])
        commands += self.get_blocking_scripts()
        with open("%s/script.beanshell" % self.job_dir, "wb") as o:
            o.write("\n".join(commands))


    def run_job_script(self):
        """Runs the 'script.beanshell' located in the job directory."""
        with open("%s/script.beanshell" % self.job_dir, "rb") as i:
            raw, html = self.heritrix.execute_script(self.name, " beanshell", i.read())


    def write_act_info(self):
        """Writes w3act job information to disk."""
        with open("%s/latest/w3act-info.json" % self.job_dir, "wb") as o:
            o.write(json.dumps(self.info, indent=4))


    def waitfor(self, status):
        """Waits for the job to reach a particular status."""
        try:
            while self.heritrix.status(self.name) not in status:
                time.sleep(10)
        except ParseError:
            time.sleep(10)


    def start(self):
        """Starts the job."""
        logger.info("Building %s" % self.name)
        self.heritrix.build_job(self.name)
        self.waitfor("NASCENT")
        logger.info("Launching %s" % self.name)
        self.heritrix.launch_job(self.name)
        self.waitfor("PAUSED")
        self.write_act_info()
        logger.info("Running scripts for %s" % self.name)
        self.run_job_script()
        # NOTE: The below line is a kludge to avoid an issue in the AsynchronousMQExtractor... TODO Remove?
        #self.heritrix.execute_script(self.name, "groovy", "appCtx.getBean(\"extractorMq\").setupChannel();")
        if self.use_credentials:
            for i, target in enumerate(self.info):
                if "secretId" in target["watchedTarget"].keys() and target["watchedTarget"]["secretId"]:
                    logger.info("Getting credentials for %s..." % target["title"])
                    new_info = credentials.handle_credentials(target, self.name, self.heritrix)
                    self.info[i] = new_info
        logger.info("Unpausing %s" % self.name)
        self.heritrix.unpause_job(self.name)
        self.waitfor("RUNNING")


    def stop(self):
        """Stops the job if already running, then starts."""
        status = self.heritrix.status(self.name)
        if status != "":
            logger.info("Killing alread-running job: %s (STATUS: %s)" % (self.name, status))
            if status is "RUNNING":
                self.heritrix.pause_job(self.name)
                self.waitfor("PAUSED")
            self.heritrix.terminate_job(self.name)
            self.waitfor("FINISHED")
            self.heritrix.teardown_job(self.name)
            self.waitfor("")


    def restart(self):
        self.stop()
        self.start()

