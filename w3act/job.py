#!/usr/bin/env python

"""
Methods for controlling w3act jobs in Heritrix.
"""

import os
import json
import time
import shutil
import logging
import rfc3987
import heritrix
import requests
import settings
from w3act.lib import ACT
from lxml import etree
from xml.etree.ElementTree import ParseError


requests.packages.urllib3.disable_warnings()

logger = logging.getLogger("w3act.%s" % __name__)
handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

#Try to set logging output for all modules.
logging.root.setLevel(logging.WARNING)
logging.getLogger("").addHandler(handler)


mandatory_fields = ["field_url", "field_depth", "field_scope", settings.W3ACT_JOB_FIELD]
depth_sheets = {"capped_large": "higherLimit", "deep": "noLimit"}
scope_sheets = {"resource": "resourceScope", "plus1": "plus1Scope", "subdomains": "subdomainsScope"}


def to_surt(url):
        parsed = rfc3987.parse(url, rule="URI")
        authority = parsed["authority"].split(".")
        authority.reverse()
        return "http://(" + ",".join(authority) + ","


def get_surt_association_script(surt, sheet):
    """Creates the beanshell script for a SURT->Sheet association."""
    return "appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"%s\" );" % (surt, sheet)


def get_depth_scripts(seeds, depth):
    """Creates a list of beanshell commands for seed/depth."""
    script = []
    if depth in depth_sheets.keys():
        for seed in seeds:
            surt = to_surt(seed)
            sheet = depth_sheets[depth]
            script.append(get_surt_association_script(surt, sheet))
            logger.info("Setting cap for %s to %s" % (surt, sheet))
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


def get_blocking_scripts():
    """Blocks access to w3act's 'nevercrawl' targets."""
    w = ACT()
    j = w.get_ld_export("nevercrawl")
    script = []
    for target in j:
        blocked_urls = [s.strip() for s in target["field_url"].split(",")]
        for url in blocked_urls:
            try:
                surt = to_surt(url)
            except ValueError as v:
                logger.warning(str(v))
                continue
            sheet = "blockAll"
            script.append(get_surt_association_script(surt, sheet))
            logger.info("Blocking access to %s" % (surt))
    return script

def create_profile(job_name):
    """Creates the CXML content for a H3 job."""
    profile = etree.parse(settings.HERITRIX_PROFILE)
    profile.xinclude()
    cxml = etree.tostring(profile, pretty_print=True, xml_declaration=True, encoding="UTF-8")
    cxml = cxml.replace("REPLACE_JOB_NAME", job_name)
    cxml = cxml.replace("REPLACE_CLAMD_PORT", settings.CLAMD_PORT)
    cxml = cxml.replace("REPLACE_JOB_ROOT", job_name)
    cxml = cxml.replace("REPLACE_HERITRIX_JOBS", settings.HERITRIX_JOBS)
    return cxml
    

def setup_job_directory(name, seeds, depth, scope):
    """Creates the Heritrix job directory."""
    job_dir = "%s/%s/" % (settings.HERITRIX_JOBS, name)
    if not os.path.isdir(job_dir):
        os.makedirs(job_dir)

    shutil.copy(settings.HERITRIX_SHORTENERS, job_dir)
    shutil.copy(settings.HERITRIX_EXCLUDE, job_dir)
    shutil.copy(settings.HERITRIX_SURTS, job_dir)

    # Write seeds to disk:
    with open("%s/seeds.txt" % job_dir, "wb") as o:
        o.write("\n".join(seeds))

    # Write profile to disk:
    cxml = create_profile(name)
    with open("%s/crawler-beans.cxml" % job_dir, "wb") as o:
        o.write(cxml)

    # Write Sheet-associations to disk:
    commands = get_depth_scripts(seeds, depth)
    commands += get_scope_scripts(seeds, scope)
    commands += get_blocking_scripts()
    with open("%s/script.beanshell" % job_dir, "wb") as o:
        o.write("\n".join(commands))

    return job_dir


class W3actJob(object):
    """Represents a Heritrix job for w3act."""

    def __init__(self, w3act_target, heritrix=None):
        self.name = w3act_target[settings.W3ACT_JOB_FIELD]
        self.seeds = [s.strip() for s in w3act_target["field_url"].split(",")]
        self.job_dir = setup_job_directory(
            self.name,
            self.seeds,
            w3act_target["field_depth"],
            w3act_target["field_scope"]
        )
        if heritrix is not None:
            self.heritrix = heritrix
            self.heritrix.add(self.name)
        

    def setup_heritrix(self, api=None, host=None, port=None):
        if api is not None:
            self.heritrix = api
        else:
            self.heritrix = heritrix.API(host="https://%s:%s/engine" % (host, port), user="admin", passwd="bl_uk", verbose=False, verify=False)
        self.heritrix.add(self.name)


    def run_job_script(self):
        """Runs the 'script.beanshell' located in the job directory."""
        with open("%s/script.beanshell" % self.job_dir, "rb") as i:
            self.heritrix.execute(job=self.name, script=i.read(), engine="beanshell")


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
        self.heritrix.build(self.name)
        self.waitfor("NASCENT")
        logger.info("Launching %s" % self.name)
        self.heritrix.launch(self.name)
        self.waitfor("PAUSED")
        logger.info("Running scripts for %s" % self.name)
        self.run_job_script()
        logger.info("Unpausing %s" % self.name)
        self.heritrix.unpause(self.name)
        self.waitfor("RUNNING")


    def stop(self):
        """Stops the job if already running, then starts."""
        if self.heritrix.status(self.name) != "":
            logger.info("Killing alread-running job: %s" % job)
            self.heritrix.pause(self.name)
            waitfor("PAUSED")
            self.heritrix.terminate(self.name)
            waitfor("FINISHED")
            self.heritrix.teardown(self.name)
            waitfor("")


    def restart(self):
        self.stop()
        self.start()

