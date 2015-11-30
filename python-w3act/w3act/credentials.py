import re
import sys
import json
import w3act
import logging
import heritrix
import requests
from lxml import html
from slugify import slugify
from urlparse import urlparse, urljoin

logger = logging.getLogger("w3act.credentials")
logging.getLogger("requests").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()

def scrape_login_page(url, username, password):
    """Scrape a page for hidden fields pre-login."""
    r = requests.get(url)
    h = html.fromstring(r.content)
    path = urlparse(url).path
    forms = h.xpath("//form")
    form = None
    if len(forms) > 1:
        logger.warning("Multiple forms found!")
        logger.warning("Looking for 'login'...")
        for f in forms:
            if f.xpath("contains(translate(@name, 'LOGIN', 'login'), 'login')"):
                form = f
                logger.info("Using form: %s" % form.attrib["name"])
                break
        if form is None:
            logger.warning("Looking for '%s'..." % path)
            for f in forms:
                if f.xpath("contains(@action, %s)" % path):
                    form = f
                    logger.info("Using form: %s" % form.xpath("@action"))
                    break
        if form is None:
            logger.error("Could not determine correct login form: %s" % url)
            sys.exit(1)
    else:
        form = forms[0]
    fields = []
    for f in form.xpath(".//input"):
        if any(val in f.attrib["name"].lower() for val in ["user", "email"]):
            fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": username})
        elif any(val in f.attrib["name"].lower() for val in ["pass"]):
            fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": password})
        else:
            if "value" in f.keys():
                fields.append({"name": f.attrib["name"].replace("$", "\\$"), "value": f.attrib["value"]})
    form_action = urljoin(url, form.attrib["action"])
    return (form_action, fields)

def get_logout_regex(logout_url):
    """Generates the regular expression to avoid the logout page."""
    return re.escape(logout_url).replace("\\", "\\\\")

def get_credential_script(info, fields):
    """Generates a Heritrix script to add new credentials."""
    domain = urlparse(info["watchedTarget"]["loginPageUrl"]).netloc
    id = slugify(domain)
    script = """hfc = new org.archive.modules.credential.HtmlFormCredential()
    hfc.setDomain("%s")
    hfc.setLoginUri("%s")
    fi = hfc.getFormItems()""" % (domain, info["watchedTarget"]["loginPageUrl"])
    for field in fields:
        script += """\nfi.put("%s", "%s")""" % (field["name"], field["value"])
    script += """\nappCtx.getBean("credentialStore").getCredentials().put("%s", hfc)""" % (id)
    return script

def get_seeds_script(seeds):
    """Generates script to add a list of seeds."""
    script = ""
    for seed in seeds:
        script += """\nappCtx.getBean("seeds").seedLine("%s")""" % (seed)
    return script

def get_logout_exclusion_script(logout_regex):
    return """appCtx.getBean("listRegexFilterOut").regexList.add(java.util.regex.Pattern.compile("%s"))""" % (logout_regex)

def handle_credentials(info, job, api):
    """Retrieves credentials for a w3act Target and adds these via the Heritrix API.

    Arguments:
    info --  a w3act info record.
    job -- Heritrix job name.
    api -- a python-heritrix instance.

    """
    w = w3act.ACT()
    if "watched" in info.keys() and info["watched"]:
        if info["watchedTarget"]["secretId"] is not None:
            secret = w.get_secret(info["watchedTarget"]["secretId"])
            form_action, fields = scrape_login_page(info["watchedTarget"]["loginPageUrl"], secret["username"], secret["password"])
            logout_regex = get_logout_regex(info["watchedTarget"]["logoutUrl"])
            logger.info("Excluding: %s" % logout_regex)
            api.execute(script=get_logout_exclusion_script(logout_regex), job=job, engine="groovy")
            logger.info("Adding credentials...")
            api.execute(script=get_credential_script(info, fields), job=job, engine="groovy")
            logger.info("Adding login page as a seeds...")
            new_seeds = list(set([form_action, info["watchedTarget"]["loginPageUrl"]]))
            info["seeds"] += new_seeds
            api.execute(script=get_seeds_script(new_seeds), job=job, engine="groovy")
    return info

