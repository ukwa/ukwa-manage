import re
import csv
import json
import time
import requests
import psycopg2
from lxml import html
from requests.auth import HTTPBasicAuth

"""
Premise

We need to migrate all Targets from WCT and import them into w3act. For 
this, not only do we need the Target data, but we need to determine into 
which Subjects and Categories a Target will be placed in the new system.
For this, we determine the existing Subjects/Categories (in WCT) and their 
relevant mappings in w3act.
Then, when running through our list of WCT Targets, we need to skip (but log) 
those where the seeds are already in w3act (as we can't duplicate URLs). 
Similarly, we need to log any occasion w3act does not add a Target for later analysis.

Setup

Firstly, set up everything we need (imports, classes, connections to the database and w3act)...

"""

conn = psycopg2.connect("dbname='Dwct' user='postgres' host='safari-private'")
cur = conn.cursor()

response = requests.post("https://www.webarchive.org.uk/actstage/login", 
	data={"email": "roger.coram@bl.uk", "password": "XXXX"})
cookie = response.history[0].headers["set-cookie"]
headers = {
    "Cookie": cookie
}

class Dictlist(dict):
    def __setitem__(self, key, value):
        try:
            self[key]
        except KeyError:
            super(Dictlist, self).__setitem__(key, [])
        self[key].insert(0, value)


"""
ORGANISATIONS
Pulled straight from the web interface using the DISTINCT values from 
agency.agc_name and only those which are actually present in w3act.
"""
organisations = {
    "British Library": 1,
    "National Library of Scotland": 3,
    "National Library of Wales": 2
}


"""
COLLECTIONS
Firstly, we pull the old Collections from WCT and build an Target-ID->Collections lookup:
"""

cur.execute("""SELECT at_name, gm_child_id
    FROM db_wct.abstract_target
    JOIN db_wct.target_group ON (at_oid = tg_at_oid)
    JOIN db_wct.group_member ON (tg_at_oid = gm_parent_id)
    WHERE tg_type != 'Subject' AND tg_type IS NOT NULL;""")
rows = cur.fetchall()

wct_collections = Dictlist()
wct_collection_names = list(set([collection_name for collection_name, target_id in rows]))
for collection_name, target_id in rows:
    wct_collections[target_id] = collection_name

"""
Then we pull the Collections from w3act and build a Collection-name -> ID lookup:
"""

r = requests.get("https://www.webarchive.org.uk/actstage/api/collections", headers=headers)
collections = json.loads(r.text)

# Build our collection lookup...
w3act_collections = {}
def traverse(it):
    if (isinstance(it, list)):
        for item in it:
            traverse(item)
    elif (isinstance(it, dict)):
        w3act_collections[re.sub(" \([0-9]+\)$", "", it["title"])] = int(it["key"].replace("\"", ""))
        for key in it.keys():
            traverse(it[key])

traverse(collections)

# And hard-code some values...
w3act_collections["Candidates (UK General Election 2015)"] = 59
w3act_collections["Election Blogs (UK General Election 2015)"] = 105
w3act_collections["First World War Centenary, 2014-18 > Heritage Lottery Funded First World War Centenary Projects"] = 127
w3act_collections["Health and Social Care Act 2012 - NHS Reforms > Local Involvement Networks (LINks)"] = 165
w3act_collections["Health and Social Care Act 2012 - NHS Reforms > NHS London - Strategic Health Authority Cluster"] = 168
w3act_collections["Health and Social Care Act 2012 - NHS Reforms > NHS Midlands and East - Strategic Health Authority Cluster"] = 27
w3act_collections["Health and Social Care Act 2012 - NHS Reforms > NHS North of England - Strategic Health Authority Cluster"] = 50
w3act_collections["Health and Social Care Act 2012 - NHS Reforms > NHS South of England - Strategic Health Authority Cluster"] = 36
w3act_collections["Interest Groups (UK General Election 2015)"] = 142
w3act_collections["Isle of Man"] = 509
w3act_collections["News and Commentary (UK General Election 2015)"] = 187
w3act_collections["Opinion Polls (UK General Election 2015)"] = 201
w3act_collections["Political Parties - Local (UK General Election 2015)"] = 210
w3act_collections["Political Parties - National (UK General Election 2015)"] = 211
w3act_collections["Public & Community Engagement (UK General Election 2015)"] = 227
w3act_collections[u"Pêl Droed Cymru - Welsh Football"] = 514
w3act_collections["Regulation & Guidance (UK General Election 2015)"] = 234
w3act_collections["Research Centers and Think Tanks (UK General Election 2015)"] = 240
w3act_collections["Scottish Government"] = 121
w3act_collections["Scottish Independence Referendum 2014"] = 65


"""
SUBJECTS
Firstly, we create a WCT Target-ID->Subjects lookup, much as we did with Collections:
"""
cur.execute("""SELECT at_name, gm_child_id
    FROM db_wct.abstract_target
    JOIN db_wct.target_group ON (at_oid = tg_at_oid)
    JOIN db_wct.group_member ON (tg_at_oid = gm_parent_id)
    WHERE tg_type != 'Collection' AND tg_type IS NOT NULL;""")
rows = cur.fetchall()

wct_subjects = Dictlist()
wct_subject_names = list(set([subject_name for subject_name, target_id in rows]))
for at_name, gm_child_id in rows:
    wct_subjects[gm_child_id] = at_name

# Next we create a w3act Subject-name -> ID lookup, using the data in the raw HTML of:
#     https://www.webarchive.org.uk/actstage/subjects/list
#
r = requests.get("https://www.webarchive.org.uk/actstage/subjects/list", headers=headers)
h = html.fromstring(r.content)
src = h.xpath("//script[not(@src)]/text()")[0]
subjects = json.loads(src[src.index("["):src.rindex("]")+1])

w3act_subjects = {}
def traverse(it):
    if (isinstance(it, list)):
        for item in it:
            traverse(item)
    elif (isinstance(it, dict)):
        w3act_subjects[re.sub(" \([0-9]+\)$", "", it["title"])] = int(it["key"].replace("\"", ""))
        for key in it.keys():
            traverse(it[key])

traverse(subjects)


"""
SEEDS
We need a WCT Target-ID->Seeds lookup:

By making sure s_primary seeds appear LAST the
Dictlist() always has the primary seed FIRST.

"""

cur.execute("""SELECT s_target_id, s_primary, s_seed FROM db_wct.seed ORDER BY s_primary""")
rows = cur.fetchall()

seeds = Dictlist()
for s_target_id, s_primary, s_seed in rows:
    seeds[s_target_id] = s_seed

# Similarly, we need a list of URLs already in w3act. We cannot, however, connect 
# to the database due to the firewall. Instead, we can export and read them in as per the comment below:

# Need a list of all URLs in w3act; could use:
#     conn = psycopg2.connect("dbname='w3act' user='postgres' host='192.168.45.25'")
# ...but that port is firewalled. So dump them using:
#    SELECT url FROM field_url;

with open("/home/rcoram/field_url.txt", "r") as i:
    w3act_urls = [u.strip() for u in i]

"""
SELECTORS
We need a of Curators/Selectors:
"""
selectors = {}
for i in range(0,6):
    r = requests.get("https://www.webarchive.org.uk/actstage/curators/list?p=%s" % i, headers=headers)
    h = html.fromstring(r.content)
    for a in h.xpath("//a[contains(@href, '/actstage/curators/')]"):
        id = a.attrib["href"].split("/")[-1]
        if not id.isdigit():
            continue
        selectors[a.text] = a.attrib["href"].split("/")[-1]

# To accommodate any name-changes, hard-code some values:
selectors["Nicola Johnson"] = 39


"""
TARGETS
    "title": title,
    "field_crawl_frequency": freq,
    "field_nominating_org": org,
    "field_urls": [url],
    "field_collection_cats": [coll],
    "field_crawl_start_date": start_date,
    "field_crawl_end_date": end_date,
    "selector": sel,
    "field_subjects": [subj],
    "field_uk_postal_address": True,
    "uk_postal_address_url": addr_url,
    "field_via_correspondence": false,
    "field_professional_judgement": false,
    "field_professional_judgement_exp": "",
    "field_ignore_robots_txt": True,
"""

"""
1 = Pending
2 = Reinstated
3 = Nominated
4 = Rejected
5 = Approved
6 = Cancelled
7 = Completed
"""

cur.execute("""SELECT DISTINCT at_oid, at_name, regexp_replace(at_desc, E'[\\r\\n?]+', ' ', 'g'), agc_name, usr_email, usr_firstname, usr_lastname
    FROM db_wct.abstract_target_grouptype_view
    JOIN db_wct.wctuser ON (at_owner_id = usr_oid)
    JOIN db_wct.agency ON (usr_agc_oid = agc_oid)
    WHERE tg_type IS NULL
    AND at_state NOT IN (4)""")
rows = cur.fetchall()

"""
Now we build up our list of Targets to submit to w3act. Note that we're 
logging failures to a file, targets-00-wct-vs-w3act.json, and skipping 
Targets with no seeds or where any seed is already in w3act:
"""

targets = []
targets_skipped = {}
for at_oid, at_name, at_desc, agc_name, usr_email, usr_firstname, usr_lastname in rows:
    # Skip if there are no URLs for this Target...
    if at_oid not in seeds.keys():
        targets_skipped[str(at_oid)] = "TARGET: %s - no URLs.\n" % (at_oid)
        continue
    # Skip if any URLs for this Target are already in w3act...
    if any(url in w3act_urls for url in seeds[at_oid]):
        targets_skipped[str(at_oid)] = "TARGET: %s - URL(s) already in w3act.\n" % (at_oid)
        continue
    target = {
        "title": at_name,
        "field_wct_id": str(at_oid),
        "field_urls": seeds[at_oid],
        "field_subjects": [],
        "field_collection_cats": []
    }
    if agc_name in organisations.keys():
        target["field_nominating_org"] = organisations[agc_name]
    if at_oid in wct_collections.keys():
        for col in wct_collections[at_oid]:
            if col.decode("utf-8") in w3act_collections.keys():
                target["field_collection_cats"].append(w3act_collections[col.decode("utf-8")])
    if at_oid in wct_subjects.keys():
        for sub in wct_subjects[at_oid]:
            if sub in w3act_subjects.keys():
                target["field_subjects"].append(w3act_subjects[sub])
    if "%s %s" % (usr_firstname, usr_lastname) in selectors.keys():
        target["selector"] = selectors["%s %s" % (usr_firstname, usr_lastname)]
    targets.append(target)


"""
PERMISSIONS
Adding the permissions is done separately. Here, we use the list of Targets 
created above to pull permissions information (from SPT) and add these to the Target metadata:
"""

pe_statuses = {
    "2": "GRANTED",
    "3": "PENDING",
}

for target in targets:
    cur.execute("""SELECT DISTINCT aa_name, aa_adress, aa_contact, aa_email, aa_phone_number, aa_desc, pe_start_date, pe_status, s_id, l_third, l_publicity, l_authorize
        FROM db_wct.seed
        LEFT OUTER JOIN db_wct.seed_permission ON (s_oid = sp_seed_id)
        LEFT OUTER JOIN db_wct.permission ON (sp_permission_id = pe_oid)
        LEFT OUTER JOIN db_wct.authorising_agent ON (pe_auth_agent_id = aa_oid)
        LEFT OUTER JOIN permissions.selection ON (s_target_id = s_wct_target_id)
        LEFT OUTER JOIN permissions.licence on (s_id = l_s_id)
        WHERE s_target_id = %(field_wct_id)s
        AND pe_status = 2""", (target))
    rows = cur.fetchall()
    for aa_name, aa_adress, aa_contact, aa_email, aa_phone_number, aa_desc, pe_start_date, pe_status, s_id, l_third, l_publicity, l_authorize in rows:
        epoch = time.mktime(pe_start_date.timetuple())
        if aa_name is None or not aa_name.startswith("SRO"):
            target["licenseStatus"] = pe_statuses[str(pe_status)]
            target["licenses"] = [295]
            target["crawlPermissions"] = [
                {
                    "createdAt": int(epoch)*1000,
                    "contactPerson": {
                        "postalAddress": aa_adress.strip() if aa_adress is not None else "",
                        "email": aa_email.strip() if aa_email is not None else "",
                        "description": aa_desc.strip() if aa_desc is not None else "",
                        "contactOrganisation": aa_name.strip() if aa_name is not None else "",
                        "name": aa_contact.strip() if aa_contact is not None else "",
                        "phone": aa_phone_number.strip() if aa_phone_number is not None else ""
                    },
                    "license": {
                        "id": 295
                    },
                    "status": pe_statuses[str(pe_status)],
                    "requestFollowup": False,
                }
            ]
            if s_id is not None:
                target["field_spt_id"] = str(s_id)
            if l_third is not None:
                target["crawlPermissions"][0]["thirdPartyContent"] = l_third
            if l_publicity is not None:
                target["crawlPermissions"][0]["publish"] = l_publicity
            if l_authorize is not None:
                target["crawlPermissions"][0]["agree"] = l_authorize
        # Shouldn't really need to...
        break

"""
Now we submit the Target records (including Target and Permission metadata) to w3act:

"""

with open("targets-01-exported.json", "a") as o:
    o.write(json.dumps(targets, indent=4))

targets_success = []
targets_failed = []
headers["content-type"] = "application/json"
for target in targets:
    r = requests.post("https://www.webarchive.org.uk/actstage/api/targets", json.dumps(target), auth=HTTPBasicAuth("roger.coram@bl.uk", "g07*$ECAVDU8QoU"), headers=headers)
    if r.status_code == 201:
        target["w3act_id"] = r.text.split("/")[-1]
        targets_success.append(target)
    else:
        target["w3act_error"] = r.content
        targets_failed.append(target)


with open("targets-02-success.json", "a") as o:
    o.write(json.dumps(targets_success, indent=4))


with open("targets-03-failed.json", "a") as o:
    o.write(json.dumps(targets_failed, indent=4))

