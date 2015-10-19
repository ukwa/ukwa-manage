import re
import csv
import json
import time
import requests
import psycopg2
from lxml import html
from requests.auth import HTTPBasicAuth

conn = psycopg2.connect("dbname='Dwct' user='postgres' host='safari-private'")
cur = conn.cursor()

response = requests.post("https://www.webarchive.org.uk/actstage/login", data={"email": "roger.coram@bl.uk", "password": "g07*$ECAVDU8QoU"})
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
Pulled straight from the web interface using the
DISTINCT values from agency.agc_name.
"""
organisations = {
    "British Library": 1,
    "National Library of Scotland": 3,
    "National Library of Wales": 2
}


"""
COLLECTIONS
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

# Pulled straight from:
#     https://www.webarchive.org.uk/actstage/subjects/list
#
subjects = [{"title":"Arts & Humanities","url":"/actstage/subjects/24","key":"\"24\"","children":[{"title":"Architecture","url":"/actstage/subjects/23","key":"\"23\""},{"title":"Art and Design","url":"/actstage/subjects/29","key":"\"29\""},{"title":"Comedy and Humour","url":"/actstage/subjects/71","key":"\"71\""},{"title":"Dance","url":"/actstage/subjects/89","key":"\"89\""},{"title":"Family History / Genealogy","url":"/actstage/subjects/112","key":"\"112\""},{"title":"Film / Cinema","url":"/actstage/subjects/113","key":"\"113\""},{"title":"Geography","url":"/actstage/subjects/119","key":"\"119\""},{"title":"History","url":"/actstage/subjects/130","key":"\"130\""},{"title":"Languages","url":"/actstage/subjects/148","key":"\"148\""},{"title":"Literature","url":"/actstage/subjects/161","key":"\"161\""},{"title":"Local History","url":"/actstage/subjects/164","key":"\"164\""},{"title":"Music","url":"/actstage/subjects/181","key":"\"181\""},{"title":"News and Contemporary Events","url":"/actstage/subjects/188","key":"\"188\""},{"title":"Philosophy and Ethics","url":"/actstage/subjects/205","key":"\"205\""},{"title":"Publishing, Printing and Bookselling","url":"/actstage/subjects/232","key":"\"232\""},{"title":"Religion","url":"/actstage/subjects/237","key":"\"237\""},{"title":"Theatre","url":"/actstage/subjects/273","key":"\"273\""},{"title":"TV and Radio","url":"/actstage/subjects/281","key":"\"281\""}]},{"title":"Business, Economy & Industry","url":"/actstage/subjects/13","key":"\"13\"","children":[{"title":"Agriculture, Fishing, and Forestry","url":"/actstage/subjects/12","key":"\"12\""},{"title":"Banking, Insurance, Accountancy and Financial Economics","url":"/actstage/subjects/33","key":"\"33\""},{"title":"Business Studies and Management Theory","url":"/actstage/subjects/53","key":"\"53\""},{"title":"Company Web Sites","url":"/actstage/subjects/75","key":"\"75\""},{"title":"Economic Development, Enterprise and Aid","url":"/actstage/subjects/101","key":"\"101\""},{"title":"Economics and Economic Theory","url":"/actstage/subjects/103","key":"\"103\""},{"title":"Employment, Unemployment and Labour Economics","url":"/actstage/subjects/106","key":"\"106\""},{"title":"Industries","url":"/actstage/subjects/139","key":"\"139\""},{"title":"Marketing and Market Research","url":"/actstage/subjects/173","key":"\"173\""},{"title":"Trade, Commerce, and Globalisation","url":"/actstage/subjects/277","key":"\"277\""},{"title":"Transport and Infrastructure","url":"/actstage/subjects/278","key":"\"278\""}]},{"title":"Education & Research","url":"/actstage/subjects/96","key":"\"96\"","children":[{"title":"Dictionaries, Encyclopaedias, and Reference Works","url":"/actstage/subjects/95","key":"\"95\""},{"title":"Further Education","url":"/actstage/subjects/116","key":"\"116\""},{"title":"Higher Education","url":"/actstage/subjects/129","key":"\"129\""},{"title":"Libraries, Archives and Museums","url":"/actstage/subjects/156","key":"\"156\""},{"title":"Lifelong Learning","url":"/actstage/subjects/159","key":"\"159\""},{"title":"Preschool Education","url":"/actstage/subjects/218","key":"\"218\""},{"title":"School Education","url":"/actstage/subjects/242","key":"\"242\""},{"title":"Special Needs Education","url":"/actstage/subjects/260","key":"\"260\""},{"title":"Vocational Education","url":"/actstage/subjects/287","key":"\"287\""}]},{"title":"Government, Law & Politics","url":"/actstage/subjects/63","key":"\"63\"","children":[{"title":"Central Government","url":"/actstage/subjects/62","key":"\"62\""},{"title":"Civil Rights, Pressure Groups, and Trade Unions","url":"/actstage/subjects/69","key":"\"69\""},{"title":"Crime, Criminology, Police and Prisons","url":"/actstage/subjects/84","key":"\"84\""},{"title":"Devolved Government","url":"/actstage/subjects/93","key":"\"93\""},{"title":"Inter-Governmental Agencies","url":"/actstage/subjects/140","key":"\"140\""},{"title":"International Relations, Diplomacy, and Peace","url":"/actstage/subjects/143","key":"\"143\""},{"title":"Law and Legal System","url":"/actstage/subjects/150","key":"\"150\""},{"title":"Local Government","url":"/actstage/subjects/163","key":"\"163\""},{"title":"Political Parties","url":"/actstage/subjects/209","key":"\"209\""},{"title":"Politics, Political Theory and Political Systems","url":"/actstage/subjects/216","key":"\"216\""},{"title":"Public Inquiries","url":"/actstage/subjects/231","key":"\"231\""}]},{"title":"Medicine & Health","url":"/actstage/subjects/15","key":"\"15\"","children":[{"title":"Alternative Medicine / Complementary Medicine","url":"/actstage/subjects/14","key":"\"14\""},{"title":"Conditions and Diseases","url":"/actstage/subjects/80","key":"\"80\""},{"title":"Health Organisations and Services","url":"/actstage/subjects/125","key":"\"125\""},{"title":"Medicines, Treatments and Therapies","url":"/actstage/subjects/177","key":"\"177\""},{"title":"Public Health and Safety","url":"/actstage/subjects/229","key":"\"229\""}]},{"title":"Science & Technology","url":"/actstage/subjects/79","key":"\"79\"","children":[{"title":"Computer Science, Information Technology and Web Technology","url":"/actstage/subjects/78","key":"\"78\""},{"title":"Engineering","url":"/actstage/subjects/108","key":"\"108\""},{"title":"Environment","url":"/actstage/subjects/109","key":"\"109\""},{"title":"Life Sciences","url":"/actstage/subjects/158","key":"\"158\""},{"title":"Mathematics","url":"/actstage/subjects/175","key":"\"175\""},{"title":"Physical Sciences","url":"/actstage/subjects/206","key":"\"206\""},{"title":"Popular Science","url":"/actstage/subjects/217","key":"\"217\""},{"title":"Zoology, Veterinary Science and Animal Health","url":"/actstage/subjects/294","key":"\"294\""}]},{"title":"Society & Culture","url":"/actstage/subjects/74","key":"\"74\"","children":[{"title":"Communities","url":"/actstage/subjects/73","key":"\"73\""},{"title":"Digital Society","url":"/actstage/subjects/97","key":"\"97\""},{"title":"Food and Drink","url":"/actstage/subjects/115","key":"\"115\""},{"title":"Social Problems and Welfare","url":"/actstage/subjects/249","key":"\"249\""},{"title":"Sociology, Anthropology and Population Studies","url":"/actstage/subjects/252","key":"\"252\""},{"title":"Sports and Recreation","url":"/actstage/subjects/264","key":"\"264\""},{"title":"Travel & Tourism","url":"/actstage/subjects/279","key":"\"279\""}]}]

# Build our subject lookup...
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
By making sure s_primary seeds appear LAST the
Dictlist() always has the primary seed FIRST.
"""

cur.execute("""SELECT s_target_id, s_primary, s_seed FROM db_wct.seed ORDER BY s_primary""")
rows = cur.fetchall()

seeds = Dictlist()
for s_target_id, s_primary, s_seed in rows:
    seeds[s_target_id] = s_seed

# Need a list of all URLs in w3act; could use:
#     conn = psycopg2.connect("dbname='w3act' user='postgres' host='192.168.45.25'")
# ...but that port is firewalled. So dump them using:
#    SELECT url FROM field_url;

with open("/home/rcoram/field_url.txt", "r") as i:
    w3act_urls = [u.strip() for u in i]

"""
SELECTORS
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

# Hard-code some values...
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

cur.execute("""SELECT DISTINCT at_oid, at_name, regexp_replace(at_desc, E'[\\r\\n?]+', ' ', 'g'), agc_name, usr_email, usr_firstname, usr_lastname
    FROM db_wct.abstract_target_grouptype_view
    JOIN db_wct.wctuser ON (at_owner_id = usr_oid)
    JOIN db_wct.agency ON (usr_agc_oid = agc_oid)
    WHERE tg_type IS NULL
    AND at_state NOT IN (4)""")
rows = cur.fetchall()

"""
1 = Pending
2 = Reinstated
3 = Nominated
4 = Rejected
5 = Approved
6 = Cancelled
7 = Completed
"""

targets = []
with open("targets-00-wct-vs-w3act.json", "w") as o:
    for at_oid, at_name, at_desc, agc_name, usr_email, usr_firstname, usr_lastname in rows:
        # Skip if there are no URLs for this Target...
        if at_oid not in seeds.keys():
            o.write("TARGET: %s - no URLs.\n" % (at_oid))
            continue
        # Skip if any URLs for this Target are already in w3act...
        if any(url in w3act_urls for url in seeds[at_oid]):
            o.write("TARGET: %s - URL(s) already in w3act.\n" % (at_oid))
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
"""

pe_statuses = {
    "2": "GRANTED",
    "3": "PENDING",
}

for target in targets:
    # Shouldn't be necessary but...
    #target["field_uk_postal_address"] = False
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

