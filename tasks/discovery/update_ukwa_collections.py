import json
import requests
import configparser
import pysolr


def addCollection(c_id, parent_id):
    col_url = actUrl + "/api/collections/%s" % c_id
    col_req = requests.get(col_url, headers=headers)
    col = json.loads(col_req.content.decode('utf8').encode('ascii', 'ignore'))
    if col['field_publish']:
        print("Publishing...",c['title'])

        # add a document to the Solr index
        s.add([
            {
                "id": col["id"],
                "type": "collection",
                "name": col["name"],
                "description": col["description"],
                "parentId": parent_id
            }
        ])

        # Look up all Targets within this Collection and add them.
        t_url = actUrl + "/api/targets/bycollection/%s" % c_id
        t_req = requests.get(t_url, headers=headers)
        targets = json.loads(t_req.content.decode('utf8').encode('ascii', 'ignore'))

        for t in targets:
            target_url = actUrl + "/api/targets/%s" % int(t)
            print "target_url=" + target_url
            target_req = requests.get(target_url, headers=headers)
            target = json.loads(target_req.content.decode('utf8').encode('ascii', 'ignore'))
            #target['collection'] = c_id

            # add a document to the Solr index
            s.add([{
                "id": target["id"],
                "type": "target",
                "parentId": c_id,
                "title": target["title"],
                "description": target["description"],
                "url": target["fieldUrls"][0]["url"],
                "additionalUrl": [t["url"] for t in target["fieldUrls"] if t["position"] > 0],
                "language": target["language"],
                "startDate": target["crawlStartDateISO"],
                "endDate": target["crawlEndDateISO"],
                "licenses": [l["id"] for l in target["licenses"]]
            }])

        # Add child collections
        for cc in col["children"]:
            addCollection(int(cc["id"]), c_id)
    else:
        print("Skipping...",c['title'])

    return

config = configparser.ConfigParser()
config.read('act.cfg')

actUrl = config.get('act', 'url')
fullUrl = actUrl + "/login"
print "fullUrl=" + fullUrl

# create a connection to a Solr server
solrUrl = config.get('solr', 'url')
print "solrUrl=" + solrUrl
s = pysolr.Solr(solrUrl, timeout=30)

response = requests.post(fullUrl, 
    data={"email": config.get('act', 'username'),
    "password": config.get('act','password')})

if response.status_code != 200:
    print "Web request returned status " + str(response.status_code)

else:	
    cookie = response.history[0].headers["set-cookie"]
    headers = {
        "Cookie": cookie
    }

    all = requests.get(actUrl + "/api/collections", headers=headers)

    collections_tree = json.loads(all.content)
    for c in collections_tree:
        addCollection(int(c['key']), None)
        s.commit()


