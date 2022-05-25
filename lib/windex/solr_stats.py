import requests
import json


def gather_solr_stats():
    r = requests.get("http://solr.api.wa.bl.uk/solr/admin/collections?action=CLUSTERSTATUS&wt=json")

    collections = r.json()['cluster']['collections']
    for collection in collections:
        shards = collections[collection]['shards']
        for shard in shards:
            replicas = shards[shard]['replicas']
            for replica in replicas:
                core = replicas[replica]['core']
                base_url = replicas[replica]['base_url']
                status_url = f"{base_url}/admin/cores?action=STATUS&core={core}&wt=json"
                r = requests.get(status_url)
                stats = r.json()['status'][core]['index']
                stats['solr_collection'] = collection
                stats['solr_shard'] = shard
                stats['solr_replica'] = replica
                stats['solr_core'] = core
                stats['solr_base_url'] = base_url
                print(json.dumps(stats))


if __name__ == '__main__':
    gather_solr_stats()