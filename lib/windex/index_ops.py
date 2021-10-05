import requests
import json

# 
# Scan a given Solr index for a given set of field values
# e.g. url = [A, B, C]
# and offer to remove those records.
# 
def remove_solr_records(solr_url, field, values):
	# Panic if there's an unexpected character in the values:
	if '"' in ','.join(values):
		raise Exception("Unexpected quote character in values!")
	# Escape ampersand:
	#values = (v.replace('&', '%26') for v in values)
	# Otherwise, build a query:
	selector = ' OR '.join(f'{field}:"{v}"' for v in values)
	# Run the query directly to check how many records match:
	select = {
		'q': selector,
		'wt': 'json',
	}
	print(f"Querying Solr at: {solr_url}")
	print(f"For records matching: {selector}")
	r  = requests.post(f"{solr_url}/select", data=select)
	if r.status_code != 200:
		raise Exception(f"Query failed!\n{r.text}")
	result = r.json()
	num_found = result['response']['numFound']
	print(f"Found {num_found} matching records. Here are {len(result['response']['docs'])} examples:")
	# If we have matches, offer to delete them:
	if num_found > 0:
		print(f"Here are {len(result['response']['docs'])} examples:")
		for doc in result['response']['docs']:
			print(f"    id = {doc['id']}, {field} = {doc[field]}")
		confirm = input(f"Delete these {num_found} records? (enter 'Y' to confirm deletion): ")
		if confirm == "Y":
			print("Deleting...")
			h = {
				"Content-Type": "application/json"
			}
			update = {
				"delete": {
					"query": selector
				},
				"commit": {}
			}
			# Use JSON update syntax as per 
			# https://solr.apache.org/guide/7_0/uploading-data-with-index-handlers.html#sending-json-update-commands
			r  = requests.post(f"{solr_url}/update", data=json.dumps(update), headers=h)
			if r.status_code != 200:
				raise Exception(f"Operation failed! {r.text}")
			else:
				print(r.text)
		else:
			print("Delete operation has not been attempted.")


if __name__ == "__main__":
	remove_solr_records(
		"http://trackdb.dapi.wa.bl.uk/solr/tracking",
		"file_path_s",
		[
			"/heritrix/output/logs/quarterly-0-202300/20150323152237/runtime-errors.log.cp00001-20150325080247.gz", 
			"/heritrix/output/logs/quarterly-0-202300/20150323152237/runtime-errors.log.cp00002-20150327004250.gz"
		]
	)