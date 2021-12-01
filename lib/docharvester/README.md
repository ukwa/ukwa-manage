Document Harvester
==================

The Document Harvester post-processes crawls to find Documents belonging to Watched Targets (as defined in W3ACT).

The tools here help implement this process.

The main log processor is currently part of the log analysis job implemented by the `windex` command. This job populates a `documents_found` PostgreSQL database, which are then processed using the tools provided here. 

```
python -m lib.docharvester.cmd process -T /home/anj/2021-11-23-w3act-csv-crawl-feed-npld.all.json -v --act-pw XXXXX
```

Two utilities are also provided to populate the `documents_found` database from other sources.

e.g. from the Kafka crawl log:

```
python -m lib.docharvester.cmd import-kafka -T ~/2021-11-23-w3act-csv-crawl-feed-npld.all.json -k crawler06.n45.bl.uk:9094 -v  -G TEMPG1 | tee kafka-docs-5.jsonl
```

There is also an `import-luigi` command that is used to migrate the older Luigi implmentation's document state files into this `documents_found` system.


To Do
-----

- [ ] Run on Crawler06 to populate missing documents.
- [ ] Regenerate missed crawl logs.
- [ ] Accept extended fields from processed documents, `publishers[]` `publisher` `publication_date` `match_failed` `api_call_failed`

2021-12-01 14:09:08,352: INFO - lib.docharvester.to_w3act - Sending doc: {'document_url': 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1034406/test-and-trace-week-76.pdf', 'title': 'Weekly statistics for NHS Test and Trace (England): 4 to 10 November 2021', 'filename': 'test-and-trace-week-76.pdf', 'landing_page_url': 'https://www.gov.uk/government/publications/weekly-statistics-for-nhs-test-and-trace-england-4-to-10-november-2021', 'source': 'tid:114728:https://www.gov.uk/government/collections/slides-and-datasets-to-accompany-coronavirus-press-conferences', 'wayback_timestamp': '20211119001232', 'launch_id': None, 'job_name': 'frequent-npld', 'size': 1433224, 'target_id': 147227, 'status': 'ACCEPTED', 'publication_date': '2021-11-18T15:00:04.000+00:00', 'publishers': ['UK Health Security Agency'], 'publisher': 'UK Health Security Agency'}

2021-12-01 14:10:11,009: ERROR - lib.docharvester.to_w3act - The document has been REJECTED! : {'document_url': 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/588647/Smart_Systems_Forum_expression_of_interest_letter.pdf', 'title': '', 'filename': 'Smart_Systems_Forum_expression_of_interest_letter.pdf', 'landing_page_url': 'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/588647/Smart_Systems_Forum_expression_of_interest_letter.pdf', 'source': 'tid:46004:https://energyinst.org/', 'wayback_timestamp': '20211119081406', 'launch_id': None, 'job_name': 'frequent-npld', 'size': 64377, 'target_id': None, 'status': 'REJECTED', 'api_call_failed': "Could not find rel['up'] relationship.", 'match_failed': True}


