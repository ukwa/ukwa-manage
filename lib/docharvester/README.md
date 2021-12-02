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



