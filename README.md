Shepherd
========

Coordinates the services that make up the UK Web Archive.

Issues

- Need to string together the actual crawl running process and try it out, see how well it works.

- Need to finish docstow3act.py script.
    - Need API hook to lookup watched target ID? Or maybe use that as the source instead? WTID:123?
- Need copytohdfs to run, but no indexing please. Maybe extend based on the syncer script and remove the confirmed copies?
- Need to complete sipstodls, based on scanning a crawl log file to determine the WARCs, and then packaging it all up together.
- Do we need a WARC message chain? I'd say no.
- Need to collect WARC filename to HDFS path mapping when we copy to HDFS.
- Need to collect WARC filename to ARK mapping when we make the old SIPs.

- Need to go through a real SIP and check I understand it and it's contents. DONE.
- Watch out for rotated logs? (on restart, they are moved out of the way by setupLogFile like so: crawl.log.20160127091852)
- Run a frequent-by (FC) crawl stream as well as the NPLD frequent crawl FC stream.
- Also capture HAR WARCs for the same period? Tricky to precisely arrange. Simplest idea is just to take whatever was completed/closed in the last time period, and leave the .open ones where they are. Not worth optimising right now.

- Pick up Geo DB location from an environment variable (GEOLITE2_CITY_MMDB_LOCATION done)



    $ python agents/sipstodls.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f"

    $ python agents/docstow3act.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f" post-crawl DH-1-documents-to-catalogue


h3cc - Heritrix3 Crawl Controller
---------------------------------

Script to interact with Heritrix directly, to perform some general crawler operations.

The ```info-xml``` command downloads the raw XML version of the job information page, which can the be filtered by other tools to extract information. For example, the Java heap status can be queried like this:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//heapReport/usedBytes"

Similarly, the number of novel URLs stored in the WARCs can be determined from:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//warcNovelUrls"
