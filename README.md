# python-sip-verification
Package for the verification of SIP submission into DLS.

#### verify-sips.py
Intended to be scheduled as a nightly cronjob, this script:
1. Iterates over every message in a configured queue (`sip-submitted`) by default.
2. For each message, checks whether:
  * The message is outside the 7-day embargo period.
  * All the ARKs listed in the SIP are present in the list exposed by DLS.
3. If these criteria are met, the message is passed to the `index` queue.
4. If either criteria are not met, the message is placed back on the queue.

#### index-sips.py
**Note that this is experimental and thus not included in the `setup.py` file!**

Intended to be run on a regular basis, this script should index (CDX and Solr) all those WARCs created by the jobs indicated in every message on the `index` queue:

1. Pull all the messages from the `index` queue.
2. Generate a list of WARCs to be indexed from the messages.
3. Submit a MapReduce job to create a CDX in `/wayback/cdx-index/` from the list of WARCs.
4. Submit a MapReduce job to update Solr from the list of WARCs, outputting OAI-PMH XML to HDFS in the process.

