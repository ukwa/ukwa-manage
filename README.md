python-sip-verification
---------------

Various SIPs are stored in gzipped tarfiles in HDFS. This script will pull a single message from a queue, verify that the corresponding SIP exists in HDFS, retrieve it, parse the METS file for ARK identifiers and verify that each ARK is available in DLS. If so, the corresponding SIP is flagged for indexing.

