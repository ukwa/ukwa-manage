python-hdfslogs
======================

Script to synchronise log directories between the local filesystem and HDFS.

Currently this is configured to store all logs in a subdirectory of hdfs:///logs/,
named after the local hostname. Logs are stored under this location, mirroring
their local location, e.g.:

    /logs/opera.bl.uk/opt/tomcat/logs/catalina.out

NB: This requires that the subdirectory already exists: the script will not 
attempt to create it!

