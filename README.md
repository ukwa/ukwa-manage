python-hdfslogs
===============

Script to synchronise log directories between the local filesystem and HDFS.

Currently this is configured to store all logs in a subdirectory of hdfs:///logs/,
named after the local hostname. Logs are stored under this location, mirroring
their local location, e.g.:

    /logs/opera.bl.uk/opt/tomcat/logs/catalina.out

NB: This requires that the subdirectory already exists: the script will not 
attempt to create it!

Prerequisites & Installation
----------------------------

This depends on the python-webhdfs package:

    git clone https://github.com/PsypherPunk/python-webhdfs.git
    pip install ./python-webhdfs/

Once this is installed, installation follows in a similar fashion:

    git clone gitlab@git.wa.bl.uk:/repos/products/python-hdfslogs.git

At this point you can edit the python-hdfslogs/hdfslogs/settings.py file to
include additional directories; paths are stored as a comma-separated list:

    directories="/opt/tomcat/logs,/var/log/httpd"

Once updated, install via Pip:

    pip install ./python-hdfslogs/

