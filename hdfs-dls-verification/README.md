HDFS - DLS Verification
=======================

Base of code for comparing content on HDFS with content on DLS.

*gilh-ids.py*
Traverses a HDFS directory (specified inside the script) for tar.gz files. Assumes such files are SIPS so does necessary processing to extract data; outputs following warc file information:
- mimetype
- checksum
- checksum_type
- (HDFS) path
- ark
- size

Remember that the ARK ID identifies the warc file in DLS. To extract a file inside the warc, the offset must be used, which is documented inside the CDX record.
