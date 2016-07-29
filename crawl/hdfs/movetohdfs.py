#!/usr/bin/env python
'''
Description:	Script to move files to HDFS
Author:		Gil
Date:		2016 April 26
'''

# libraries ---------
import os
import sys
import time
import re
import logging
import argparse
import string
import subprocess
from datetime import datetime, timedelta
from hdfs import InsecureClient
import hashlib

# variables ---------
logger = ()
args = ()
patternRegex = ''


# functions ---------
def script_die(msg):
    logger.error(msg)
    logger.error("Script died")
    sys.exit(1)


def setup_logging():
    # set handler
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
    handler.setFormatter(formatter)

    # attach to root logger
    logging.root.addHandler(handler)

    # set logging levels
    global logger
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)


def get_args():
    parser = argparse.ArgumentParser('Copy discovered files to HDFS')
    parser.add_argument('--webhdfs-prefix', dest='webhdfs_prefix', default='http://hdfs.gtw.wa.bl.uk:14000',
                        help="WebHDFS prefix to use [default: %(default)s")
    parser.add_argument('--webhdfs-user', dest='webhdfs_user', default='root',
                        help="WebHDFS user to use [default: %(default)s")
    parser.add_argument('--dir', dest='dir', type=str, default='/heritrix/output/warcs/',
                        help="Directory path to recursively traverse for files [default: %(default)s]")
    parser.add_argument('--pattern', dest='pattern', type=str, help="File pattern to move [no default]")
    parser.add_argument('--startdate', dest='startDate', type=str,
                        help="Start timestamp to search from, in YYYY-MM-DD:HH.MM.SS format [no default]")
    parser.add_argument('--enddate', dest='endDate', help="Optional end timestamp to search up to [no default]")
    parser.add_argument('--hdfsdir', dest='hdfsDir', help="HDFS directory prefix [no default]")

    # parse arguments
    global args
    args = parser.parse_args()

    # test arguments
    if not os.path.exists(args.dir):
        script_die("Directory argument [%s] doesn't exist" % args.dir)

    if not args.pattern:
        script_die("Pattern argument must be supplied")

    if args.hdfsDir is None:
        script_die("HDFS directory prefix must be supplied")

    if args.startDate:
        try:
            args.startDate = datetime.strptime(args.startDate, "%Y-%m-%d:%H.%M.%S")
        except ValueError:
            script_die("Start date string not in correct YYYY-MM-DD:HH.MM.SS format [%s]" % args.startDate)
    else:
        args.startDate = datetime.now() - timedelta(hours=24)

    if args.endDate:
        try:
            args.endDate = datetime.strptime(args.endDate, "%Y-%m-%d:%H.%M.%S")
        except ValueError:
            script_die("End date string not in correct YYYY-MM-DD:HH.MM.SS format [%s]" % args.endDate)
    else:
        args.endDate = datetime.now() - timedelta(minutes=1)

    if args.startDate >= args.endDate:
        script_die("Start date must be earlier than end date [start: %s, end: %s]" % (args.startDate, args.endDate))

    # construct regex pattern
    global patternRegex
    patternRegex = r"/" + args.pattern + r"$"

    # remove trailing / from directory argument
    if re.search(r'/$', args.dir):
        args.dir = args.dir[:-1]

    # dump arguments
    logger.info("==== Start ==== ==== ====")
    logger.info("args: %s" % args)
    logger.info("patternRegex: %s" % patternRegex)


def get_checksum(fpFile, on_hdfs=False, hdfsClient=None):
    # get hash for local or hdfs file
    if on_hdfs:
        with hdfsClient.read(hdfs_path=fpFile) as reader:
            hdfsStream = reader.read()
        fileHash = hashlib.sha512(hdfsStream).hexdigest()
    else:
        getHash = 'sha512sum -b "%s"' % fpFile
        runScript = subprocess.Popen(getHash, stdout=subprocess.PIPE, shell=True)
        out, err = runScript.communicate()
        if err:
            script_die("Failed to gain %s hash [%s]" % (fpFile, err))
        fileHash = out.split(" ")[0]

    # test hash
    logger.debug("file %s hash %s" % (fpFile, fileHash))
    if len(fileHash) != 128:
        script_die("%s hash not 128 character length [%s]" % (fpFile, len(fileHash)))
    if not all(c in string.hexdigits for c in fileHash):
        script_die("%s hash not all hex [%s]" % (fpFile, fileHash))

    return fileHash

class Uploader():
    """
    Initialise and set-up the HDFS connection:
    """
    def __init__(self, hadoop_url, hadoop_user):
        # Set up client:
        self.hdfsClient = InsecureClient(hadoop_url, user=hadoop_user)

    def write_hash_file(self, path, hash, on_hdfs=False):
        if on_hdfs:
            raise Exception("Writing hash to HDFS not supported yet.")
        else:
            hash_path = "%s.sha512" % path
            if( os.path.exists(hash_path)):
                logger.warning("Hash file %s already exists." % hash_path)
            else:
                with open(hash_path, 'w') as hash_file:
                    hash_file.write("%s\n" % hash)

    def safe_upload(self, localFile, hdfsFile, removeLocal=True):
        """
        This performs a safe upload - it will never overwrite a file on HDFS, and it uses checksums to verify the transfer.

        :param localFile:
        :param hdfsFile:
        :return:
        """

        # get local file hash and size
        localHash = get_checksum(localFile)
        self.write_hash_file(localFile,localHash)
        localSize = os.path.getsize(localFile)
        localModtime = datetime.fromtimestamp(os.path.getmtime(localFile))

        # store checksum as a local file:

        # upload file to HDFS if not already existing
        hdfsFileStatus = self.hdfsClient.status(hdfsFile, strict=False)
        if hdfsFileStatus == None:
            logger.info('---- ----')
            logger.info("Copying %s to HDFS %s" % (localFile, hdfsFile))
            logger.info("localFile size %i hash %s date %s" % (localSize, localHash, localModtime))
            with open(localFile,'r') as f:
                self.hdfsClient.write(data=f, hdfs_path=hdfsFile, overwrite=False)
            time.sleep(1)
            hdfsFileStatus = self.hdfsClient.status(hdfsFile, strict=False)

        # test if local and HDFS same
        if localSize != hdfsFileStatus['length']:
            logger.error("hdfsFile %s size differs %i, %s size %i" % (
                hdfsFile, hdfsFileStatus['length'], localFile, localSize))

        else:
            hdfsHash = get_checksum(hdfsFile, on_hdfs=True, hdfsClient=self.hdfsClient)
            if localHash != hdfsHash:
                logger.debug("hdfsFile %s hash differs %s, %s hash %s" % (hdfsFile, hdfsHash, localFile, localHash))

            else:
                # if uploaded HDFS file hash same as local file hash, delete local file
                logger.info("hdfsFile size %i hash %s" % (hdfsFileStatus['length'], hdfsHash))
                logger.info("Deleting %s" % localFile)
                os.remove(localFile)
        time.sleep(1)


# main --------------
def main():
    setup_logging()
    get_args()

    upl = Uploader(args.webhdfs_prefix, args.webhdfs_user)

    # traverse directory argument
    for dirName, subdirList, fileList in os.walk(args.dir):

        # for each file in scope to be copied
        for localFn in fileList:
            logger.debug("Looking at [%s]/[%s]" % (dirName, localFn))
            localFile = "%s/%s" % (dirName, localFn)
            hdfsFile = args.hdfsDir + localFile
            if not re.search(patternRegex, localFile):
                logger.debug("Not pattern match, skipping localFile: %s" % localFile)
                continue

            # check within start/end date
            localModtime = datetime.fromtimestamp(os.path.getmtime(localFile))
            if localModtime < args.startDate or localModtime > args.endDate:
                logger.info("Out of date range, skipping localFile: %s [%s]" % (localFile, localModtime))
                continue

            upl.safe_upload(localFile=localFile,hdfsFile=hdfsFile, removeLocal=True)

    logger.info("==== Stop  ==== ==== ====")

if __name__ == "__main__":
    main()
