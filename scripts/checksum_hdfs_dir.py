from __future__ import absolute_import

import hdfs
import posixpath
import hashlib
import logging
import time

# set handler
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
handler.setFormatter(formatter)

# attach to root logger
logging.root.addHandler(handler)

# set logging levels
logging.basicConfig(level=logging.WARNING)
logging.root.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def calculateHash( path, client ):
    logger.info("Starting to generate hash for %s" % path)
    sha = hashlib.sha512()
    with client.read(path) as file:
        while True:
            data = file.read( 10485760 )
            if not data:
                file.close()
                break
            sha.update( data )
    logger.info("Finished generating hash for %s" % path)
    return sha.hexdigest()


def checksum_dir(src):
    """
    Walk the source folder, and compute the SHA512

    Args:
        src:

    Returns:

    """

    client = hdfs.InsecureClient('http://hdfs.gtw.wa.bl.uk:14000', user='hdfs')
    #client = hdfs.InsecureClient('http://dls.httpfs.wa.bl.uk:14000', user='hdfs')

    print("Scanning %s" % src)
    sames = 0
    misses = 0
    for (path, dirs, files) in client.walk(src):
        # Loop through the files:
        i = 0
        for file in files:
            srcpath = posixpath.join(path,file)
            srcstatus = client.status(srcpath)
            srchash = client.checksum(srcpath)
            if len(srchash['bytes']) != 64 or srchash['bytes'] == bytearray(64):
                raise Exception("Got nonsense hash %s" % srchash)
            srcsha = calculateHash(srcpath,client=client)
            logger.info("%s *%s" %(srcsha,srcpath))


if __name__ == "__main__":
    dummy=False
    checksum_dir("/heritrix/output/warcs/dc0-20150827")



