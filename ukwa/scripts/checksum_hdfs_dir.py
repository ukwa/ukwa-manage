from __future__ import absolute_import

import hdfs
import posixpath
import hashlib
import logging
import sys
import time
from queue import Queue
from threading import Thread

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


def calculateHash( path, client=None ):
    if client is None:
        client = hdfs.InsecureClient('http://hdfs.gtw.wa.bl.uk:14000', user='hdfs')
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


def generate_checksums(q, results):
    while True:
        try:
            w = HDFSFile(q.get())
            results.append(w)
            q.task_done()
        except Exception:
            break
    logger.info("Worker exiting...")


class HDFSFile:
    def __init__(self, path):
        self.path = path
        self.hash = None
        try:
            self.hash = calculateHash(path)
        except Exception as e:
            logger.error("Failed to calculate hash for %s" % path)
        logger.debug("%s %s" % (self.path, self.hash))


def checksum_dir(src, local_manifest, num_threads=10):
    """
    Walk the source folder, and compute the SHA512

    Args:
        src:

    Returns:

    """

    known = {}
    logger.info("Opening up %s" % local_manifest)
    with open(local_manifest,'r') as f:
        for line in f:
            (sha512,path) = line.split(' ',1)
            path = path.lstrip('*')
            path = path.rstrip()
            known[path] = sha512
    logger.info("Loaded %i known hashes." % len(known))

    client = hdfs.InsecureClient('http://hdfs.gtw.wa.bl.uk:14000', user='hdfs')
    #client = hdfs.InsecureClient('http://dls.httpfs.wa.bl.uk:14000', user='hdfs')

    wq = Queue()

    print("Scanning %s" % src)
    sames = 0
    misses = 0
    for (path, dirs, files) in client.walk(src):
        # Loop through the files:
        i = 0
        for file in files:
            srcpath = posixpath.join(path,file)
            if srcpath in known:
                logger.info("Hash for %s is already known." % srcpath)
                continue
            srcstatus = client.status(srcpath)
            srchash = client.checksum(srcpath)
            if len(srchash['bytes']) != 64 or srchash['bytes'] == bytearray(64):
                raise Exception("Got nonsense hash %s" % srchash)
            logger.info("Queueing %s" %(srcpath))
            wq.put(srcpath)
            break

    logger.info("Launching %i workers to process %i files..." % (num_threads, wq.qsize()) )
    results = []
    for i in range(num_threads):
        worker = Thread(target=generate_checksums, args=(wq, results))
        worker.setDaemon(True)
        worker.start()

    # Wait for the queue to be processed:
    wq.join()

    logger.info("Appending results to %s" % local_manifest)
    for r in results:
        if r.hash:
            with open(local_manifest, 'a') as f:
                print("%s *%s" % (r.hash, r.path))


if __name__ == "__main__":
    hdfs_dir = "/heritrix/output/warcs/dc0-20150827"
    local_manifest = "../dc0-20150827.manifest.sha512"
    checksum_dir(hdfs_dir, local_manifest, 10)



