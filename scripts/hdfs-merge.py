import hdfs
import posixpath
import hashlib
import logging
import time
from crawl.hdfs.movetohdfs import get_checksum

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


def scan_for_merge(src, dest, dummy_run=True):
    """
    Walk the source folder, and compare the contents with the destination folder.

    Optionally, move all non-conflicting files from src to dest.

    Args:
        src:
        dest:

    Returns:

    """

    #client = hdfs.InsecureClient('http://hdfs.gtw.wa.bl.uk:14000', user='hdfs')
    client = hdfs.InsecureClient('http://dls.httpfs.wa.bl.uk:14000', user='hdfs')

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

            targetpath = posixpath.join(dest,file)

            print("Comparing %s to %s (%i/%i)" % (srcpath, targetpath, i+1, len(files)))
            targetstatus = client.status(targetpath, strict=False)
            if targetstatus:
                logger.debug("Path %s already exists." % targetpath)
                logger.debug("Source size is: %i" % srcstatus['length'])
                logger.debug("Target size is: %i" % targetstatus['length'])
                if srcstatus['length'] != targetstatus['length']:
                    raise Exception("Two versions of different lengths! %s %s " % (srcpath, targetpath))
                targethash = client.checksum(targetpath)
                logger.debug(srchash, targethash)
                if srchash['bytes'] != targethash['bytes']:
                    raise Exception("Two versions of different hashes! %s %s " % (srcpath, targetpath))
                if dummy_run:
                    print("Could remove %s (%s)" % (srcpath, srchash))
                else:
                    print("Removing %s (%s)" % (srcpath, srchash))
                    client.delete(srcpath)
                sames += 1
            else:
                if dummy_run:
                    print("Could move %s to %s" % (srcpath,targetpath))
                else:
                    print("Moving %s to %s" % (srcpath,targetpath))
                    client.rename(srcpath,targetpath)
                misses = misses + 1
            i += 1
    print("%i files appear to be the same." % sames)
    print("%i files are only in the source folder." % misses)

if __name__ == "__main__":
    dummy=False
    scan_for_merge("/0_original/dc/crawler04/heritrix/output/images", "/heritrix/output/images", dummy)
    # scan_for_merge("/0_original/dc/crawler04/heritrix/output/viral/dc0-20150827", "/heritrix/output/viral/dc0-20150827", dummy)
    # scan_for_merge("/0_original/dc/crawler04/heritrix/output/viral/dc1-20150827", "/heritrix/output/viral/dc1-20150827", dummy)
    # scan_for_merge("/0_original/dc/crawler04/heritrix/output/viral/dc2-20150827", "/heritrix/output/viral/dc2-20150827",
    #            dummy)
    # scan_for_merge("/0_original/dc/crawler04/heritrix/output/viral/dc3-20150827", "/heritrix/output/viral/dc3-20150827",
    #            dummy)

