import os
import sys
import subprocess
import string
from hdfs import InsecureClient

client = InsecureClient('http://dls.httpfs.wa.bl.uk:14000')

def getMD5hash(path, on_hadoop=False):
    if on_hadoop:
        cmd = 'hadoop fs -cat "%s" | md5sum -b' % path
    else:
        cmd = 'md5sum -b "%s"' % path
    print "Getting hash via $ %s" % cmd
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if err:
        print "ERROR on command! "+err
	sys.exit(1)
    hash = out.split(" ")[0]
    if len(hash) != 32:
        print "ERROR hash is wrong length! "+out
	sys.exit(1)
    if not all(c in string.hexdigits for c in hash):
        print "ERROR hash is not hex! "+out
	sys.exit(1)
    return hash

def checkDir(dirPath):
    for dirName, subdirList, fileList in os.walk(dirPath):
        for fname in fileList:
            filePath = '%s/%s' % (dirName, fname)
            print "\nNEXT: %s" % filePath
            if filePath.startswith('/heritrix/output/logs/') or filePath.endswith('.warc.gz.open'):
                print "SKIPPING %s" % filePath
                continue
            status = client.status(filePath, strict=False)
            if status == None:
                print "MISSING FROM HDFS: "+filePath
            else:
                length = os.path.getsize(filePath)
                if length == status['length']:
                    print "OK by size (%i,%i): %s" % (length,status['length'],filePath)
                    lh = getMD5hash(filePath)
                    rh = getMD5hash(filePath,on_hadoop=True)
                    if lh != rh:
                        print "FILE ON HDFS HAS DIFFERENT MD5 HASH: %s %s %s" % (filePath, lh, rh)
                    else:
                        print "OK by MD5 hash (%s,%s): %s" %( lh, rh, filePath )
                else:
                    print "FILE ON HDFS IS DIFFERENT SIZE: %i %i %s" % (length, status['length'], filePath)

#print getMD5hash('/heritrix/output/images/BL-20151001184440404166-1.warc.gz')
#print getMD5hash('/heritrix/output/images/BL-20151001184440404166-1.warc.gz',on_hadoop=True)

checkDir('/heritrix/output/warcs')


