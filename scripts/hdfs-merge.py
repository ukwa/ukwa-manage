import hdfs
import posixpath

def scan_for_merge(src, dest, merge=False):
    """
    Walk the source folder, and compare the contents with the destination folder.

    Optionally, move all non-conflicting files from src to dest.

    Args:
        src:
        dest:

    Returns:

    """

    client = hdfs.InsecureClient('http://hdfs.gtw.wa.bl.uk:14000', user='hdfs')

    print("Scanning %s" % src)
    sames = 0
    misses = 0
    for (path, dirs, files) in client.walk(src):
        print(path, dirs, files)
        for file in files:
            fullpath = posixpath.join(path,file)
            srcstatus = client.status(fullpath)
            targetpath = posixpath.join(dest,file)
            print("---\nComparing %s to %s " % (fullpath, targetpath))
            targetstatus = client.status(targetpath, strict=False)
            if targetstatus:
                print("Path %s already exists." % targetpath)
                print("Source size is: %i" % srcstatus['length'])
                print("Target size is: %i" % targetstatus['length'])
                if srcstatus['length'] != targetstatus['length']:
                    raise Exception("Two versions of different lengths! %s %s " % (fullpath, targetpath))
                sames = sames + 1
            else:
                print("Path %s does not exist." % targetpath)
                misses = misses + 1
    print("%i files appear to be the same." % sames)
    print("%i files are only in the source folder." % misses)

if __name__ == "__main__":
    print("GO")
    scan_for_merge("/0_original/dc/crawler04/heritrix/output/warcs/dc0-20150827", "/heritrix/output/warcs/dc0-20150827")

