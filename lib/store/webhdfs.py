'''
File storage back-end based on WebHDFS

Uses https://hdfscli.readthedocs.io
'''

import string
import logging
import hashlib
import time
from hdfs import InsecureClient

DEFAULT_WEBHDFS = "http://hdfs.api.wa.bl.uk/"
DEFAULT_WEBHDFS_USER = "access"

logger = logging.getLogger(__name__)

def check_sha512_hash(path, file_hash):
    '''
    Utility funtion to check if a hash is well-formed.
    '''
    logger.debug("Checking file %s hash %s" % (path, file_hash))
    if len(file_hash) != 128:
        raise Exception("%s hash not 128 character length [%s]" % (path, len(file_hash)))
    if not all(c in string.hexdigits for c in file_hash):
        raise Exception("%s hash not all hex [%s]" % (path, file_hash))

def calculate_sha512_local(path):
    with open(path, 'rb') as reader:
        file_hash = calculate_reader_hash(reader, path)

    return file_hash

def calculate_reader_hash(reader, path="unknown-path"):
    """
    Reads a file-like object in chunks, building up the SHA512 hash.

    :param reader: A file-like object that allows the data to be read
    :param path: The path of this file-like object, for reporting purposes
    :return:
    """
    sha = hashlib.sha512()
    while True:
        data = reader.read(10485760)
        if not data:
            reader.close()
            break
        sha.update(data)
    path_hash = sha.hexdigest()

    # check hash is not obviously wrong:
    check_sha512_hash(path, path_hash)

    # return it:
    return path_hash


class WebHDFSStore(object):
    '''
    A file store based on the WebHDFS protocol.
    '''

    def __init__(self, webhdfs_url = DEFAULT_WEBHDFS, webhdfs_user = DEFAULT_WEBHDFS_USER):
        self.webhdfs_url = webhdfs_url
        self.webhdfs_user = webhdfs_user
        self.client = InsecureClient(self.webhdfs_url, self.webhdfs_user)

    def put(self, local_path, hdfs_path):
        """
        Copy up to HDFS, making it suitably atomic by using a temporary filename during upload.

        :return: None
        """

        # Calculate hash of local file:
        local_hash = calculate_sha512_local(local_path)
        logger.warning("Local %s hash is %s " % (local_path, local_hash))

        #
        # TODO Allow upload  to overwrite truncated files?
        #
        
        # Check if the destination file exists and raise an exception if so:
        if self.exists(hdfs_path):
            raise Exception("Path %s already exists! This should never happen!" % hdfs_path)

        # Create the temporary file name:
        tmp_path = "%s.temp" % hdfs_path

        # Now upload the file, allowing overwrites as this is a temporary file and
        # simultanous updates should not be possible:
        logger.warning("Uploading as %s" % tmp_path)
        with open(local_path, 'rb') as reader, self.client.write(tmp_path, overwrite=True) as writer:
            while True:
                data = reader.read(10485760)
                if not data:
                    break
                writer.write(data)

        # Move the uploaded file into the right place:
        logger.warning("Renaming %s to %s..."% (tmp_path, hdfs_path))
        self.client.rename(tmp_path, hdfs_path)

        # Give the namenode a moment to catch-up with itself and then check it's there:
        # FIXME I suspect this is only needed for our ancient HDFS
        time.sleep(2)
        status = self.client.status(hdfs_path)

        hdfs_hash = self.calculate_sha512(hdfs_path)
        logger.warning("HDFS %s hash is %s " % (hdfs_path, hdfs_hash))
        if local_hash != hdfs_hash:
            raise Exception("Local & HDFS hashes do not match for %s" % self.path)
        else:
            logger.warning("Hashes are equal!")

        # Log successful upload:
        logger.warning("Upload completed for %s" % hdfs_path)

    def move(self, local_path, hdfs_path):
        # Perform the PUT
        self.put(local_path,hdfs_path)
        # And delete the local file:
        os.remove(local_path)

    def calculate_sha512(self, path):
        '''
        Calculate the SHA512 hash of a single file on HDFS
        '''
        with self.client.read(path) as reader:
            file_hash = calculate_reader_hash(reader, path)
        
        return file_hash

    def list(self, path):
        return self.client.list(path)
    
    def exists(self, path):
        status = self.client.status(path, strict=False)
        print(status)
        if status:
            return True
        else:
            return False