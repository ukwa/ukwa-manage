'''
File storage back-end based on WebHDFS

Uses https://hdfscli.readthedocs.io
'''

import os
import time
import string
import logging
import hashlib
import posixpath as psp
from datetime import datetime
from hdfs import InsecureClient

DEFAULT_WEBHDFS = "http://hdfs.api.wa.bl.uk/"
DEFAULT_WEBHDFS_USER = "access"

logger = logging.getLogger(__name__)

def permissions_octal_to_string(octal):
    result = ''
    for _ in range(3):
        octal, digit = divmod(octal, 10)
        for value, letter in reversed([(4, "r"), (2, "w"), (1, "x")]):
            result = (letter if digit & value else "-") + result
    return result

def ts_to_iso_date(t):
    return datetime.utcfromtimestamp(t).isoformat() + 'Z'

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
        # Get the status of the destination:
        dest_status = self.client.status(hdfs_path, strict=False)

        # Handle files or directories:
        if os.path.isfile(local_path):
            hdfs_path = self._combine_paths(dest_status, local_path, hdfs_path)
            self._upload_file(local_path, hdfs_path)
        elif os.path.isdir(local_path):
            # TODO, if it's a directory
            raise Exception("Cannot upload anything other than single files at this time!")
        else:
            raise Exception("Unknown path type! Can't handle %s" % local_path)

    def _combine_paths(self, dest_status, local_path, hdfs_path):
        # If the hdfs_path is a directory, combine the paths:
        if dest_status and dest_status['type'] == 'DIRECTORY':
            combined_path = psp.join(hdfs_path, local_path)
            logger.info("Using combined path: %s" % combined_path)
            return combined_path
        else:
            # Otherwise, just return the path:
            return hdfs_path

    def _upload_file(self, local_path, hdfs_path):
        """
        Copy up to HDFS, making it suitably atomic by using a temporary filename during upload.

        :return: None
        """

        # Set up flag to record outcome:
        success = False

        # Calculate hash of local file:
        logger.info("Calculating hash of %s" % local_path)
        if not os.path.isfile(local_path):
            raise Exception("Cannot upload %s - individual files only!")
        local_hash = calculate_sha512_local(local_path)
        logger.info("Local %s hash is %s " % (local_path, local_hash))

        #
        # TODO Allow upload  to overwrite truncated files?
        #

        # Check if the destination file exists and just perform hash-check if so:
        if self.exists(hdfs_path):
            logger.warning("Path %s already exists! No upload will be attempted." % hdfs_path)
        else:
            # Upload to a temporary path:
            tmp_path = "_%s_temp" % hdfs_path

            # Now upload the file, allowing overwrites as this is a temporary file and
            # simultanous updates should not be possible:
            logger.info("Uploading as %s" % tmp_path)
            with open(local_path, 'rb') as reader, self.client.write(tmp_path, overwrite=True) as writer:
                while True:
                    data = reader.read(10485760)
                    if not data:
                        break
                    writer.write(data)

            # Move the uploaded file into the right place:
            logger.info("Renaming %s to %s..."% (tmp_path, hdfs_path))
            self.client.rename(tmp_path, hdfs_path)

            # Give the namenode a moment to catch-up with itself and then check it's there:
            # FIXME I suspect this is only needed for our ancient HDFS
            time.sleep(2)
            status = self.client.status(hdfs_path)

        logger.info("Calculating hash of HDFS file %s" % hdfs_path)
        hdfs_hash = self.calculate_sha512(hdfs_path)
        logger.info("HDFS %s hash is %s " % (hdfs_path, hdfs_hash))
        if local_hash != hdfs_hash:
            raise Exception("Local & HDFS hashes do not match for %s" % self.path)
        else:
            logger.info("Hashes are equal!")
            success = True

        # Log successful upload:
        logger.warning("Upload completed for %s" % hdfs_path)

        # And return success flag so caller knows it worked:
        return success

    def move(self, local_path, hdfs_path):
        # Perform the PUT
        success = self.put(local_path,hdfs_path)
        # And delete the local file:
        if success == True:
            os.remove(local_path)

    def calculate_sha512(self, path):
        '''
        Calculate the SHA512 hash of a single file on HDFS
        '''
        with self.client.read(path) as reader:
            file_hash = calculate_reader_hash(reader, path)
        
        return file_hash

    @staticmethod
    def _to_info(path, status):
        # Work out the permissions string:
        permissions = permissions_octal_to_string(int(status['permission']))
        if status['type'] == 'DIRECTORY':
            permissions = "d" + permissions
        else:
            permissions = "-" + permissions
        # And return as a 'standard' dict:
        return {
            'filename': path,
            'type' : status['type'].lower(),
            'permissions' : permissions,
            'number_of_replicas': status['replication'],
            'userid': status['owner'],
            'groupid': status['group'],
            'filesize': status['length'],
            'modified_at': ts_to_iso_date(status['modificationTime']/1000),
            'accessed_at': ts_to_iso_date(status['accessTime']/1000),
            'block_size': status['blockSize']
        }
    
    def list(self, path, recursive=False):
        # Handle non-existant entry, or a file:
        path_status = self.client.status(path, strict=False)
        if path_status is None:
            raise Exception("No such file or directory: %s" % path)
        elif path_status['type'] == 'FILE':
            # Plain old file:
            yield self._to_info(path, path_status)
        else:
            # Handle folders:
            if recursive:
                for dir_info, dirs_info, files_info in self.client.walk(path, status=True):
                    dir_path, dir_status = dir_info
                    for file_name, file_status in files_info:
                        file_path = psp.join(dir_path, file_name)
                        yield self._to_info(file_path, file_status)
            else:
                for file_name, file_status in self.client.list(path, status=True):
                    file_path = psp.join(path, file_name)
                    yield self._to_info(file_path, file_status)
    
    def exists(self, path):
        status = self.client.status(path, strict=False)
        print(status)
        if status:
            return True
        else:
            return False

    def rm(self, path):
        # And delete from HDFS (usually prevented by API proxy)
        self.client.delete(path, recursive=False)