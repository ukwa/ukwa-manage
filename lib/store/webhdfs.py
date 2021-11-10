'''
File storage back-end based on WebHDFS

Uses https://hdfscli.readthedocs.io
'''

import os
import time
import string
import logging
import hashlib
import datetime
import posixpath as psp
from hdfs import InsecureClient
from lib.store.hdfs_layout import HdfsPathParser


# Define the Hadoop systems are we talking to:
HADOOPS = {
    'h020': {
        'id_prefix': 'hdfs://hdfs:54310',
        'webhdfs_url': 'http://hdfs.api.wa.bl.uk',
        'webhdfs_user': 'access'
    },
    'h3': {
        'id_prefix': 'hdfs://h3nn.wa.bl.uk:54310',
        'webhdfs_url': 'http://h3httpfs.api.wa.bl.uk',
        'webhdfs_user': 'spark'
    },
}

logger = logging.getLogger(__name__)

def permissions_octal_to_string(octal):
    result = ''
    for _ in range(3):
        octal, digit = divmod(octal, 10)
        for value, letter in reversed([(4, "r"), (2, "w"), (1, "x")]):
            result = (letter if digit & value else "-") + result
    return result

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


class WebHDFSStore():
    '''
    A file store based on the WebHDFS protocol.
    '''
    # Set a refresh-date to indicate when we did this lookup:
    refresh_date = datetime.datetime.utcnow().isoformat(timespec='milliseconds')+'Z'
    
    def __init__(self, service_id):
        self.service_id = service_id
        self.webhdfs_url = HADOOPS[service_id]['webhdfs_url']
        self.webhdfs_user = HADOOPS[service_id]['webhdfs_user']
        self.id_prefix = HADOOPS[service_id]['id_prefix']
        self.client = InsecureClient(self.webhdfs_url, self.webhdfs_user)

    def put(self, local_path, hdfs_path, backup_and_replace=False):
        # Get the status of the destination:
        dest_status = self.client.status(hdfs_path, strict=False)

        # Handle files or directories:
        if os.path.isfile(local_path):
            hdfs_path = self._combine_paths(dest_status, local_path, hdfs_path)
            self._upload_file(local_path, hdfs_path, backup_and_replace)
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

    def _upload_file(self, local_path, hdfs_path, backup_and_replace=False):
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

        # Check if the destination file exists:
        already_exists = self.exists(hdfs_path)
        if already_exists and not backup_and_replace:
            logger.warning("Path %s already exists! No upload will be attempted." % hdfs_path)
        else:
            # Upload to a temporary path:
            tmp_path = "%s_temp_" % hdfs_path

            # Now upload the file, allowing overwrites as this is a temporary file and
            # simultanous updates should not be possible:
            logger.info("Uploading as %s" % tmp_path)
            with open(local_path, 'rb') as reader, self.client.write(tmp_path, overwrite=True) as writer:
                while True:
                    data = reader.read(10485760)
                    if not data:
                        break
                    writer.write(data)
            
            # If set, backup-and-replace as needed:
            if backup_and_replace and already_exists:
                date_stamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                backup_path = "%s.bkp_%s" %( hdfs_path, date_stamp)
                logger.warning("Renaming %s to %s..."% (hdfs_path, backup_path))
                self.client.rename(hdfs_path, backup_path)

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
            raise Exception("Local & HDFS hashes do not match for %s" % local_path)
        else:
            logger.info("Hashes are equal!")
            success = True

        # Log successful upload:
        logger.warning("Upload completed for %s" % hdfs_path)

        # And return success flag so caller knows it worked:
        return success

    def move(self, local_path, hdfs_path):
        # Perform the PUT first:
        success = self.put(local_path,hdfs_path)
        # And delete the local file if that worked:
        if success == True:
            os.remove(local_path)

    def calculate_sha512(self, path):
        '''
        Calculate the SHA512 hash of a single file on HDFS
        '''
        with self.client.read(path) as reader:
            file_hash = calculate_reader_hash(reader, path)
        
        return file_hash

    def _to_info(self, path, status):
        # Add the file path:
        status['file_path'] = path
        # Classify based on HDFS storage conventions:
        item = HdfsPathParser(status).to_dict()
        # Work out the permissions string:
        if status['permission'].isnumeric():
            permissions = permissions_octal_to_string(int(status['permission']))
            if status['type'] == 'DIRECTORY':
                permissions = "d" + permissions
            else:
                permissions = "-" + permissions
        else:
            permissions = status['permission']
        # Defined fields based on directory/file status
        if permissions[0] == 'd':
            fs_type = 'directory'
            access_url = '%s/webhdfs/v1%s?op=LISTSTATUS&user.name=%s' %(self.webhdfs_url, item['file_path'], self.webhdfs_user)
        else:
            fs_type = 'file'
            access_url = '%s/webhdfs/v1%s?op=OPEN&user.name=%s' %(self.webhdfs_url, item['file_path'], self.webhdfs_user)
        # And return as a 'standard' dict:
        return {
                'id': '%s%s' % (self.id_prefix, item['file_path']),
                'refresh_date_dt': self.refresh_date,
                'file_path_s': item['file_path'],
                'file_size_l': item['file_size'],
                'file_ext_s': item['file_ext'],
                'file_name_s': item['file_name'],
                'permissions_s': permissions,
                'hdfs_replicas_i': item['number_of_replicas'],
                'hdfs_user_s': item['user_id'],
                'hdfs_group_s': item['group_id'],
                'modified_at_dt': "%sZ" % item['modified_at'],
                'timestamp_dt': "%sZ" % item['timestamp'],
                'year_i': item['timestamp'][0:4],
                'recognised_b': item['recognised'],
                'kind_s': item['kind'],
                'collection_s': item['collection'],
                'stream_s': item['stream'],
                'job_s': item['job'],
                'layout_s': item['layout'],
                'hdfs_service_id_s': self.service_id,
                'hdfs_type_s': fs_type,
                'access_url_s': access_url
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
        if status:
            return True
        else:
            return False

    def rm(self, path):
        # And delete from HDFS (usually prevented by API proxy)
        # Hard-coded to never act recursively - if you want that, do it manually via the back-end.
        self.client.delete(path, recursive=False)

    def stream(self, path, offset=0, length=None):
        # NOTE our WebHDFS service is very old and uses 'len' not 'length' for controlling the response length:
        # The API proxy we use attempts to remedy this by mapping any 'length' parameter to 'len'.
        return self.client.read(path, offset=offset, length=length)

    def read(self, path, offset=0, length=None):
        with self.stream(path,offset, length) as reader:
            while True:
                data = reader.read(10485760)
                if not data:
                    break
                yield data

    def lsr_to_items(self, reader):
        """
        This task processes a raw list of files generated by the hadoop fs -lsr command.

        As this can be a very large list, it avoids reading it all into memory. It
        parses each line, and yields a suitable stream of parsed objects matching the WebHDFS API.
        """
        for line in reader:
            if "lsr: DEPRECATED: Please use 'ls -R' instead." in line:
                logger.warning(line)
            else:
                permissions, number_of_replicas, userid, groupid, filesize, modification_date, modification_time, filename = line.split(None, 7)
                filename = filename.strip()
                timestamp = datetime.datetime.strptime('%s %s' % (modification_date, modification_time), '%Y-%m-%d %H:%M')
                info = {
                    'permission' : permissions,
                    'replication': number_of_replicas,
                    'owner': userid,
                    'group': groupid,
                    'length': filesize,
                    'modificationTime': timestamp.timestamp() * 1000,
                    'pathSuffix': filename
                }
                # Skip directories:
                if permissions[0] != 'd':
                    yield self._to_info(filename,info)
                    info['type'] = 'DIRECTORY'
                else:
                    info['type'] = 'FILE'