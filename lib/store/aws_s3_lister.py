import os
import boto3
import json
import datetime
from lib.store.hdfs_layout import HdfsPathParser

#
# This works a bit like the webhdfs.py class, but scans and classifies holdings from AWS
#

session = boto3.Session( 
         aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
         aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

#Then use the session to get the resource
s3 = session.resource('s3')

# Date
refresh_date = datetime.datetime.utcnow().isoformat(timespec='milliseconds')+'Z'

for bucket in s3.buckets.all():
#    print(f'  {bucket}')

    if bucket.name.startswith('ukwa-dc'):

        # Lookup region
        region = boto3.client('s3').get_bucket_location(Bucket=bucket.name)['LocationConstraint']

        for o in bucket.objects.all():
            # Add the file path:
            status = { 
                "file_path": f"/{o.key}",
                "file_size": o.size,
                "permission": "-",
                "replication": 1,
                "length": o.size,
                "modificationTime": o.last_modified.timestamp() * 1000,
            }
            if o.owner.get('DisplayName', None):
                status['owner'] = o.owner['DisplayName']
                status['group'] = ''
            else:
                status['owner'] = f"id:{o.owner['ID']}"
                status['group'] = ''
            # Classify based on HDFS storage conventions:
            item = HdfsPathParser(status).to_dict()
            fs_type = 'file'
            access_url = f'http://{o.bucket_name}.s3-website.{region}.amazonaws.com/{o.key}'
            aws_date = o.last_modified.isoformat().replace("+00:00",".000Z")
            # And return as a 'standard' dict:
            item = {
                "id": f"s3a://{o.bucket_name}/{o.key}",
                    'refresh_date_dt': refresh_date,
                    'file_path_s': item['file_path'],
                    'file_size_l': item['file_size'],
                    'file_ext_s': item['file_ext'],
                    'file_name_s': item['file_name'],
                    'permissions_s': item['permissions'],
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
                    'hdfs_service_id_s': "aws_s3",
                    'hdfs_type_s': fs_type,
                    'access_url_s': access_url,
                    "aws_bucket_name_s": o.bucket_name,
                    "aws_storage_class_s": o.storage_class,
                    "aws_checksum_algorithm_s": o.checksum_algorithm,
                    "aws_e_tag_s": o.e_tag.strip('"'),
                    "aws_region_s": region,
                    "aws_modified_at_dt": f"{aws_date}",
                }
            print(json.dumps(item))
