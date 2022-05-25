import os
import boto3
import json

session = boto3.Session( 
         aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
         aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

#Then use the session to get the resource
s3 = session.resource('s3')

for bucket in s3.buckets.all():
    #print(f'  {bucket}')

    if bucket.name.startswith('ukwa-dc'):

        for o in bucket.objects.all():
            item = {
              "id": f"s3a://{o.bucket_name}/{o.key}",
              "file_path_s": f"/{o.key}",
              "file_size_l": o.size,
              "modified_at_dt": o.last_modified.isoformat(),
              "storage_service_id_s": "aws-s3",
              "aws_bucket_name": o.bucket_name,
              "aws_storage_class_s": o.storage_class,
              "aws_checksum_algorithm_s": o.checksum_algorithm,
              "aws_e_tag_s": o.e_tag.strip('"'),
            }
            if o.owner.get('DisplayName', None):
                item['owner'] = o.owner['DisplayName']
            else:
                item['owner'] = f"id:{o.owner['ID']}"
            print(json.dumps(item))
