'''
This script is for migrating the file-based document harvester records to the Tracking Database.

The file-based harvester uses thousands of files with names based on hashed URLs, like this: 
/mnt/gluster/ingest/task-state/documents/documents-gov.wales/000b4b5b2c3d904d7d2cedd94a78803d

```
{
    "wayback_timestamp": "20191016131833",
    "filename": "15969_3_e.pdf",
    "source": "https://careinspectorate.wales/our-reports",
    "landing_page_url": "https://careinspectorate.wales/docs/cssiw/report/inspection_reports/15969_3_e.pdf",
    "document_url": "https://gov.wales/docs/cssiw/report/inspection_reports/15969_3_e.pdf",
    "launch_id": "20191014080209",
    "job_name": "weekly",
    "size": 177614,
    "title": "",
    "target_id": 92551,
    "status": "ACCEPTED"
}
```

As files on disk these are hard to manage, but they are a good fit for Solr/TrackDB.

For TrackDB, they need an ID and a `kind`, we can manage them properly. The ID should be `document:<URL>` 
and the kind just `document`.

The implementation can be moved to TrackDB, which can then use the `status` flag to track processing.

'''

import json
import os
import psycopg2
import psycopg2.extras

DOC_FILES_PATH = '/mnt/gluster/ingest/task-state/documents/'


def doc_generator(path):
    for de in os.scandir(DOC_FILES_PATH):
        if de.name.startswith('documents-') and de.is_dir():
            for dee in os.scandir(de):
                if dee.is_file():
                    with open(dee.path,'rb') as f:
                        item = json.load(f)
                        yield item


def to_json():
    for item in doc_generator(DOC_FILES_PATH):
        print(json.dumps(item))


def to_db():

    connection = psycopg2.connect(
        host="dev1.n45.wa.bl.uk",
        port="5435",
        database="ddhapt",
        user="ddhapt",
        password="ddhapt",
    )

    sql = """
        INSERT INTO documents_found (
            document_url, 
            wayback_timestamp,
            filename,
            source,
            landing_page_url,
            launch_id,
            job_name,
            size,
            title,
            target_id,
            status
        )
        VALUES (
            %(document_url)s,
            %(wayback_timestamp)s,
            %(filename)s,
            %(source)s,
            %(landing_page_url)s,
            %(launch_id)s,
            %(job_name)s,
            %(size)s,
            %(title)s,
            %(target_id)s,
            %(status)s
        )
    """

    with connection.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, sql, doc_generator(DOC_FILES_PATH), page_size=1000)
        cursor.commit()



if __name__ == '__main__':
    to_db()