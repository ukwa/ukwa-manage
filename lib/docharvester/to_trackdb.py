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
import logging
import psycopg2
import psycopg2.extras

DOC_FILES_PATH = '/mnt/gluster/ingest/task-state/documents/'

logger = logging.getLogger(__name__)

def doc_generator(path):
    counter = 0
    for de in os.scandir(DOC_FILES_PATH):
        if de.name.startswith('documents-') and de.is_dir():
            for dee in os.scandir(de):
                if dee.is_file():
                    with open(dee.path,'rb') as f:
                        item = json.load(f)
                        # Make sure any missing keys are explicitly null:
                        for key in ['title', 'target_id']:
                            if not key in item:
                                item[key] = None
                        # Log progress
                        counter += 1
                        if counter%1000 == 0:
                            print(f"Sent {counter}...")
                        # And yield:
                        yield item


class DocumentHarvester():

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
        ) ON CONFLICT DO NOTHING;
    """

    docs = []
    docs_sent = 0

    def files_to_json(self, path=DOC_FILES_PATH):
        for item in doc_generator(path):
            print(json.dumps(item))

    def files_to_db(self, path=DOC_FILES_PATH):
        self.send(doc_generator(path))

    def add_document(self, item):
        logger.info("Got document: %s" % item)
        if not 'status' in item:
            item['status'] = 'NEW'
        for key in ['title', 'target_id']:
            if not key in item:
                item[key] = None        
        logger.info("Updated document with defaults: %s" % item)
        self.docs.append(item)

    def flush_added_documents(self):
        logger.info(f"Sending {len(self.docs)} documents...")
        self._send(self.docs)
        self.docs_sent += len(self.docs)
        self.docs = []
        logger.info(f"Total documents sent: {self.docs_sent}")

    def _send(self, item_generator):
        # Open connection:
        connection = psycopg2.connect(
            host="dev1.n45.wa.bl.uk",
            port="5435",
            database="ddhapt",
            user="ddhapt",
            password="ddhapt",
        )
        connection.autocommit = True

        # Send all the documents in large batches:
        with connection.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, self.sql, item_generator, page_size=1000)

        # Commit at the end:
        connection.commit()


if __name__ == '__main__':
    dh = DocumentHarvester()
    dh.files_to_db()