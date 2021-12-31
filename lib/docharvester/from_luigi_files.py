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
from lib.docharvester.find import DocumentsFoundDB


logger = logging.getLogger(__name__)

def _doc_generator(path):
    counter = 0
    for de in os.scandir(path):
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
                            logger.info(f"Sent {counter}...")
                        # And yield:
                        yield item

class LuigiStateScanner():

    def __enter__(self, db_uri):
        # Set up target DB
        self.dh = DocumentsFoundDB(db_uri=db_uri)

        return self

    def scan(self, path):
        for item in _doc_generator(path):
            #print(json.dumps(item))
            self.dh.add_document(item)

    def __exit__(self, exc_type, exc_value, traceback):
        self.dh.flush_added_documents()
        stats = {}
        stats["total_sent_records_i"] = self.dh.docs_sent
        print(stats)

    


if __name__ == '__main__':
    with LuigiStateScanner() as lss:
        lss.scan('/mnt/gluster/ingest/task-state/documents/')