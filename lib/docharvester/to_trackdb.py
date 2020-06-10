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

def to_json():
    for de in os.scandir('/mnt/gluster/ingest/task-state/documents/'):
        if de.name.startswith('documents-') and de.is_dir():
            for dee in os.scandir(de):
                if dee.is_file():
                    with open(dee.path,'rb') as f:
                        item = json.load(f)
                        print(json.dumps(item))

