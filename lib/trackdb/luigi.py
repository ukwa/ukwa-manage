import os
import luigi
from lib.trackdb.solr import SolrTrackDB

DEFAULT_TRACKDB = os.environ.get("TRACKDB_URL","http://trackdb.dapi.wa.bl.uk/solr/tracking")

class TrackingDBTaskTarget(luigi.Target):

    def __init__(
        self, task_id, field, value, trackdb=None
    ):
        """
        Args:
            task_id (str): The ID of the task to update
            field (str): The field to use to record the status
            value (str): The value the field should hold to indicate task completion
            trackdb (str): URL of the Solr tracking database (optional)
        """
        self.doc_id = "task:%s" % task_id
        self.field = field
        self.value = value

        # Setup connection:
        trackdb = trackdb or self.DEFAULT_TRACKDB
        self.tdb = SolrTrackDB(DEFAULT_TRACKDB, kind="tasks")

    def exists(self):
        result = self.tdb.get(self.task_id)
        if result:
            if result[self.field] != self.value:
                return True
        return False

    def touch(self):
        self.tdb.update(self.task_id, self.field, self.value, action='set')

    def open(self, mode):
        raise NotImplementedError("Cannot open() TrackingDBStatusField")
