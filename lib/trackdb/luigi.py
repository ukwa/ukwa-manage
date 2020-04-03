import os
import luigi
import pysolr

class TrackingDBStatusField(luigi.Target):

    DEFAULT_TRACKDB = os.environ.get('TRACKING_DB_SOLR_URL', 'http://localhost:8983/solr/tracking')

    def __init__(
        self, doc_id, field, value, trackdb=None
    ):
        """
        Args:
            doc_id (str): The ID of the document to update
            field (str): The field to use to record the status
            value (str): The value the field should hold to indicate task completion
            trackdb (str): URL of the Solr tracking database
        """
        self.doc_id = doc_id
        self.field = field
        self.value = value
        self.trackdb = trackdb or self.DEFAULT_TRACKDB

        # Setup connection:
        self.solr = pysolr.Solr(self.trackdb, always_commit=True)

    def exists(self):
        result = self.solr.search(q='id:"%s" AND %s:"%s"' % (self.doc_id, self.field, self.value))
        if result.hits == 0:
            return False
        else:
            return True

    def touch(self):
        doc = {'id': self.doc_id, self.field: self.value}
        result = self.solr.add([doc], fieldUpdates={self.field: 'set'}, softCommit=True)
        print(result)

    def open(self, mode):
        raise NotImplementedError("Cannot open() TrackingDBStatusField")


class TrackingDBTaskStatusField(luigi.Target):

    DEFAULT_TRACKDB = os.environ.get('TRACKING_DB_SOLR_URL', 'http://localhost:8983/solr/tracking')

    def __init__(
        self, task, field, value, trackdb=None
    ):
        """
        Args:
            task (luigi.Task): The task we are recording
            field (str): The field to use to record the status
            value (str): The value the field should hold to indicate task completion
            trackdb (str): URL of the Solr tracking database
        """
        self.doc_id = task.id
        self.field = field
        self.value = value
        self.trackdb = trackdb or self.DEFAULT_TRACKDB

        # Setup connection:
        self.solr = pysolr.Solr(self.trackdb, always_commit=True)

    def exists(self):
        result = self.solr.search(q='id:"%s" AND %s:"%s"' % (self.doc_id, self.field, self.value))
        if result.hits == 0:
            return False
        else:
            return True

    def touch(self):
        doc = {'id': self.doc_id, self.field: self.value}
        result = self.solr.add([doc], fieldUpdates={self.field: 'set'}, softCommit=True)
        print(result)

    def open(self, mode):
        raise NotImplementedError("Cannot open() TrackingDBStatusField")
