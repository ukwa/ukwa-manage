// groovy
// see org.archive.crawler.frontier.BdbMultipleWorkQueues.forAllPendingDo()
 
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
 
MAX_URLS_TO_LIST = 10000
 
pendingUris = job.crawlController.frontier.pendingUris
 
//rawOut.println "(this seems to be more of a ceiling) pendingUris.pendingUrisDB.count()=" + pendingUris.pendingUrisDB.count()
//rawOut.println()
 
cursor = pendingUris.pendingUrisDB.openCursor(null, null);
key = new DatabaseEntry();
value = new DatabaseEntry();
count = 0;
 
while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS && count < MAX_URLS_TO_LIST) {
    if (value.getData().length == 0) {
        continue;
    }
    curi = pendingUris.crawlUriBinding.entryToObject(value);
    rawOut.println( curi.pathFromSeed + " " + curi );
    count++
}
cursor.close();
 
rawOut.println()
rawOut.println "---- Total " + count + " pending urls listed ----"