//classKey = "com,theguardian,www,"
classKey = "uk,co,bbc,www,"
MAX_URLS_TO_LIST = 10

// groovy
// see org.archive.crawler.frontier.BdbMultipleWorkQueues.forAllPendingDo()
 
import com.sleepycat.je.CursorConfig
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import org.archive.modules.CrawlURI
import org.archive.net.UURI
import org.archive.modules.extractor.LinkContext
import org.archive.crawler.frontier.BdbMultipleWorkQueues
import org.archive.crawler.frontier.WorkQueueFrontier
 
pendingUris = job.crawlController.frontier.pendingUris

cursor = pendingUris.pendingUrisDB.openCursor(null, CursorConfig.READ_COMMITTED);
key = new DatabaseEntry(BdbMultipleWorkQueues.calculateOriginKey(classKey));
value = new DatabaseEntry();
count = 0;
cursor.getSearchKey(key, value, null);
while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS && count < MAX_URLS_TO_LIST) {
    if (value.getData().length == 0) {
        continue;
    }
    curi = pendingUris.crawlUriBinding.entryToObject(value);
    //if( curi.toString().startsWith(uri) ) {
      rawOut.println( curi.getClassKey() + "" + curi.getSchedulingDirective() + "." + curi.getPrecedence() + "." + curi.getOrdinal() + " " + curi.pathFromSeed + " " + curi );
      count++
    //}
}
cursor.close();
 
rawOut.println()
rawOut.println "---- Total " + count + " pending urls listed ----"
