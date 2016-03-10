uri="http://static.bbc.co.uk/"
pathFromSeed=""
MAX_URLS_TO_LIST = 10000

// groovy
// see org.archive.crawler.frontier.BdbMultipleWorkQueues.forAllPendingDo()
 
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import org.archive.modules.CrawlURI
import org.archive.net.UURI
import org.archive.modules.extractor.LinkContext
import org.archive.crawler.frontier.BdbMultipleWorkQueues
import org.archive.crawler.frontier.WorkQueueFrontier
 
pendingUris = job.crawlController.frontier.pendingUris

// Set up the curi:
try {
  uuri = new UURI(uri,false);
} catch( groovy.lang.GroovyRuntimeException e ) {
  uuri = new UURI(uri,false,"UTF-8");
}
lc = LinkContext.NAVLINK_MISC;
curi = new CrawlURI( uuri, pathFromSeed, null, lc)
job.crawlController.frontier.frontierPreparer.prepare(curi) 

cursor = pendingUris.pendingUrisDB.openCursor(null, null);
key = BdbMultipleWorkQueues.calculateInsertKey(curi)
value = new DatabaseEntry();
count = 0;
cursor.getSearchKey(key, value, null);
while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS && count < MAX_URLS_TO_LIST) {
    if (value.getData().length == 0) {
        continue;
    }
    curi = pendingUris.crawlUriBinding.entryToObject(value);
    if( curi.toString().startsWith(uri) ) {
      rawOut.println( curi.pathFromSeed + " " + curi );
      count++
    }
}
cursor.close();
 
rawOut.println()
rawOut.println "---- Total " + count + " pending urls listed ----"
