//classKey = "com,theguardian,www,"
//classKey = "uk,co,bbc,www,"
//MAX_URLS_TO_LIST = 10

uri = "{{ url }}"
MAX_URLS_TO_LIST = {{ limit }}
pathFromSeed = ""

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


// Construct the CrawlURI, taking care to cope with API changes in recent H3 updates:
try {
  uuri = new UURI(uri,false);
} catch( groovy.lang.GroovyRuntimeException e ) {
  uuri = new UURI(uri,false,"UTF-8");
}
lc = LinkContext.NAVLINK_MISC;
curi = new CrawlURI( uuri, pathFromSeed, null, lc)
job.crawlController.frontier.frontierPreparer.prepare(curi)

classKey = curi.getClassKey()

//  
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
