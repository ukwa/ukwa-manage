uri="{{ url }}"
MAX_URLS_TO_LIST = {{ limit }}
pathFromSeed = ""

import org.json.JSONObject
import com.sleepycat.je.CursorConfig
import com.sleepycat.je.DatabaseEntry
import com.sleepycat.je.OperationStatus
import java.util.regex.Pattern
import org.archive.modules.CrawlURI
import org.archive.net.UURI
import org.archive.modules.extractor.LinkContext
import org.archive.crawler.frontier.BdbMultipleWorkQueues
import org.archive.crawler.frontier.WorkQueueFrontier

json = new JSONObject()

// Construct the CrawlURI, taking care to cope with API changes in recent H3 updates:
try {
  uuri = new UURI(uri,false);
} catch( groovy.lang.GroovyRuntimeException e ) {
  uuri = new UURI(uri,false,"UTF-8");
}
lc = LinkContext.NAVLINK_MISC;
curi = new CrawlURI( uuri, pathFromSeed, null, lc)
job.crawlController.frontier.frontierPreparer.prepare(curi)

// Get the seed and frontier status:
frontier = new JSONObject();
m = null
pendingUris = job.crawlController.frontier.pendingUris
cursor = pendingUris.pendingUrisDB.openCursor(null, CursorConfig.READ_COMMITTED)
//key = BdbMultipleWorkQueues.calculateInsertKey(curi)
key = new DatabaseEntry(BdbMultipleWorkQueues.calculateOriginKey(curi.getClassKey()))
value = new DatabaseEntry();
count = 0;
cursor.getSearchKey(key, value, null);
while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS && count < MAX_URLS_TO_LIST) {
    if (value.getData().length == 0) {
        continue;
    }
    curiMatch = pendingUris.crawlUriBinding.entryToObject(value);
    if( curiMatch.toString().equals(uri) ) {
      frontier.append("frontier-exact-match",curiMatch)
    }
    if( curiMatch.toString().startsWith(uri) ) {
      frontier.append("frontier-startswith-topten",curiMatch)
      count++
    }
}
cursor.close();
json.append(uri,frontier);


// get the fetch history:
loadProcessor = appCtx.getBean("persistLoadProcessor")
key = loadProcessor.persistKeyFor(uri)
history = loadProcessor.store.get(key)
if( history != null ) {
  fetches = new JSONObject();
	history.get(org.archive.modules.recrawl.RecrawlAttributeConstants.A_FETCH_HISTORY).each{historyStr ->
    	fetches.append( "fetched", historyStr )
	}
  json.append(uri, fetches)
}

rawOut.println( json.toString(2) )



