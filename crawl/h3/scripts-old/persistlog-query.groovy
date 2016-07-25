uri="http://www.bbc.co.uk/news/"
loadProcessor = appCtx.getBean("persistLoadProcessor") //this name depends on config
key = loadProcessor.persistKeyFor(uri)
history = loadProcessor.store.get(key)
if( history != null ) {
    json = new org.json.JSONObject();
	history.get(org.archive.modules.recrawl.RecrawlAttributeConstants.A_FETCH_HISTORY).each{historyStr ->
    	json.append( uri, historyStr )
	}
    rawOut.println( json.toString(2) )
}


