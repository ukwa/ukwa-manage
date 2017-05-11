tpool = job.crawlController.toePool;

tpool.toes.each{
    if( it != null) {
        rawOut.println("Killing toe thread " + it.serialNumber)
        tpool.killThread(it.serialNumber, false)
    }
}