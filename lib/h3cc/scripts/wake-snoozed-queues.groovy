//Groovy
  
countBefore = job.crawlController.frontier.getSnoozedCount()
 
job.crawlController.frontier.forceWakeQueues()
countAfter = job.crawlController.frontier.getSnoozedCount()
 
rawOut.println("Snoozed queues.")
rawOut.println(" - Before: " + countBefore)
rawOut.println(" - After: " + countAfter)