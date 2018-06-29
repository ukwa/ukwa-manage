# UKWA Manage
Luigi tasks for managing the UK Web Archive

## Getting started

n.b. we currently run Python 2.7 on the Hadoop cluster, so streaming
Hadoop tasks need to stick to that version. Other code should be written
in Python 3 but be compatible with both where possible.

### Set up a Python 2.7 environment

    sudo pip install virtualenv
    virtualenv -p python2.7 venv
    source venv/bin/activate
    pip install -r requirements.txt



## Heritrix Jargon


| State | meaning |
|---|---|
| ready | Queues ready to emit a URL now. |
| in-process | Queues that have emitted a URL that is currently being processed. |
| snoozed | Due to the crawl delay, or waiting before retries. |
| active | Total in-process + ready + snoozed |
| inactive | Queues currently not being considered (due to queue rotation). |
| ineligible | Inactive queues where the queue precedence exceeds the precedence floor. |
| retired | Disabled for some reason, e.g. that queue has hit it's allocated quota. |
| exhausted | Queues that are now empty. |


### Notes on queue precedence

A queue's precedence is determined by the precedence provider, usually based on the last crawled URI. Note that a lower precedence value means 'higher priority'.

Precedence is used to determine which queues are brought from inactive to active first. Once the precedence of a queue exceeds the 'floor' (255 by default), it is considered ineligible and won't be crawled any further.

The vernicular here is confusing. Floor is in reference to the least priority but is actually the highest allowed integer value.

In practice, unless you use a special precedence policy or tinker with the precedence floor, you will never hit an ineligible condition.

A use for this would be a precedence policy that gradually lowers the precedence (cumulatively) as it encounters more and more 'junky' URLs. But I'm not aware of anyone using it in that manner.