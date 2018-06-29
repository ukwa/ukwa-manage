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



## Dash


| State | meaning |
|---|---|
| this | that|