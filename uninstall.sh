#!/bin/bash

rm -rf /etc/init.d/sipd-init /opt/sipd/ /var/run/sipd/
mv /var/log/sipd/sipd.log /var/log/sipd/sipd.log.$(date +%Y%m%d%H%M%S) && touch /var/log/sipd/sipd.log && chown heritrix:heritrix /var/log/sipd/sipd.log

