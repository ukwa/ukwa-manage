#!/bin/bash

pip install "$(dirname "$0")/" #--install-option="--prefix=/opt/sipd"

[[ -d /var/run/sipd ]] || mkdir /var/run/sipd
chown heritrix:heritrix /var/run/sipd
#chown -R heritrix:heritrix /opt/sipd/
#find /opt/sipd -name sipd.py -exec chmod +x '{}' \;

