#!/bin/bash

pip install "$(dirname "$0")/"

[[ -d /var/run/harchiverd ]] || mkdir /var/run/harchiverd
[[ -d /var/log/harchiverd ]] || mkdir /var/log/harchiverd
chown heritrix:heritrix /var/run/harchiverd
chown heritrix:heritrix /var/log/harchiverd
chown -R heritrix:heritrix /opt/harchiverd/
chmod +x /usr/local/bin/harchiverd.py

