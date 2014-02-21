#!/bin/bash

pip install "$(dirname "$0")/"

[[ -d /var/run/harchiverd ]] || mkdir /var/run/harchiverd
[[ -d /var/log/harchiverd ]] || mkdir /var/log/harchiverd
chown -R heritrix:heritrix /var/run/harchiverd
chown -R heritrix:heritrix /var/log/harchiverd

