#!/bin/bash

ROOT="/sips/wayback/cdx-index/legal-deposit-hdfs"
CDX="$ROOT.cdx"
NEW="$ROOT.new"
OLD="$ROOT.old"

log()
{
	echo "[$(date "+%Y-%m-%d %H:%M:%S")] $1"
}

hadoop fs -cat "/data/wayback/cdx-index/*/*.cdx" | sort -T /dev/shm/ | sed -r 's@#$@@' > "$NEW"
if [[ -e "$OLD" ]]
then
	log "Removing $OLD..."
	rm "$OLD"
fi

log "Renaming indexes..."
mv -f "$CDX" "$OLD" && mv -f "$NEW" "$CDX"

