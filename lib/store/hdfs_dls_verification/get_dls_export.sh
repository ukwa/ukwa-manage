#!/usr/bin/env bash

rsync -a "root@dls-gtw:/heritrix/sips/dls-export/Public\ Web\ Archive\ Access\ Export.txt" dls_wa_export.txt
grep -P "^vdc" dls_wa_export.txt > ttt
cat ttt | sort | uniq > dls_wa_export.txt
chmod 644 dls_wa_export.txt
rm ttt
