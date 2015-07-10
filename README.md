python-sip-daemon
==============

This package provides a Linux daemon intended to monitor a queue and, when messages arrive on said queue, create SIPs for ingest into the DLS in accordance with the format specified as part of LDP06.

The `bin` directory contains an `init.d` script, `sipd`, which can be manipulated via the standard:

    service sipd (stop|start|restart|status)

The `init.d` script does presume the existence of a `heritrix` user. Afterward installation (which can be done via `pip`) it will be necessary to run:

    [[ -d "/var/run/sipd" ]] || mkdir "/var/run/sipd"
    [[ -d "/var/run/sipd" ]] && chown heritrix:heritrix "/var/run/sipd"

The daemon expects messages of the format `<job-name>/<launch-id>` (e.g. `daily/20150708110924`); specifically, the message will correspond to the directory structure of the Heritrix job (e.g. WARCs will be written to `/heritrix/output/warcs/daily/20150708110924/`).

