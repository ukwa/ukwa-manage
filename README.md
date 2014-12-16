python-sip-daemon
==============

LDP06 SIP creation daemon. Install using:

    pip install ./python-sip-daemon

Presumes the existence of a "heritrix" user. Afterwards it might be necessary to run:

	[[ -d "/var/run/sipd" ]] || mkdir "/var/run/sipd"
	[[ -d "/var/run/sipd" ]] && chown heritrix:heritrix "/var/run/sipd"

The service can be started/stopped with:

    service sipd (stop|start|restart|status)
