python-sip-daemon
==============

LDP06 SIP creation daemon. Install to /opt/sipd/ using:

    pip install ./python-sip-daemon --install-option="--prefix=/opt/sipd"

Presumes the existence of a "heritrix" user. Afterwards it might be necessary to run:

	[[ -d "/var/run/sipd" ]] || mkdir "/var/run/sipd"
	chown heritrix:heritrix "/var/run/sipd"
	chown -R heritrix:heritrix "/opt/sipd"
	chmod +x "/opt/sipd/lib/python2.7/site-packages/sipd/sipd.py"

