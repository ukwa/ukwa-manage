from distutils.core import setup

setup(
	name="python-sip-daemon",
	version="0.0.1",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	packages=[ "sipd" ],
	description="SIP creation daemon.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"requests >= 2.0.0",
		"pika",
		"bagit",
		"python-legal-deposit-sip",
		"python-daemonize",
		"python-webhdfs",
	],
	data_files=[
		( "/etc/init.d", [ "sipd-init" ] ),
	]
)

