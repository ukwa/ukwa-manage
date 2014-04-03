from distutils.core import setup

setup(
	name="python-sip-verification",
	version="0.0.1",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	description="SIP verification.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"pika",
		"python-webhdfs",
		"requests",
		"lxml",
		"python-dateutil",
	],
	data_files=[
		( "/usr/local/bin", [ "verify-sips.py" ] ),
	],
)
