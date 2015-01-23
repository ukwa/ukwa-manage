from distutils.core import setup

setup(
	name="python-legal-deposit-sip",
	version="0.0.1",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	packages=[ "sip" ],
	description="SIP creation library.",
	long_description=open( "README.md" ).read(),
	install_requires=[
        "python-webhdfs",
        "bagit",
        "dateutil"
        "lxml",
	],
)
