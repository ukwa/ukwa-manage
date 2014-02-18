from distutils.core import setup

setup(
	name="python-warcwriterpool",
	version="0.0.2",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	packages=[ "warcwriterpool" ],
	license="LICENSE.txt",
	description="Maintains a pool of writable WARC files.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"hanzo-warctools",
	],
)
