from distutils.core import setup
from warcwriterpool.warcwriterpool import __version__

setup(
	name="python-warcwriterpool",
	version=__version__,
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	packages=[ "warcwriterpool" ],
	license="LICENSE.txt",
	description="Maintains a pool of writable WARC files.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"warctools",
	],
)
