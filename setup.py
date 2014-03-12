from distutils.core import setup

setup(
	name="python-hdfslogs",
	version="0.0.1",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	description="HDFS log-syncing.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"python-webhdfs",
	],
	data_files=[
		( "/usr/local/bin", [ "hdfssync.py" ] ),
	],
)
